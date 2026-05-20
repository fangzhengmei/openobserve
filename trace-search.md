# OpenObserve Trace 检索机制深度解析

## 一、核心架构概述

OpenObserve 的 Trace 检索系统采用"**双层索引 + 列式存储 + 多阶段 SQL 查询**"的架构设计，解决大数据量下 trace 和 span 的高效检索问题。

```
┌─────────────────────────────────────────────────────────────────┐
│                     Trace 数据写入链路                            │
├─────────────────────────────────────────────────────────────────┤
│  OTLP 请求 → Span 解析 → 扁平化处理 → 写入主表(Parquet)          │
│                                    ↓                              │
│                          TraceListIndex 索引表                   │
│                                    ↓                              │
│                          Bloom Filter(trace_id)                  │
└─────────────────────────────────────────────────────────────────┘
```

## 二、Trace 入库索引机制

### 2.1 数据写入主流程

**入口文件**：`src/service/traces/mod.rs`

#### 2.1.1 OTLP 请求解析

```rust
// 处理 OTLP protobuf/json 请求
pub async fn handle_otlp_request(
    org_id: &str,
    request: ExportTraceServiceRequest,
    req_type: OtlpRequestType,
    in_stream_name: Option<&str>,
    user: IngestUser,
) -> Result<HttpResponse, Error>
```

**关键处理步骤**：

1. **资源属性提取**：从 `ResourceSpans` 中提取 `service.name` 等资源属性
2. **Span 字段映射**：
   - `trace_id` (16字节) → 十六进制字符串
   - `span_id` (8字节) → 十六进制字符串
   - `parent_span_id` → 存入 `reference.parent_span_id`
   - `start_time_unix_nano` / `end_time_unix_nano` → 计算 `duration` (微秒)
   - `attributes` → 扁平化后存入 `attributes.*`
   - `events` → JSON 序列化后存储
   - `links` → JSON 序列化后存储

3. **Span 结构**：
```rust
struct Span {
    trace_id: String,           // 关联键
    span_id: String,            // 唯一标识
    span_kind: String,
    span_status: String,        // OK/ERROR/UNSET
    operation_name: String,
    start_time: u64,            // 纳秒
    end_time: u64,              // 纳秒
    duration: u64,              // 微秒 = (end - start) / 1000
    reference: HashMap<String, Value>,  // parent_span_id, ref_type
    service_name: String,
    attributes: HashMap<String, Value>,
    service: HashMap<String, Value>,
    events: String,             // JSON 序列化
    links: String,              // JSON 序列化
}
```

#### 2.1.2 TraceListIndex 索引表

**文件**：`src/service/metadata/trace_list_index.rs`

这是 OpenObserve 为 trace 检索优化设计的**轻量级索引表**，用于快速定位 trace。

**索引表结构**：
| 字段 | 类型 | 说明 |
|------|------|------|
| `_timestamp` | Int64 | 索引写入时间 |
| `stream_name` | Utf8 | 数据流名称 |
| `service_name` | Utf8 | 服务名称 |
| `trace_id` | Utf8 | Trace ID |

**索引特性**：
```rust
// 索引表配置
let settings = StreamSettings {
    bloom_filter_fields: vec!["trace_id".to_string()],  // 布隆过滤器
    partition_keys: vec![],
    full_text_search_keys: vec![],
    index_fields: vec![],
    // ...
};
```

**布隆过滤器 (Bloom Filter)**：
- 作用：快速判断某个 `trace_id` 是否存在于索引表中
- 优势：O(1) 时间复杂度，内存占用极小
- 场景：当用户输入 trace_id 查询时，先通过布隆过滤器过滤，避免全表扫描

#### 2.1.3 写入流程

```
write_traces()
  ├─ check_for_schema()         # 检查并演进 schema
  ├─ 为每个 span 构建 TraceListItem
  │   └─ MetadataItem::TraceListIndexer(TraceListItem {
  │       _timestamp,
  │       stream_name,
  │       service_name,
  │       trace_id
  │   })
  ├─ write_file()              # 主数据写入 WAL → Parquet
  └─ write(MetadataType::TraceListIndexer, trace_index_values)
                                # 索引表异步写入
```

## 三、Span 关联机制

### 3.1 父子关系存储

**关联字段**：`reference_parent_span_id`

在入库时，OpenObserve 将 OTLP 中的 `parent_span_id` 转换为扁平化字段：

```rust
// src/service/traces/mod.rs:337-347
let mut span_ref = HashMap::new();
if !span.parent_span_id.is_empty()
    && span.parent_span_id.len() == SPAN_ID_BYTES_COUNT
{
    span_ref.insert(PARENT_TRACE_ID.to_string(), trace_id.clone());
    span_ref.insert(
        PARENT_SPAN_ID.to_string(),  // "reference.parent_span_id"
        SpanId::from_bytes(span.parent_span_id.try_into().unwrap()).to_string(),
    );
    span_ref.insert(REF_TYPE.to_string(), format!("{:?}", SpanRefType::ChildOf));
}
```

扁平化后存储为：
- `reference.parent_span_id` → `reference_parent_span_id` (Parquet 列名)
- `reference.parent_trace_id` → `reference_parent_trace_id`
- `reference.ref_type` → `reference_ref_type`

### 3.2 DAG 构建逻辑

**文件**：`src/handler/http/request/traces/dag.rs`

查询单个 trace 的完整调用链时：

1. **查询所有 span**：
```sql
SELECT span_id, trace_id, service_name, operation_name, span_status,
       reference_parent_span_id, start_time, end_time, gen_ai_operation_name
FROM {stream_name}
WHERE trace_id = '{trace_id}'
```

2. **构建节点和边**：
```rust
for item in resp_search.hits {
    let span_id = ...;
    let parent_span_id = item.get("reference_parent_span_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    // 创建节点
    nodes.push(SpanNode { span_id, parent_span_id, ... });

    // 创建边
    if let Some(parent_id) = parent_span_id {
        edges.push(SpanEdge { from: parent_id, to: span_id });
    }
}
```

**关联关键点**：
- 根 span：`parent_span_id` 为 NULL 或空字符串
- 子 span：通过 `reference_parent_span_id` 指向父 span
- 跨 trace 链接：通过 `links` 字段存储（JSON 格式）

## 四、查询过滤逻辑

### 4.1 最新 Trace 列表查询

**文件**：`src/handler/http/request/traces/mod.rs`

**API**：`GET /api/{org_id}/{stream_name}/traces/latest`

采用 **三阶段 SQL 查询** 策略，解决大数据量下的性能问题：

#### 阶段 1 (Q1)：Trace 聚合查询

```sql
-- 普通 trace 流
SELECT trace_id,
       min(_timestamp) as zo_sql_timestamp,
       min(start_time) as trace_start_time,
       max(end_time) as trace_end_time,
       (max(end_time) - min(start_time)) as zo_sql_duration
FROM "{stream_name}"
[WHERE {filter}]
GROUP BY trace_id
ORDER BY {sql_order_expr}
LIMIT {size} OFFSET {from}
```

```sql
-- LLM trace 流（带 AI 字段）
SELECT trace_id,
       min(_timestamp) as zo_sql_timestamp,
       min(start_time) as trace_start_time,
       max(end_time) as trace_end_time,
       (max(end_time) - min(start_time)) as zo_sql_duration,
       sum(gen_ai_usage_input_tokens) as gen_ai_usage_details_input,
       sum(gen_ai_usage_output_tokens) as gen_ai_usage_details_output,
       sum(gen_ai_usage_total_tokens) as gen_ai_usage_details_total,
       sum(gen_ai_usage_cost) as gen_ai_usage_cost_details,
       array_agg(DISTINCT gen_ai_response_model) FILTER (...) as gen_ai_response_models,
       FIRST_VALUE(gen_ai_input_messages ORDER BY _timestamp ASC) FILTER (...) as gen_ai_input_messages
FROM "{stream_name}"
[WHERE {filter}]
GROUP BY trace_id
ORDER BY {sql_order_expr}
```

**设计意图**：
- 只聚合 trace 级别的摘要信息，避免加载大量 span 数据
- 利用列式存储的优势，只读取需要的列
- 支持按 `start_time` 或 `duration` 排序

#### 阶段 2 (Q2a)：Span 统计查询

针对 Q1 返回的 trace_id 列表，进行 span 级别的统计：

```sql
SELECT trace_id,
       count(*) AS span_count,
       sum(CASE WHEN span_status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
       min(start_time) AS min_start_time,
       max(end_time) AS max_end_time,
       max(duration) AS max_duration,
       count(DISTINCT service_name) AS service_count,
       max(CASE WHEN reference_parent_span_id IS NULL OR reference_parent_span_id = ''
                THEN service_name END) AS root_service_name,
       max(CASE WHEN reference_parent_span_id IS NULL OR reference_parent_span_id = ''
                THEN operation_name END) AS root_operation_name,
       first_value(service_name ORDER BY _timestamp ASC) AS first_service_name,
       first_value(operation_name ORDER BY _timestamp ASC) AS first_operation_name
FROM "{stream_name}"
WHERE trace_id IN ('{trace_ids}')
GROUP BY trace_id
```

**关键优化**：
- `trace_id IN (...)` 利用 Parquet 统计信息和索引快速定位
- 单服务 trace 直接从 Q2a 获取完整信息，无需后续查询

#### 阶段 3 (Q2b)：多服务 Trace 详情查询

仅针对跨多个服务的 trace，查询服务级别的统计：

```sql
SELECT trace_id, service_name,
       count(*) AS svc_count,
       max(duration) AS svc_duration
FROM "{stream_name}"
WHERE trace_id IN ('{multi_service_tids}')
GROUP BY trace_id, service_name
```

### 4.2 流式查询优化

**API**：`GET /api/{org_id}/{stream_name}/traces/latest_stream`

针对大时间范围查询，采用**按时间分区流式返回**策略：

1. 获取时间分区列表（新→旧排序）
2. 对每个分区独立执行 Q1 → Q2a → Q2b
3. 结果通过 SSE (Server-Sent Events) 逐步返回给客户端

**优势**：
- 首屏加载快：第一个分区结果返回即可显示
- 内存占用低：不需要一次性加载所有分区数据
- 用户体验好：可以看到查询进度

### 4.3 Trace ID 精确查询优化

当查询参数中包含 `trace_id` 时：

```rust
// 从 trace_id 中提取时间戳（UUID v7 格式）
let start_time_from_trace_id = config::ider::get_start_time_from_trace_id(&trace_id).unwrap_or(0);
if start_time_from_trace_id > 0 {
    start_time = start_time_from_trace_id - 60 * 1_000_000;  // 向前 60 秒
    end_time = start_time_from_trace_id + 3600 * 1_000_000;  // 向后 1 小时
}
```

**原理**：
- OpenObserve 使用 UUID v7 格式的 trace_id，其中嵌入了时间戳
- 通过解析 trace_id 可以精确缩小查询时间窗口
- 避免全时间范围扫描，极大提升查询速度

### 4.4 过滤条件支持

查询支持通过 `filter` 参数传递 SQL WHERE 条件，例如：
```
filter=service_name='frontend' AND span_status='ERROR'
```

支持的常用过滤字段：
- `trace_id` - 精确匹配
- `service_name` - 服务名称
- `operation_name` - 操作名称
- `span_status` - 状态 (OK/ERROR/UNSET)
- `duration` - 持续时间
- `start_time` / `end_time` - 时间范围
- 任意自定义属性字段

## 五、性能优化要点

### 5.1 索引层优化
1. **TraceListIndex 索引表**：独立的元数据表，只存 trace 级摘要
2. **Bloom Filter**：trace_id 快速存在性判断
3. **Parquet 统计信息**：min/max/count 用于谓词下推

### 5.2 查询层优化
1. **三阶段查询**：先聚合后详情，减少数据加载
2. **时间窗口收缩**：从 trace_id 提取时间戳缩小范围
3. **列式投影**：只读取需要的列（Parquet 谓词下推）
4. **流式处理**：大时间范围分区处理，增量返回
5. **Trace ID  sanitization**：防止 SQL 注入，只允许十六进制字符和连字符

### 5.3 存储层优化
1. **按时间分区**：数据按时间分片，过期自动清理
2. **Parquet 列式存储**：高压缩比，高扫描性能
3. **WAL 异步写入**：先写 WAL，后台异步刷盘到对象存储

## 六、代码文件索引

| 功能模块 | 文件路径 |
|---------|---------|
| Trace 入库主逻辑 | `src/service/traces/mod.rs` |
| TraceListIndex 索引 | `src/service/metadata/trace_list_index.rs` |
| Trace 列表查询 | `src/handler/http/request/traces/mod.rs` |
| Trace DAG 查询 | `src/handler/http/request/traces/dag.rs` |
| OTEL Span 处理器 | `src/service/traces/otel/processor.rs` |
| 元数据写入 | `src/service/metadata/mod.rs` |
