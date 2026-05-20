# OpenObserve Trace 检索机制深度解析

## 一、核心架构概述

OpenObserve 的 Trace 检索系统采用"**列式存储直接查询 + 三阶段 SQL 聚合 + 按时间分区流式处理**"的架构设计，解决大数据量下 trace 和 span 的高效检索问题。

> **重要更正**：`trace_list_index` 仅作为**写入侧可选元数据表**，**不参与查询路径**。latest 和 latest_stream 查询均直接对 trace 主数据表执行 SQL 查询。

```
┌─────────────────────────────────────────────────────────────────┐
│                     Trace 数据写入链路                            │
├─────────────────────────────────────────────────────────────────┤
│  OTLP 请求 → Span 解析 → 扁平化处理 → 写入主表(Parquet)          │
│                                    ↓                              │
│                          [可选] TraceListIndex                   │
│                          (traces_list_index_enabled)             │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Trace 查询链路（latest）                       │
├─────────────────────────────────────────────────────────────────┤
│  Q1: 主表 GROUP BY trace_id 聚合 → Q2a: 主表 span 统计           │
│                                            ↓                      │
│                                      Q2b: 多服务详情              │
└─────────────────────────────────────────────────────────────────┘
```

## 二、Trace 入库与索引机制

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

#### 2.1.2 TraceListIndex：写入侧可选元数据

**文件**：`src/service/metadata/trace_list_index.rs`

> **关键事实**：`trace_list_index` 是写入侧的可选元数据表，由配置项 `traces_list_index_enabled` 控制是否启用。**当前版本（2026-05-20）查询链路未使用此表**。

**启用条件**（`src/service/traces/mod.rs:1161`）：
```rust
if cfg.common.traces_list_index_enabled
```

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
- 作用：在索引表上快速判断某个 `trace_id` 是否存在
- 优势：O(1) 时间复杂度，内存占用极小
- 注意：当前查询路径未使用此索引表，仅写入侧可选生成

#### 2.1.3 写入流程

```
write_traces()
  ├─ check_for_schema()         # 检查并演进 schema
  ├─ write_file()              # 主数据写入 WAL → Parquet（必须执行）
  └─ [可选] 写入 TraceListIndex
     └─ if cfg.common.traces_list_index_enabled:
         └─ write(MetadataType::TraceListIndexer, trace_index_values)
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

1. **查询所有 span**（直接查询主表）：
```sql
SELECT span_id, trace_id, service_name, operation_name, span_status,
       reference_parent_span_id, start_time, end_time, gen_ai_operation_name
FROM {stream_name}
WHERE trace_id = '{trace_id}'
```

2. **构建节点和边**（应用层内存构建）：
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
- 根 span：`reference_parent_span_id` 为 NULL 或空字符串
- 子 span：通过 `reference_parent_span_id` 指向父 span
- 跨 trace 链接：通过 `links` 字段存储（JSON 格式）

## 四、latest 查询：三阶段 SQL 聚合逻辑

**文件**：`src/handler/http/request/traces/mod.rs`

**API**：`GET /api/{org_id}/{stream_name}/traces/latest`

**核心设计**：三阶段 SQL 查询，所有阶段**均直接查询 trace 主数据表**，不经过 `trace_list_index`。

---

### 4.1 阶段 1 (Q1)：Trace 聚合查询

**代码位置**：`src/handler/http/request/traces/mod.rs:362-412`

**作用**：按 `trace_id` 分组聚合，获取 trace 级别摘要信息，用于分页和排序。

#### 4.1.1 普通 Trace 流查询
```sql
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

#### 4.1.2 LLM Trace 流查询（带 AI 字段）
```sql
SELECT trace_id,
       min(_timestamp) as zo_sql_timestamp,
       min(start_time) as trace_start_time,
       max(end_time) as trace_end_time,
       (max(end_time) - min(start_time)) as zo_sql_duration,
       sum(gen_ai_usage_input_tokens) as gen_ai_usage_details_input,
       sum(gen_ai_usage_output_tokens) as gen_ai_usage_details_output,
       sum(gen_ai_usage_total_tokens) as gen_ai_usage_details_total,
       sum(gen_ai_usage_cost) as gen_ai_usage_cost_details,
       array_agg(DISTINCT gen_ai_response_model) FILTER (
           WHERE gen_ai_response_model IS NOT NULL AND gen_ai_response_model != ''
       ) as gen_ai_response_models,
       FIRST_VALUE(gen_ai_input_messages ORDER BY _timestamp ASC) FILTER (
           WHERE gen_ai_input_messages IS NOT NULL AND gen_ai_input_messages != ''
       ) as gen_ai_input_messages
FROM "{stream_name}"
[WHERE {filter}]
GROUP BY trace_id
ORDER BY {sql_order_expr}
```

#### 4.1.3 Legacy LLM Trace 流（`_o2_llm`  schema）
```sql
SELECT trace_id,
       min(_timestamp) as zo_sql_timestamp,
       min(start_time) as trace_start_time,
       max(end_time) as trace_end_time,
       (max(end_time) - min(start_time)) as zo_sql_duration,
       sum(llm_usage_tokens_input) as gen_ai_usage_details_input,
       sum(llm_usage_tokens_output) as gen_ai_usage_details_output,
       sum(llm_usage_tokens_total) as gen_ai_usage_details_total,
       sum(llm_usage_cost_total) as gen_ai_usage_cost_details,
       array_agg(DISTINCT llm_model_name) FILTER (
           WHERE llm_model_name IS NOT NULL AND llm_model_name != ''
       ) as gen_ai_response_models,
       FIRST_VALUE(llm_input ORDER BY _timestamp ASC) FILTER (
           WHERE llm_input IS NOT NULL AND llm_input != ''
       ) as gen_ai_input_messages
FROM "{stream_name}"
[WHERE {filter}]
GROUP BY trace_id
ORDER BY {sql_order_expr}
```

#### 4.1.4 排序支持
```rust
let sql_order_expr = match sort_by.as_str() {
    "duration" => format!("zo_sql_duration {sort_order}"),
    "start_time" | "_timestamp" => format!("zo_sql_timestamp {sort_order}"),
    _ => return MetaHttpResponse::bad_request(...),
};
```

**设计意图**：
- 只聚合 trace 级别的摘要信息，避免加载大量 span 数据
- 利用列式存储的优势，只读取需要的列（投影下推）
- 支持按 `start_time` 或 `duration` 排序
- 通过 `LIMIT/OFFSET` 实现分页

---

### 4.2 阶段 2 (Q2a)：Span 统计查询

**代码位置**：`src/handler/http/request/traces/mod.rs:546-576`

**作用**：针对 Q1 返回的 trace_id 列表，进行 span 级别的详细统计。

```sql
SELECT trace_id,
       count(*) AS span_count,
       sum(CASE WHEN span_status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
       min(start_time) AS min_start_time,
       max(end_time) AS max_end_time,
       max(duration) AS max_duration,
       count(DISTINCT service_name) AS service_count,
       max(CASE
           WHEN reference_parent_span_id IS NULL OR reference_parent_span_id = ''
           THEN service_name
       END) AS root_service_name,
       max(CASE
           WHEN reference_parent_span_id IS NULL OR reference_parent_span_id = ''
           THEN operation_name
       END) AS root_operation_name,
       first_value(service_name ORDER BY _timestamp ASC) AS first_service_name,
       first_value(operation_name ORDER BY _timestamp ASC) AS first_operation_name
FROM "{stream_name}"
WHERE trace_id IN ('{trace_ids}')
GROUP BY trace_id
```

**关键要点**：
- **Trace ID 安全过滤**：在拼接 SQL 前对 trace_id 进行 sanitization，只允许十六进制字符和连字符，防止 SQL 注入
- **根 span 识别**：通过 `reference_parent_span_id IS NULL OR reference_parent_span_id = ''` 条件识别根 span
- **单服务 Trace 优化**：`service_count = 1` 的 trace 从 Q2a 即可获得完整信息，无需 Q2b
- **请求参数设置**：`from=0, size=trace_count`，因为每个 trace 只返回一行

**代码细节**：
```rust
// Trace ID 安全过滤（src/handler/http/request/traces/mod.rs:552-562）
let sanitized_ids: Vec<String> = traces_data
    .values()
    .map(|v| {
        v.trace_id
            .chars()
            .filter(|c| c.is_ascii_hexdigit() || *c == '-')
            .collect::<String>()
    })
    .filter(|tid| !tid.is_empty())
    .collect();
let trace_ids = sanitized_ids.join("','");
```

---

### 4.3 阶段 3 (Q2b)：多服务 Trace 详情查询

**代码位置**：`src/handler/http/request/traces/mod.rs:588-644`

**作用**：仅针对跨多个服务的 trace（`service_count > 1`），查询服务级别的统计信息。

```sql
SELECT trace_id, service_name,
       count(*) AS svc_count,
       max(duration) AS svc_duration
FROM "{stream_name}"
WHERE trace_id IN ('{multi_service_tids}')
GROUP BY trace_id, service_name
```

**执行条件**：
```rust
// 仅当 trace 的 service_count > 1 时才执行 Q2b
if service_count > 1 {
    multi_service_tids.push(trace_id.clone());
    multi_service_total += service_count as i64;
}

// 如果没有多服务 trace，直接返回
if multi_service_tids.is_empty() {
    return Ok(...);
}
```

**请求参数设置**：
- `from=0, size=multi_service_total`
- 精确设置大小，避免分页

---

### 4.4 latest 查询完整流程图

```
用户请求 /traces/latest
    │
    ├─ 参数解析（filter, start_time, end_time, from, size, sort_by）
    │
    ├─ [优化] 从 trace_id 中提取时间戳，缩小查询范围
    │
    ├─ Q1: 主表 GROUP BY trace_id 聚合（获取 trace_id 列表 + 摘要）
    │    └─ 读取列：trace_id, _timestamp, start_time, end_time, [AI 字段]
    │
    ├─ 如果 Q1 结果为空，直接返回
    │
    ├─ Q2a: 主表 span 统计（针对 Q1 返回的 trace_id 列表）
    │    ├─ 读取列：trace_id, span_status, reference_parent_span_id,
    │    │          service_name, operation_name, duration, start_time, end_time
    │    │
    │    └─ 结果：span_count, error_count, service_count, root_* 等
    │
    ├─ [可选] Q2b: 多服务 trace 详情
    │    └─ 仅针对 service_count > 1 的 trace
    │
    └─ 结果组装，返回给客户端
```

## 五、latest_stream 查询：按时间分区流式处理

**API**：`GET /api/{org_id}/{stream_name}/traces/latest_stream`

**文件**：`src/handler/http/request/traces/mod.rs` (函数 `get_latest_traces_stream`)

### 5.1 核心设计思想

针对大时间范围查询，采用**"按时间分区 + 分区内三阶段查询 + SSE 流式返回"**策略，解决：
1. 大时间范围查询超时问题
2. 首屏加载慢问题
3. 内存占用过高问题

### 5.2 执行流程

#### 步骤 1：获取时间分区列表
```rust
// 按时间从新到旧排序
let partitions = SearchService::search_partition(...).await?;
// 分区按 start_time 降序排列（最新的在前）
partitions.sort_by(|a, b| b.start_time.cmp(&a.start_time));
```

#### 步骤 2：对每个分区独立执行三阶段查询
```
for partition in partitions:
    ├─ Q1: 分区内 GROUP BY trace_id 聚合
    ├─ Q2a: 分区内 span 统计
    ├─ Q2b: 分区内多服务详情（可选）
    └─ 通过 SSE 发送该分区的结果给客户端
```

#### 步骤 3：Q1 SQL 构建（与 latest 完全相同）
```rust
// 代码位置：src/handler/http/request/traces/mod.rs:1174-1215
// 与 latest 的 Q1 SQL 构建逻辑完全一致
let query_sql_base = if is_llm_stream && has_gen_ai_fields {
    format!("SELECT trace_id, ... FROM \"{stream_name}\"")
} else {
    format!("SELECT trace_id, ... FROM \"{stream_name}\"")
};
```

#### 步骤 4：结果合并与去重
由于 trace 可能跨多个时间分区，客户端需要负责合并和去重。服务端仅按分区流式返回。

### 5.3 latest_stream 与 latest 的对比

| 对比项 | latest | latest_stream |
|--------|--------|---------------|
| 查询范围 | 全时间范围一次性查询 | 按时间分区逐个查询 |
| 返回方式 | 完整结果一次性返回 | SSE 流式增量返回 |
| 首屏时间 | 较慢（需等待全量查询） | 快（第一个分区返回即可显示） |
| 内存占用 | 高（全量结果在内存） | 低（仅单分区结果） |
| 适用场景 | 小时间范围、精确查询 | 大时间范围、探索性查询 |
| 结果完整性 | 完整去重 | 可能有重复（跨分区 trace） |
| Q1/Q2a/Q2b 逻辑 | 完全相同 | 完全相同（仅缩小到单个分区） |

## 六、查询过滤与优化机制

### 6.1 Trace ID 精确查询优化

**代码位置**：`src/handler/http/request/traces/mod.rs:294-319`

当查询参数中包含 `trace_id` 时，利用 UUID v7 嵌入的时间戳缩小查询范围：

```rust
// 从 query 参数或 filter 字符串中提取 trace_id
let search_trace_id = query.get("trace_id").map(|v| v.to_string());
let start_time_from_trace_id = if let Some(trace_id) = search_trace_id {
    config::ider::get_start_time_from_trace_id(&trace_id).unwrap_or(0)
} else if filter.contains("trace_id=") {
    // 从 filter 字符串中安全提取 trace_id
    // ...
} else {
    0
};

// 缩小查询时间窗口
if start_time_from_trace_id > 0 {
    start_time = start_time_from_trace_id - 60 * 1_000_000;  // 向前 60 秒
    end_time = std::cmp::min(now_micros(), start_time_from_trace_id + 3600 * 1_000_000);  // 向后 1 小时
}
```

**原理**：
- OpenObserve 使用 UUID v7 格式的 trace_id，其中嵌入了时间戳
- 通过解析 trace_id 可以精确缩小查询时间窗口
- 避免全时间范围扫描，极大提升查询速度

### 6.2 过滤条件支持

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

### 6.3 安全过滤

**Trace ID 安全过滤**（`src/handler/http/request/traces/mod.rs:552-562`）：
```rust
let sanitized_ids: Vec<String> = traces_data
    .values()
    .map(|v| {
        v.trace_id
            .chars()
            .filter(|c| c.is_ascii_hexdigit() || *c == '-')
            .collect::<String>()
    })
    .filter(|tid| !tid.is_empty())
    .collect();
```

**Filter 安全过滤**（`src/handler/http/request/traces/mod.rs:1170-1171`）：
```rust
let f = filter.replace("--", "").replace(';', "");
f.trim().to_string()
```

## 七、性能优化要点

### 7.1 存储层优化
1. **Parquet 列式存储**：高压缩比，高扫描性能，支持谓词下推和投影下推
2. **按时间分区**：数据按时间分片，过期自动清理，查询时可跳过不相关分区
3. **WAL 异步写入**：先写 WAL，后台异步刷盘到对象存储
4. **统计信息**：Parquet 文件自带 min/max/count 统计信息，用于查询优化

### 7.2 查询层优化
1. **三阶段查询**：先聚合后详情，逐步减少数据加载量
2. **时间窗口收缩**：从 trace_id 提取时间戳缩小范围
3. **列式投影**：只读取需要的列（Parquet 谓词下推）
4. **分区处理**：大时间范围按时间分区处理，增量返回
5. **Trace ID Sanitization**：防止 SQL 注入，只允许十六进制字符和连字符

### 7.3 关于 TraceListIndex 的说明
- 当前版本：仅写入侧可选元数据，查询路径未使用
- 布隆过滤器：虽已配置，但查询链路未调用
- 未来可能：预留作为 trace 列表快速查询的优化点

## 八、代码文件索引

| 功能模块 | 文件路径 |
|---------|---------|
| Trace 入库主逻辑 | `src/service/traces/mod.rs` |
| TraceListIndex 索引定义 | `src/service/metadata/trace_list_index.rs` |
| Trace 列表查询 (latest/latest_stream) | `src/handler/http/request/traces/mod.rs` |
| Trace DAG 查询 | `src/handler/http/request/traces/dag.rs` |
| OTEL Span 处理器 | `src/service/traces/otel/processor.rs` |
| 元数据写入 | `src/service/metadata/mod.rs` |
| 配置项定义 | `src/config/src/config.rs:1454` |
