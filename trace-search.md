# OpenObserve Trace 检索机制深度解析

## 一、核心架构概述

OpenObserve 的 Trace 检索系统采用"**列式存储直接查询 + 三阶段 SQL 聚合 + 按时间分区流式处理 + 服务端全局去重**"的架构设计，解决大数据量下 trace 和 span 的高效检索问题。

> **重要更正**：
> 1. `trace_list_index` 仅作为**写入侧可选元数据表**，**不参与查询路径**。latest 和 latest_stream 查询均直接对 trace 主数据表执行 SQL 查询。
> 2. `latest_stream` 通过服务端 `seen_trace_ids` HashSet 实现**全局去重**，**客户端无需负责去重**。

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
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Trace 查询链路（latest_stream）                  │
├─────────────────────────────────────────────────────────────────┤
│  按时间分区循环:                                              │
│    ├─ Q1: 分区内 GROUP BY trace_id 聚合                          │
│    ├─ 去重: seen_trace_ids HashSet 过滤                          │
│    ├─ Q2a: 分区内 span 统计（仅可交付 trace）               │
│    ├─ Q2b: 分区内多服务详情（可选）                         │
│    └─ SSE 流式返回客户端                                       │
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

## 五、latest_stream 查询：按时间分区流式处理 + 服务端去重

**API**：`GET /api/{org_id}/{stream_name}/traces/latest_stream`

**文件**：`src/handler/http/request/traces/mod.rs` (函数 `get_latest_traces_stream` 和 `process_latest_traces_stream`)

### 5.1 核心设计思想

针对大时间范围查询，采用**"按时间分区 + 服务端全局去重 + 分区内三阶段查询 + SSE 流式返回"**策略，解决：
1. 大时间范围查询超时问题
2. 首屏加载慢问题
3. 内存占用过高问题
4. 跨分区 trace 重复问题（通过服务端 `seen_trace_ids` 去重）

> **关键修正**：服务端通过 `seen_trace_ids` HashSet 实现全局去重，**客户端无需负责去重**。

### 5.2 核心状态变量

**代码位置**：`src/handler/http/request/traces/mod.rs:1322-1330`

```rust
// 分页相关
let mut hits_seen: i64 = 0;        // 已处理的去重后 trace 总数
let hits_to_skip = from;            // 全局跳过数量（from 参数）
let hits_to_deliver = size;         // 需要返回的数量（size 参数）
let mut hits_delivered: i64 = 0;    // 已发送给客户端的数量

// 去重相关
let mut seen_trace_ids: std::collections::HashSet<String> = HashSet::new();
```

### 5.3 执行流程详解

#### 步骤 1：获取时间分区列表

```rust
// 代码位置：src/handler/http/request/traces/mod.rs:1263-1313
let partitions = SearchService::search_partition(...).await?;

// 根据排序方向调整分区顺序
let partitions_desc = if sql_order_expr == "zo_sql_timestamp DESC" {
    partitions  // 已默认按时间降序
} else if sql_order_expr == "zo_sql_timestamp ASC" {
    partitions.into_iter().rev().collect()  // 反转按时间升序
} else {
    // 非时间排序（如 duration）：强制合并为单个分区
    // 因为跨分区排序无法保证正确性
    vec![[partition_req.start_time, partition_req.end_time]]
};
```

#### 步骤 2：分区循环处理

```
for partition in partitions_desc:
    ├─ 计算本分区 fetch_size
    │   └─ fetch_size = (remaining_to_skip + remaining_needed).min(query_default_limit)
    │
    ├─ Q1: 分区内 GROUP BY trace_id 聚合
    │
    ├─ 去重过滤（seen_trace_ids）
    │   ├─ 过滤掉已见过的 trace_id
    │   ├─ partition_total = 去重后的数量
    │   └─ hits_seen += partition_total
    │
    ├─ 按 start_time 排序
    │
    ├─ 注册所有 trace_id 到 seen_trace_ids（即使被跳过的）
    │   └─ 防止后续分区重复出现
    │
    ├─ 应用全局 offset，提取可交付的 trace
    │   ├─ skip_in_partition = max(0, hits_to_skip - hits_seen_before)
    │   └─ deliverable_q1 = sorted_hits.skip(skip).take(need)
    │
    ├─ Q2a: 仅对 deliverable_q1 的 trace_id 查询 span 统计
    ├─ Q2b: 仅对多服务 trace 查询服务详情
    │
    ├─ 组装结果，通过 SSE 发送
    │   └─ hits_delivered += deliverable.len()
    │
    └─ 如果 hits_delivered >= hits_to_deliver，提前终止循环
```

#### 步骤 3：Q1 与去重逻辑

**代码位置**：`src/handler/http/request/traces/mod.rs:1380-1427`

```rust
// 去重：过滤已见过的 trace_id
let deduped_hits: Vec<_> = agg_res
    .hits
    .into_iter()
    .filter(|item| {
        let tid = item.get("trace_id").and_then(|v| v.as_str()).unwrap_or_default();
        !tid.is_empty() && !seen_trace_ids.contains(tid)
    })
    .collect();

// 使用去重后的数量进行分页统计
let partition_total = deduped_hits.len() as i64;
hits_seen += partition_total;

// ⚠️  强制按 trace_start_time 降序排序
// 注意：这里覆盖了 Q1 SQL 中的 ORDER BY 子句结果
let mut sorted_hits = deduped_hits;
sorted_hits.sort_by(|a, b| {
    let a_t = json::get_int_value(a.get("trace_start_time").unwrap_or_default());
    let b_t = json::get_int_value(b.get("trace_start_time").unwrap_or_default());
    b_t.cmp(&a_t)
});

// 关键：注册所有 trace_id 到 seen_trace_ids（包括被跳过的）
// 防止跨分区重复
for item in &sorted_hits {
    if let Some(tid) = item.get("trace_id").and_then(|v| v.as_str())
        && !tid.is_empty()
    {
        seen_trace_ids.insert(tid.to_string());
    }
}
```

#### 步骤 4：全局 offset 应用

**代码位置**：`src/handler/http/request/traces/mod.rs:1429-1448`

```rust
let hits_seen_before = hits_seen - partition_total;
let skip_in_partition = (hits_to_skip - hits_seen_before)
    .max(0)
    .min(sorted_hits.len() as i64);
let need = (hits_to_deliver - hits_delivered) as usize;

// 仅提取本分区中需要交付的 trace
// ⚠️  skip/take 是基于 trace_start_time 排序后的结果执行的
let deliverable_q1: Vec<_> = sorted_hits
    .into_iter()
    .skip(skip_in_partition as usize)
    .take(need)
    .collect();
```

#### 步骤 5：Q2a/Q2b 与结果发送

与 latest 逻辑类似，但：
- Q2a/Q2b 仅查询 `deliverable_q1` 中的 trace_id
- 时间窗口可能扩展以覆盖 trace 的实际 span 时间范围
- 结果通过 SSE 流式发送给客户端

### 5.4 seen_trace_ids 去重机制详解

#### 5.4.1 为什么需要去重

当一个 trace 的 span 分布在多个时间分区时，每个分区的 Q1 `GROUP BY trace_id` 都会返回该 trace，导致重复。

```
时间分区 1: [10:00, 11:00) ──┐
                              ├─ trace_A (span1 在分区1, span2 在分区2)
时间分区 2: [11:00, 12:00) ──┘
```

如果不去重，trace_A 会在两个分区的结果中各出现一次。

#### 5.4.2 去重时机

**时机 1：Q1 结果过滤**（`mod.rs:1383-1393`）
- 过滤掉已经在 `seen_trace_ids` 中的 trace
- 使用去重后的数量更新 `hits_seen`，避免重复计数影响分页

**时机 2：提前注册所有 trace_id**（`mod.rs:1421-1427`）
- **即使被 offset 跳过的 trace 也要注册**
- 防止后续分区重复返回同一个 trace
- 保证全局去重的正确性

#### 5.4.3 对分页的影响

```
用户请求: from=20, size=10

分区 1 Q1 返回 15 条 → 去重后 12 条
  ├─ 注册 12 个 trace_id 到 seen_trace_ids
  ├─ skip_in_partition = min(20, 12) = 12
  ├─ deliverable_q1 = 0（全部跳过）
  └─ hits_seen = 12, hits_delivered = 0

分区 2 Q1 返回 15 条 → 去重后 10 条（2 条已在分区1见过）
  ├─ 注册 10 个 trace_id 到 seen_trace_ids
  ├─ skip_in_partition = max(0, 20 - 12) = 8
  ├─ deliverable_q1 = 10.skip(8).take(10) = 2 条
  └─ hits_seen = 22, hits_delivered = 2

分区 3 Q1 返回 15 条 → 去重后 11 条（4 条已见过）
  ├─ 注册 11 个 trace_id 到 seen_trace_ids
  ├─ skip_in_partition = max(0, 20 - 22) = 0
  ├─ deliverable_q1 = 11.skip(0).take(8) = 8 条
  └─ hits_seen = 33, hits_delivered = 10（达到 size，终止）
```

#### 5.4.4 对过滤结果的影响

- `filter` 参数在 Q1 的 WHERE 子句中应用
- 去重发生在 Q1 结果返回后，不影响过滤条件的执行
- 如果一个 trace 满足过滤条件，它的所有跨分区 span 都会被正确去重
- `total_across_partitions` 统计的是去重后的总数，用于 UI 显示 "N of M"

### 5.5 非时间排序场景下的结果正确性分析

#### 5.5.1 问题背景

当 `sort_by` 为非时间字段（如 `duration`）时，代码执行流程存在排序不一致的问题：

```
用户请求: sort_by=duration, sort_order=DESC, from=20, size=10

1. Q1 SQL 构建（mod.rs:1212
   └─ SELECT ... GROUP BY trace_id ORDER BY zo_sql_duration DESC

2. Q1 返回结果：按 duration 降序排列的 trace 列表

3. ⚠️  强制重排（mod.rs:1410-1415）
   └─ 代码强制将 Q1 结果按 trace_start_time 降序重排
   └─ 注释声称 "Q1 returns traces ordered by zo_sql_timestamp DESC"，但这只在 sort_by=start_time 时成立

4. skip/take 分页（mod.rs:1436-1440）
   └─ 基于 trace_start_time 排序后的结果执行 skip(20).take(10)

5. Q2a/Q2b 查询详情

6. 最终排序（mod.rs:1751-1766）
   └─ 按用户指定的 duration 降序重新排序
   └─ 返回给客户端
```

#### 5.5.2 正确性问题

**问题根源**：skip/take 分页是在**按 trace_start_time 排序后**执行的，而不是在**按用户指定的 duration 排序后**执行的。

**示例**：
假设 Q1 返回 30 条 trace，按 duration 降序排列：

```
Q1 返回（按 duration 排序）: [T1(dur=1000), T2(dur=900), ..., T20(dur=500), ..., T30(dur=100)

强制重排为按 start_time 排序: [T20(start=最新), T5(start=次新), ..., T1(start=最早)

skip(20).take(10) → 取按 start_time 排序的第 21-30 条
  ↓
这些 trace 按 duration 重排后返回给用户
```

**结果**：用户请求按 duration 排序取第 21-30 条，但实际返回的是**按 start_time 排序的第 21-30 条，再按 duration 排序**的结果。这与用户期望的"按 duration 排序的第 21-30 条"不一致。

#### 5.5.3 单分区降级机制

**代码位置**：`src/handler/http/request/traces/mod.rs:1304-1313`

```rust
} else {
    // order by other fields can't be multiple partitions
    log::info!(
        "[TRACES_STREAM trace_id {trace_id}] sort_by non-timestamp ({sql_order_expr}), \
        forcing single partition [{}, {}]",
        partition_req.start_time,
        partition_req.end_time,
    );
    vec![[partition_req.start_time, partition_req.end_time]]
};
```

当 sort_by 不是时间字段时，代码强制合并为**单个分区**查询。这是因为：
- 跨分区排序无法保证全局正确性
- 单分区下，虽然排序和分页在同一数据集上执行
- 但单分区仍存在上述的排序不一致问题

#### 5.5.4 影响范围

| 排序字段 | 多分区支持 | 排序正确性 | 分页正确性 |
|---------|-----------|-----------|-----------|
| start_time / _timestamp | ✅ 是 | ✅ 正确 | ✅ 正确 |
| duration | ❌ 否（强制单分区） | ⚠️  部分正确（存在排序不一致问题） | ⚠️  不正确（分页基于 start_time 排序） |

### 5.6 from 分页稳定性边界

#### 5.6.1 稳定性问题背景

**代码注释**：`src/handler/http/request/traces/mod.rs:1315-1321`

```rust
// `from` is a global offset: skip the first `from` hits across all partitions,
// then deliver `size` hits. We track how many we've seen and delivered so far.
//
// NOTE: partition boundaries come from search_partition() which is sensitive to querier
// node count. Boundaries can shift between requests, so `from`-based pagination may
// produce overlapping or missing results if cluster topology changes between page requests.
// For stable pagination, callers should use time-based cursors (end_time of last seen trace).
```

#### 5.6.2 分区边界计算

`search_partition()` 返回的分区边界对 querier 节点数量敏感：
- 分区算法可能根据 querier 数量动态调整分区大小和数量
- 当 querier 节点增加/减少时，分区边界会重新计算

#### 5.6.3 稳定性边界

| 场景 | 稳定性 | 说明 |
|------|--------|------|
| 同一请求内 | ✅ 稳定 | 分区列表只获取一次，from 分页在同一请求内是稳定的 |
| 跨请求分页（集群拓扑不变） | ⚠️  基本稳定 | 如果 querier 数量不变，分区边界不变 |
| 跨请求分页（集群拓扑变化） | ❌ 不稳定 | querier 数量变化导致分区边界移动 |

#### 5.6.4 失败模式

当 querier 拓扑变化时的失败模式：

**失败模式 1：结果重叠
```
请求 1（querier=3 个节点）: 分区 [10:00-11:00, 11:00-12:00, 12:00-13:00
  └─ 返回 trace_A 在分区 11:00-12:00
请求 2（querier=2 个节点）: 分区 [10:00-11:30, 11:30-13:00
  └─ trace_A 现在在分区 10:00-11:30
  └─ 如果 trace_A 落在请求 2 的分页范围内，会重复返回
```

**失败模式 2：结果遗漏**
```
请求 1: 分区 [10:00-11:00, 11:00-12:00
  └─ trace_B 在分区 11:00-12:00（在请求 1 的第二页
请求 2: 分区 [10:00-11:30, 11:30-13:00
  └─ trace_B 现在在分区 10:00-11:30
  └─ 如果 trace_B 落在请求 2 的第一页，但用户请求第二页时会遗漏
```

#### 5.6.5 推荐方案

使用**基于时间的游标**（Time-based Cursor）：
- 不要依赖 `from` 参数进行分页
- 使用最后一条返回结果的 `end_time` 作为下一页的 `start_time`
- 配合 `seen_trace_ids` 去重机制避免重复
- 示例：`end_time = last_trace.end_time

### 5.7 latest_stream 与 latest 的对比

| 对比项 | latest | latest_stream |
|--------|--------|---------------|
| 查询范围 | 全时间范围一次性查询 | 按时间分区逐个查询 |
| 返回方式 | 完整结果一次性返回 | SSE 流式增量返回 |
| 去重方式 | 全局 GROUP BY 自然去重 | 服务端 seen_trace_ids HashSet 去重 |
| 客户端去重责任 | 无 | 无（服务端已去重） |
| 首屏时间 | 较慢（需等待全量查询） | 快（第一个分区返回即可显示） |
| 内存占用 | 高（全量结果在内存） | 低（仅单分区结果 + seen_trace_ids） |
| 适用场景 | 小时间范围、精确查询 | 大时间范围、探索性查询 |
| 分页实现 | SQL LIMIT/OFFSET | 服务端 hits_seen/hits_delivered 计数 |
| 时间排序支持 | ✅ 完全支持 | ✅ 完全支持 |
| 非时间排序支持 | ✅ 支持 | ❌ 强制单分区 + 排序不一致问题 |
| 分页稳定性（同请求） | ✅ 稳定 | ✅ 稳定 |
| 分页稳定性（跨请求） | ⚠️  依赖底层存储 | ❌ 受 querier 拓扑变化影响 |
| Q1/Q2a/Q2b 逻辑 | 完全相同 | 完全相同（仅缩小到单个分区） |
| 结果完整性 | 完整去重 | 完整去重（服务端 seen_trace_ids 保证） |

### 5.8 latest_stream 统一结论：去重、排序、分页的协同机制

```
┌─────────────────────────────────────────────────────────────────────────┐
│                  latest_stream 统一执行模型                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  输入: sort_by, from, size, start_time, end_time                          │
│    │                                                                      │
│    ├─ [优化] 从 trace_id 提取时间戳缩小范围                               │
│    │                                                                      │
│    ├─ 分区边界获取（search_partition）                                      │
│    │   └─ ⚠️  分区边界对 querier 节点数量敏感                                │
│    │                                                                      │
│    ├─ 排序方向决定分区遍历顺序                                              │
│    │   ├─ start_time DESC: 按分区默认顺序（新→旧）                         │
│    │   ├─ start_time ASC:  反转分区顺序（旧→新）                           │
│    │   └─ 非时间排序:      强制单分区（无法跨分区排序）                    │
│    │                                                                      │
│    ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                    分区循环（按顺序遍历）                               │  │
│  │                                                                       │  │
│  │  Q1: 分区内 GROUP BY trace_id 聚合（带 ORDER BY {sort_expr}）          │  │
│  │    │                                                                  │  │
│  │    ├─ 去重过滤: seen_trace_ids 过滤已见过的 trace                       │  │
│  │    │   └─ partition_total = 去重后数量                                 │  │
│  │    │                                                                  │  │
│  │    ├─ ⚠️  强制重排: 按 trace_start_time 降序重排（覆盖 Q1 的 ORDER BY） │  │
│  │    │   └─ 这是排序不一致问题的根源                                      │  │
│  │    │                                                                  │  │
│  │    ├─ 注册所有 trace_id 到 seen_trace_ids（包括被跳过的）              │  │
│  │    │   └─ 防止后续分区重复                                              │  │
│  │    │                                                                  │  │
│  │    ├─ 应用全局 offset: skip/take（基于 start_time 排序结果）           │  │
│  │    │   └─ ⚠️  非时间排序场景下分页结果不正确                              │  │
│  │    │                                                                  │  │
│  │    ├─ Q2a: span 统计（仅 deliverable_q1 的 trace_id）                │  │
│  │    ├─ Q2b: 多服务详情（可选）                                           │  │
│  │    │                                                                  │  │
│  │    ├─ 最终排序: 按用户指定的 sort_by 重新排序                           │  │
│  │    └─ SSE 发送结果                                                     │  │
│  │                                                                       │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  输出: 流式 trace 列表 + 进度事件 + Done 事件                              │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 5.8.1 核心机制协同关系

| 机制 | 作用 | 与其他机制的交互 |
|------|------|-----------------|
| **seen_trace_ids 去重** | 跨分区去重，避免重复结果 | 在 Q1 后过滤，影响 hits_seen 计数，进而影响分页 |
| **start_time 强制重排** | 统一排序基准，保证分区遍历顺序正确 | 覆盖 Q1 的 ORDER BY，是 skip/take 分页的基础，但导致非时间排序不一致 |
| **全局 offset 计数** | 实现跨分区的 from/size 分页 | 依赖去重后的 hits_seen 计数，基于 start_time 排序结果执行 |
| **分区边界** | 决定数据分片方式 | 对 querier 拓扑敏感，影响跨请求分页稳定性 |

#### 5.8.2 已知问题与边界

1. **非时间排序一致性问题**
   - 原因：skip/take 基于 start_time 排序执行，而非用户指定的排序字段
   - 影响：非时间排序的分页结果与用户期望不一致
   - 缓解：强制单分区查询，但仍存在排序不一致

2. **跨请求分页稳定性问题**
   - 原因：分区边界对 querier 节点数量敏感
   - 影响：集群拓扑变化时，分页结果可能重叠或遗漏
   - 缓解：使用基于时间的游标（end_time of last seen trace）

3. **fetch_size 限制**
   - 原因：Q1 的 fetch_size 被限制为 `query_default_limit`（默认 1000）
   - 影响：如果单个分区内去重后的 trace 数量超过 fetch_size，会导致结果截断
   - 边界：`remaining_to_skip + remaining_needed` 超过 1000 时可能漏数

#### 5.8.3 最佳实践

| 场景 | 推荐做法 |
|------|---------|
| 时间排序分页 | ✅ 使用 start_time 排序，from/size 分页基本稳定 |
| 非时间排序 | ⚠️  优先使用 latest 接口，或接受单分区限制 |
| 大时间范围 | ✅ 使用 latest_stream 流式接口 |
| 跨请求分页 | ⚠️  避免 from 分页，使用时间游标（end_time 作为下一页 start_time） |
| 精确查询 | ✅ 使用 trace_id 精确查询 |

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
6. **服务端全局去重**：latest_stream 通过 `seen_trace_ids` HashSet 实现跨分区去重，客户端无需处理
7. **提前注册去重**：即使被 offset 跳过的 trace_id 也注册到 `seen_trace_ids`，保证后续分区不重复
8. **非时间排序降级**：按 duration 等非时间字段排序时，强制合并为单个分区查询（但仍存在排序不一致问题）
9. **动态 fetch_size**：根据 `remaining_to_skip + remaining_needed` 动态调整 Q1 查询大小，避免过度扫描

### 7.3 已知设计权衡与边界

| 设计决策 | 收益 | 代价 | 适用场景 |
|---------|------|------|---------|
| **start_time 强制重排** | 统一排序基准，简化跨分区分页逻辑 | 非时间排序场景下分页结果不正确 | 时间排序为主的场景 |
| **seen_trace_ids  HashSet 去重** | 服务端全局去重，客户端无需处理 | 内存占用随去重 trace 数量增长 | 跨分区 trace 较多的场景 |
| **非时间排序强制单分区** | 避免跨分区排序的复杂性 | 大时间范围查询性能下降，且仍存在排序不一致 | 非时间排序需求较少的场景 |
| **from 基于计数分页** | 接口简单，与 latest 保持一致 | 跨请求时受 querier 拓扑变化影响 | 单次查询或集群稳定的场景 |

### 7.4 关于 TraceListIndex 的说明
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
