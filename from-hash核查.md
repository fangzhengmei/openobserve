# from 与哈希键关系核查

## 一、完整调用链逐步对照

### 步骤1：HTTP 请求入参

**入口函数** (`handler/http/request/search/mod.rs:235-241`):

```rust
pub async fn search(
    Path(org_id): Path<String>,
    Headers(user_email): Headers<UserEmail>,
    headers: HeaderMap,
    Query(url_query): Query<HashMap<String, String>>,
    Json(mut req): Json<Request>,   // ← 请求体反序列化为 Request
) -> Response {
```

**前端发送的 JSON 请求体结构**:

```json
{
  "query": {
    "sql": "SELECT * FROM logs WHERE level='error' ORDER BY _timestamp DESC",
    "start_time": 1000000,
    "end_time": 2000000,
    "from": 100,
    "size": 100
  },
  "encoding": ""
}
```

**代码证据** (`config/src/meta/search.rs:110-148`):

```rust
pub struct Query {
    pub sql: String,         // ← SQL 文本，独立字段
    pub start_time: i64,
    pub end_time: i64,
    #[serde(default)]
    pub from: i64,           // ← 分页偏移，独立字段，默认 0
    #[serde(default = "default_size")]
    pub size: i64,           // ← 每页大小，独立字段
    // ...
}
```

**结论1**：`sql` 和 `from` 是 `Query` 结构体的**两个独立字段**，前端传入时分别放置，`sql` 字段不含 `OFFSET` 子句。

---

### 步骤2：Request 解码

**decode 方法** (`config/src/meta/search.rs:178-200`):

```rust
impl Request {
    pub fn decode(&mut self) -> Result<(), std::io::Error> {
        match self.encoding {
            RequestEncoding::Base64 => {
                let decoded = base64::decode_url(&self.query.sql)?;
                self.query.sql = replace_o2_custom_patterns(&decoded).unwrap_or(decoded);
            }
            RequestEncoding::Empty => {}
        }
        self.encoding = RequestEncoding::Empty;
        Ok(())
    }
}
```

**Handler 层调用** (`handler/http/request/search/mod.rs:290-292`):

```rust
if let Err(e) = req.decode() {
    return MetaHttpResponse::bad_request(e);
}
```

**decode 只做两件事**：
1. Base64 解码 `query.sql`（如果编码类型是 Base64）
2. 替换 o2 自定义模式

**不涉及 `from`，不向 SQL 注入任何子句。**

**Handler 后续对 SQL 的修改** (`handler/http/request/search/mod.rs:304-305`):

```rust
if let Ok(sql) = replace_o2_custom_patterns(&req.query.sql) {
    req.query.sql = sql;
};
```

**也只是模式替换，不注入 OFFSET。**

---

### 步骤3：进入 cache::search

**调用入口** (`handler/http/request/search/mod.rs:486-496`):

```rust
let res = SearchService::cache::search(
    &trace_id, &org_id, stream_type, Some(user_id.to_string()),
    &req,          // ← req.query.from 和 req.query.sql 完整传入
    range_error,
    false, dashboard_info, is_multi_stream_search,
).await
```

**cache::search 函数签名** (`cache/mod.rs:79-89`):

```rust
pub async fn search(
    trace_id: &str, org_id: &str, stream_type: StreamType,
    user_id: Option<String>,
    in_req: &search::Request,   // ← 完整的 Request 引用
    range_error: String,
    is_http2_streaming: bool,
    dashboard_info: Option<DashboardInfo>,
    is_multi_stream_search: bool,
) -> Result<search::Response, Error>
```

**函数内部** (`cache/mod.rs:90-122`):

```rust
let use_cache = if in_req.query.from == 0 {   // ← 读取 in_req.query.from
    in_req.use_cache
} else {
    false
};

let mut req = in_req.clone();                   // ← clone 整个 Request

// ...

let (mut c_resp, should_exec_query) =
    prepare_cache_response(trace_id, org_id, stream_type, &mut req, use_cache).await?;
```

**结论2**：`in_req.query.from` 仅用于 `use_cache` 门控判断，不修改 SQL。

---

### 步骤4：prepare_cache_response 内部

**函数签名** (`cache/mod.rs:497-504`):

```rust
pub async fn prepare_cache_response(
    trace_id: &str, org_id: &str, stream_type: StreamType,
    req: &mut search::Request,    // ← 可变引用，可能修改
    use_cache: bool,
) -> Result<(MultiCachedQueryResponse, bool), Error>
```

#### 4a. origin_sql 的来源

```rust
// cache/mod.rs:505
let mut origin_sql = req.query.sql.clone();
```

`origin_sql` 直接取自 `req.query.sql`。此时 `req` 是 `in_req.clone()`，`req.query.sql` 与前端传入的 SQL **完全相同**，不含 OFFSET。

#### 4b. SQL 解析过程

```rust
// cache/mod.rs:544-553
let query: SearchQuery = req.query.clone().into();
let sql = match crate::service::search::Sql::new(&query, org_id, stream_type, req.search_type)
    .await
{
    Ok(v) => v,
    Err(e) => { return Ok((MultiCachedQueryResponse::default(), true)); }
};
```

`Sql::new` 内部 (`sql/mod.rs:133-134`):

```rust
let sql = query.sql.clone();      // ← SQL 原文
let offset = query.from as i64;   // ← from 存为 offset，不修改 sql
let mut limit = query.size as i64;
```

**SQL 经过的 Visitor 变换** (`sql/mod.rs:156-281`):

| Visitor | 作用 | 是否修改 SQL 中的 LIMIT/OFFSET |
|---------|------|-------------------------------|
| `TrackTotalHitsVisitor` | 重写 track_total_hits | 否 |
| `RemoveDashboardAllVisitor` | 移除 DASHBOARD_ALL 过滤 | 否 |
| `MatchAllRawVisitor` | 重写 match_all_raw | 否 |
| `AddTimestampVisitor` | 添加 _timestamp 到 SELECT | 否（添加列，不加 OFFSET） |
| `AddO2IdVisitor` | 添加 _o2_id 到 SELECT | 否 |
| `ColumnVisitor` | 提取列名、别名、group_by、order_by、limit | 否（只读取，不修改） |

**`ColumnVisitor` 解析 OFFSET 但不写入 Sql.offset** (`sql/visitor/column.rs:158-179`):

```rust
// ColumnVisitor 能从 SQL 中解析 LIMIT/OFFSET
if let Some(sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. }) = limit_clause {
    // 解析 limit 值
    // 如果 SQL 中有 OFFSET，解析 offset 值存入 self.offset
}
```

但在 `Sql::new_with_options` 中，`Sql.offset` 的赋值是：

```rust
// sql/mod.rs:134
let offset = query.from as i64;   // ← 直接用 query.from，不用 column_visitor.offset

// sql/mod.rs:301
offset,                            // ← 写入 Sql 结构体
```

**`column_visitor.offset` 的值从未被赋给 `Sql.offset`。** `Sql.offset` 始终等于 `query.from`。

**`Sql.sql` 字段** (`sql/mod.rs:290`):

```rust
sql: statement.to_string(),    // ← AST 序列化后的 SQL 文本
```

`statement` 经过了 Visitor 变换，但这些 Visitor 都不会添加 OFFSET 子句。因此 `Sql.sql` 中不会出现 `OFFSET from_value`。

#### 4c. origin_sql 的修改（哈希计算前）

在哈希计算前，`origin_sql` 只有一种可能的修改：

```rust
// cache/mod.rs:557-572
if is_aggregate && sql.histogram_interval.is_some() {
    cacher::handle_histogram(&mut origin_sql, q_time_range, req.query.histogram_interval);
}
```

`handle_histogram` (`cache/cacher.rs:910-948`) 只替换 `histogram(...)` 函数调用的间隔参数：

```rust
*origin_sql = origin_sql.replace(
    caps.get(0).unwrap().as_str(),
    &format!("histogram({field},'{interval}')"),
);
```

**不涉及 OFFSET。**

#### 4d. 哈希计算

```rust
// cache/mod.rs:574-593
let mut hash_body = vec![
    CACHE_VERSION.to_string(),      // "v3"
    origin_sql.to_string(),         // ← SQL 文本（经 histogram 规范化后）
    req.query.size.to_string(),     // ← size（来自 query.size）
];
if let Some(vrl_function) = &query_fn {
    hash_body.push(vrl_function.to_string());
}
if let Some(action_id) = action {
    hash_body.push(action_id.to_string());
}
if !req.regions.is_empty() {
    hash_body.extend(req.regions.clone());
}
if !req.clusters.is_empty() {
    hash_body.extend(req.clusters.clone());
}
let mut h = config::utils::hash::gxhash::new();
let hashed_query = h.sum64(&hash_body.join(","));
```

**遍历 hash_body 的全部元素**：
1. `CACHE_VERSION` — 固定值 "v3"
2. `origin_sql` — SQL 文本，**不含 OFFSET**
3. `req.query.size` — 每页大小
4. `query_fn` — VRL 函数（可选）
5. `action_id` — 动作 ID（可选）
6. `regions` — 区域列表（可选）
7. `clusters` — 集群列表（可选）

**`req.query.from` 不在 hash_body 中。**

#### 4e. origin_sql 的修改（哈希计算后）

```rust
// cache/mod.rs:600-617
if !is_aggregate && origin_sql.contains('*') {
    ts_column = TIMESTAMP_COL_NAME.to_string();
} else if !is_aggregate && sql.group_by.is_empty() && sql.order_by.is_empty()
    && !origin_sql.contains('*') && let Some(caps) = RE_SELECT_FROM.captures(&origin_sql)
    && let Some(cap) = caps.get(1) {
    let cap_str = cap.as_str();
    if !cap_str.contains(TIMESTAMP_COL_NAME) {
        origin_sql = origin_sql.replacen(cap_str, &format!("{TIMESTAMP_COL_NAME},{cap_str}"), 1);
        req.query.sql = origin_sql.clone();
    }
}
```

这些修改发生在哈希计算**之后**，不影响哈希值。修改内容是向 SELECT 子句添加 `_timestamp` 列，**不涉及 OFFSET**。

---

### 步骤5：SQL 文本进入缓存层时是否携带 OFFSET 的最终判定

**逐环节排查**：

| 环节 | 代码位置 | 是否可能注入 OFFSET | 证据 |
|------|---------|--------------------|----|
| 前端请求 | `handler/.../search/mod.rs:240` | 否 | `sql` 和 `from` 是 Query 的独立字段 |
| Request::decode | `config/meta/search.rs:178-200` | 否 | 只做 base64 解码和模式替换 |
| Handler 模式替换 | `handler/.../search/mod.rs:304` | 否 | 只做 o2 模式替换 |
| Sql::new_with_options | `sql/mod.rs:133-313` | 否 | `offset=query.from`，不修改 SQL 文本；Visitor 不注入 OFFSET |
| handle_histogram | `cache/cacher.rs:910-948` | 否 | 只替换 histogram 间隔 |
| 哈希后的修改 | `cache/mod.rs:600-617` | 否 | 只添加 _timestamp 列 |

**最终判定：SQL 文本在进入缓存层时不可能携带 OFFSET。`OFFSET` 从未被注入到 SQL 字符串中。**

---

### 步骤6：query.from 的完整传递路径

```
前端 JSON: { "query": { "sql": "...", "from": 100, "size": 100 } }
    ↓
Request 反序列化: req.query.from = 100, req.query.sql = "..."（不含 OFFSET）
    ↓
cache::search:
    use_cache = (in_req.query.from == 0) ? in_req.use_cache : false
    ↓
prepare_cache_response:
    origin_sql = req.query.sql.clone()      ← 不含 OFFSET
    query: SearchQuery = req.query.clone().into()
    sql = Sql::new(&query, ...)             ← sql.offset = query.from = 100
    hash_body = [version, origin_sql, size] ← 不含 from
    hashed_query = gxhash64(hash_body)      ← from 不影响哈希
    ↓
use_cache? ──── false (from>0) ──→ MultiCachedQueryResponse::default()
    │                                       cache_query_response = false
    │ true (from=0)
    ↓
check_cache(...) → 计算delta → 返回缓存响应
    ↓
delta查询执行:
    req.query.from = 100（保持不变）
    req.query.sql = "..."（保持不变）
    SearchService::search(&trace_id, ..., &req) → 底层查询引擎
    ↓
底层 Sql::new_with_options:
    offset = query.from = 100
    AddSortAndLimitRule::new(limit, offset=100)
    → DataFusion 逻辑计划注入 OFFSET 100
    ↓
cluster/http.rs:
    result = search::Response::new(sql.offset=100, sql.limit=100)
    → 返回给前端的结果中 from=100
```

**结论3：query.from 仅通过 `Sql.offset` 字段和 `AddSortAndLimitRule` 传递，在 DataFusion 逻辑计划优化阶段生效，从不进入 SQL 文本。**

---

## 二、from=0 与 from>0 的读缓存/写缓存/排序对齐证据

### 2.1 读缓存证据

**from=0 路径** (`cache/mod.rs:633-647`):

```rust
if use_cache {
    check_cache(
        trace_id, org_id, req, &mut origin_sql, &file_path,
        is_aggregate, &sql, &ts_column, is_descending,
        &mut should_exec_query,
    ).await
}
```

`check_cache` (`cache/cacher.rs:415-449`):
1. 从内存中读取缓存元数据
2. 筛选时间范围重叠的缓存文件
3. 按策略选择最优缓存
4. 调用 `invalidate_cached_response_by_stream_min_ts` 过滤过期缓存
5. 计算 delta 时间范围
6. 返回 `MultiCachedQueryResponse`，其中 `cache_query_response = true`

**from>0 路径** (`cache/mod.rs:648-658`):

```rust
else {
    MultiCachedQueryResponse {
        ts_column,
        is_aggregate,
        is_descending,
        order_by: sql.order_by,
        limit: sql.limit,
        file_path,
        ..Default::default()   // cached_response=[], deltas=[], has_cached_data=false, cache_query_response=false
    }
}
```

**Default 值** (`common/meta/search.rs:60-76`):

```rust
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, Default)]
pub struct MultiCachedQueryResponse {
    pub cached_response: Vec<CachedQueryResponse>,     // Default: []
    pub deltas: Vec<QueryDelta>,                       // Default: []
    pub has_cached_data: bool,                         // Default: false
    pub cache_query_response: bool,                    // Default: false ← 关键！
    // ...
}
```

**读缓存差异**：

| 字段 | from=0 | from>0 |
|------|--------|--------|
| 是否调用 check_cache | ✅ 是 | ❌ 否 |
| cached_response | 有缓存数据 | `[]` 空 |
| has_cached_data | 可能为 true | `false` |
| deltas | 计算增量范围 | `[]` 空 |
| cache_query_response | `true` | `false` |
| ts_column | ✅ 相同 | ✅ 相同 |
| is_descending | ✅ 相同 | ✅ 相同 |
| order_by | ✅ 相同 | ✅ 相同 |
| limit | ✅ 相同 | ✅ 相同 |

**对齐点**：元数据（ts_column、is_descending、order_by、limit）在两条路径中完全相同，因为这些值来自**相同的 SQL 解析结果**。

### 2.2 写缓存证据

**写缓存条件** (`cache/mod.rs:435-442`):

```rust
let should_cache_results = cfg.common.result_cache_enabled
    && !is_http2_streaming
    && should_exec_query
    && c_resp.cache_query_response        // ← from=0 时为 true，from>0 时为 false
    && res.new_start_time.is_none()
    && res.new_end_time.is_none()
    && res.function_error.is_empty()
    && !res.hits.is_empty();
```

**from=0 时**：`c_resp.cache_query_response = true`（在 `check_cache` 内设置），满足条件时调用 `write_results`。

**from>0 时**：`c_resp.cache_query_response = false`（Default 值），条件始终不满足，**永不写入缓存**。

**write_results 函数签名** (`cache/mod.rs:461-473`):

```rust
write_results(
    trace_id, &c_resp.ts_column,
    req.query.start_time, req.query.end_time,
    deep_copy_response(&res),
    file_path,               // ← 缓存文件路径（基于 hashed_query）
    is_aggregate,
    c_resp.is_descending,
    req.clear_cache,
    is_histogram_non_ts_order,
).await
```

**注意**：`write_results` 中的 `file_path` 基于 `hashed_query`，而 `hashed_query` 不受 `from` 影响。这意味着 from=0 写入的缓存，在逻辑上 from=100 的查询也可以访问到（如果使用缓存的话），但 from>0 路径直接绕过了缓存。

### 2.3 排序对齐证据

#### from=0 且有缓存数据时

```rust
// cache/mod.rs:273-287
if c_resp.has_cached_data {
    merge_response(
        trace_id,
        &mut c_resp.cached_response.iter().map(|r| r.cached_response.clone()).collect(),
        &mut results,           // delta 查询结果
        &c_resp.ts_column,
        c_resp.limit,
        c_resp.is_descending,
        c_resp.took,
        c_resp.order_by,        // ← 排序参数
    )
}
```

`merge_response` 内部 (`cache/mod.rs:768`):

```rust
sort_response(is_descending, &mut cache_response, ts_column, &order_by);
```

#### from=0 无缓存数据 或 from>0 时

```rust
// cache/mod.rs:288-297
else {
    let mut reps = results[0].clone();
    sort_response(
        c_resp.is_descending,    // ← 与 merge_response 内部使用的相同
        &mut reps,
        &c_resp.ts_column,
        &c_resp.order_by,        // ← 与 merge_response 内部使用的相同
    );
    reps
}
```

**sort_response 实现** (`cache/mod.rs:830-899`):

```rust
fn sort_response(
    is_descending: bool,
    cache_response: &mut Response,
    ts_column: &str,
    in_order_by: &Vec<(String, OrderBy)>,
) {
    let order_by = if in_order_by.is_empty() {
        &vec![(ts_column.to_string(), if is_descending { OrderBy::Desc } else { OrderBy::Asc })]
    } else {
        in_order_by
    };

    cache_response.hits.sort_by(|a, b| {
        for (field, order) in order_by {
            let cmp = if ts_column == field {
                let a_ts = get_ts_value(ts_column, a);
                let b_ts = get_ts_value(ts_column, b);
                a_ts.partial_cmp(&b_ts).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                // 非时间列比较...
            };
            let final_cmp = if order == &OrderBy::Desc { cmp.reverse() } else { cmp };
            if final_cmp != std::cmp::Ordering::Equal { return final_cmp; }
        }
        std::cmp::Ordering::Equal
    });
}
```

**排序对齐证据**：

1. **from=0 (有缓存)** 和 **from=0 (无缓存)** 和 **from>0** 三条路径使用**完全相同的 `sort_response` 函数**
2. `is_descending` 和 `order_by` 来自 `c_resp`，而 `c_resp` 的这些字段在两条路径中**完全相同**（见 2.1 节对照表）
3. `sort_response` 的排序逻辑只依赖 `order_by` 和 `is_descending`，不依赖 `from`

**但需注意**：from>0 路径中，底层 DataFusion 查询引擎已通过 `AddSortAndLimitRule` 执行了排序和偏移。`sort_response` 是对**已从引擎返回的 hits** 再次排序。对于 from=0 路径，merge_response 对合并后的数据也需要排序以保证跨来源数据顺序正确。

---

## 三、哈希键是否受 query.from 影响的最终结论

### 结论：哈希键不受 query.from 影响

### 证据汇总

| 编号 | 证据 | 代码位置 | 说明 |
|------|------|---------|------|
| E1 | hash_body 不含 from | `cache/mod.rs:575-591` | 遍历 hash_body 全部7个元素，无 `req.query.from` |
| E2 | origin_sql 取自 req.query.sql | `cache/mod.rs:505` | SQL 文本从前端传入后始终不含 OFFSET |
| E3 | Request::decode 不注入 OFFSET | `config/meta/search.rs:178-200` | 只做 base64 解码和模式替换 |
| E4 | Sql::new 内 Visitor 不注入 OFFSET | `sql/mod.rs:156-281` | 5个 Visitor 只修改 SELECT/WHERE，不修改 LIMIT/OFFSET |
| E5 | Sql.offset = query.from，不修改 SQL 文本 | `sql/mod.rs:134,301` | offset 独立存储，不写入 SQL 字符串 |
| E6 | handle_histogram 不注入 OFFSET | `cache/cacher.rs:910-948` | 只替换 histogram 间隔参数 |
| E7 | 哈希后修改不含 OFFSET | `cache/mod.rs:600-617` | 只添加 _timestamp 列 |

### 相同 SQL + 不同 from 的实际影响

```
查询A: sql="SELECT * FROM logs ORDER BY _timestamp DESC", from=0, size=100
查询B: sql="SELECT * FROM logs ORDER BY _timestamp DESC", from=100, size=100

hash_body_A = ["v3", "SELECT * FROM logs ORDER BY _timestamp DESC", "100"]
hash_body_B = ["v3", "SELECT * FROM logs ORDER BY _timestamp DESC", "100"]
                                                      ↑ 完全相同 ↑

hashed_query_A = hashed_query_B  → 同一缓存目录

但查询B (from=100) 的 use_cache=false，不读取也不写入缓存
→ 不存在缓存冲突
```

### 为什么不会缓存冲突

```
缓存目录: org/default/logs/{hashed_query}/

from=0 查询A:
  ✅ 读取缓存 → 命中或计算delta → 合并结果 → 写入缓存
  → 缓存文件: 100000_200000_0_1.json

from=100 查询B:
  ❌ use_cache=false → 不调用 check_cache → 不读取缓存
  ❌ cache_query_response=false → 不写入缓存
  → 缓存目录无变化

两条路径对缓存目录的访问完全不交叉：
  from=0: 读 + 写
  from>0: 不读 + 不写
```

---

## 四、from 值在整条链路中的完整生命周期

```
┌─────────────────────────────────────────────────────┐
│ 前端 JSON: query.from = 100                          │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│ Request::decode (config/meta/search.rs:178)          │
│ → 不修改 from，不向 SQL 注入 OFFSET                  │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│ cache::search (cache/mod.rs:94)                      │
│ → use_cache = (from == 0) ? use_cache : false        │
│ → from=100 → use_cache=false                         │
└────────────────────┬────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         │ from=0                │ from>0
         ▼                       ▼
┌─────────────────┐   ┌──────────────────────────────┐
│ check_cache     │   │ Default MultiCachedQueryResp  │
│ 读缓存+计算delta │   │ 不读缓存                      │
│ cache_query=true │   │ cache_query_response=false    │
└────────┬────────┘   └──────────────┬───────────────┘
         │                           │
         └───────────┬───────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│ delta 查询 (cache/mod.rs:227-268)                    │
│ req.query.from 保持原值（0 或 100）                    │
│ req.query.sql 保持原值（不含 OFFSET）                  │
│ → 传入 SearchService::search                         │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│ Sql::new (sql/mod.rs:134)                            │
│ sql.offset = query.from (= 100)                      │
│ sql.sql = statement.to_string()（不含 OFFSET）        │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│ DataFusion 优化规则 (optimizer/mod.rs:101,135)        │
│ AddSortAndLimitRule::new(limit, offset=100)          │
│ → 在逻辑计划中注入 Sort → Limit → Offset 节点         │
│ → SQL 文本始终不含 OFFSET                              │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│ 结果返回 (cluster/http.rs:87)                        │
│ Response::new(from=sql.offset=100, size=sql.limit)   │
│ → 前端收到 from=100 标识结果偏移                       │
└─────────────────────────────────────────────────────┘
```

---

## 五、核心代码位置索引

| 关注点 | 文件 | 行号 | 关键代码/说明 |
|--------|------|------|-------------|
| 前端入参分离 | `config/src/meta/search.rs` | 110-148 | `Query { sql, from, size }` 独立字段 |
| decode 不注入 OFFSET | `config/src/meta/search.rs` | 178-200 | 只做 base64 解码 |
| use_cache 门控 | `cache/mod.rs` | 92-98 | `from==0` 才启用缓存 |
| origin_sql 来源 | `cache/mod.rs` | 505 | `req.query.sql.clone()` 不含 OFFSET |
| Sql.offset = query.from | `sql/mod.rs` | 134 | `offset` 独立存储，不修改 SQL |
| Sql.sql = AST 序列化 | `sql/mod.rs` | 290 | `statement.to_string()` 不含 OFFSET |
| Visitor 不注入 OFFSET | `sql/mod.rs` | 156-281 | 5 个 Visitor 均不涉及 OFFSET |
| handle_histogram 不注入 OFFSET | `cache/cacher.rs` | 910-948 | 只替换 histogram 间隔 |
| hash_body 不含 from | `cache/mod.rs` | 575-591 | 遍历7个元素，无 from |
| from>0 不读缓存 | `cache/mod.rs` | 648-658 | `Default::default()` → cache_query_response=false |
| from>0 不写缓存 | `cache/mod.rs` | 438 | `c_resp.cache_query_response = false` |
| 排序对齐 | `cache/mod.rs` | 273-297, 830-899 | 两条路径用相同的 sort_response |
| AddSortAndLimitRule | `optimizer/mod.rs` | 101,135 | offset 注入 DataFusion 逻辑计划 |
| Response::new(from, size) | `cluster/http.rs` | 87 | from 作为元数据返回前端 |
