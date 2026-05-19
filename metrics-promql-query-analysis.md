# PromQL 指标查询架构分析

本文档分析 OpenObserve 中兼容 PromQL 语法的指标查询路径，涵盖查询解析与重写、底层列式存储索引利用和结果序列化返回三个核心阶段。

## 整体架构

```
HTTP 请求入口
    ↓
┌─────────────────────────────────────────────────────────┐
│  阶段一：查询解析与重写                                   │
│  - HTTP 路由与参数解析                                   │
│  - PromQL 语法解析 (promql_parser)                      │
│  - 查询重写 (remove_filter_all)                         │
│  - 权限校验                                             │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│  阶段二：查询执行与引擎                                   │
│  - 集群查询分发 (search::search)                        │
│  - 执行上下文构建 (PromqlContext)                       │
│  - 表达式引擎 (Engine)                                  │
│  - 递归表达式求值 (exec_expr)                            │
│  - 聚合/函数/二元运算处理                                │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│  阶段三：存储读取与索引利用                               │
│  - TableProvider 抽象                                   │
│  - 列式存储上下文创建 (storage::create_context)         │
│  - Tantivy 倒排索引过滤                                 │
│  - Parquet 文件列表过滤                                 │
│  - DataFusion 执行计划                                  │
│  - WAL 数据读取 (wal::create_context)                   │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│  阶段四：结果序列化与返回                                 │
│  - Value 类型系统                                       │
│  - 自定义 Serialize 实现                                │
│  - ApiFuncResponse 包装                                 │
│  - HTTP JSON 响应                                       │
└─────────────────────────────────────────────────────────┘
```

---

## 阶段一：查询入口与解析重写

### 1.1 HTTP 路由入口

**文件**: `src/handler/http/router/mod.rs:666-675`

PromQL 查询通过标准的 Prometheus HTTP API 端点暴露：

```rust
// 即时查询
.route("/{org_id}/prometheus/api/v1/query", get(promql::query_get).post(promql::query_post))
// 范围查询
.route("/{org_id}/prometheus/api/v1/query_range", get(promql::query_range_get).post(promql::query_range_post))
// 元数据查询
.route("/{org_id}/prometheus/api/v1/metadata", get(promql::metadata))
.route("/{org_id}/prometheus/api/v1/series", get(promql::series_get).post(promql::series_post))
.route("/{org_id}/prometheus/api/v1/labels", get(promql::labels_get).post(promql::labels_post))
.route("/{org_id}/prometheus/api/v1/label/{label_name}/values", get(promql::label_values))
```

### 1.2 请求处理流程

**文件**: `src/handler/http/request/promql/mod.rs`

核心处理函数 `query()` (第 160 行) 和 `query_range()` (第 432 行) 执行以下步骤：

1. **参数解析与验证**
   - 解析 `start`、`end`、`step`、`timeout` 等参数
   - 时间戳转换为微秒级精度
   - step 自动对齐与最小间隔保证 (MINIMAL_INTERVAL = 1s)

2. **权限校验 (企业版)**
   - 使用 `promql_parser::parser::parse()` 解析查询 AST
   - `MetricNameVisitor` 遍历 AST 提取所有指标名称
   - 对每个指标进行细粒度权限检查

3. **构建 MetricsQueryRequest**
   ```rust
   let req = promql::MetricsQueryRequest {
       query: req.query.unwrap_or_default(),
       start,
       end,
       step,
       query_exemplars: false,
       use_cache: None,
       search_type: None,
       regions: vec![],
       clusters: vec![],
   };
   ```

### 1.3 查询重写

**文件**: `src/service/promql/rewrite.rs`

`remove_filter_all()` 函数用于移除查询中的占位符过滤器：

```rust
pub fn remove_filter_all(vs: &mut VectorSelector) {
    let placeholder = get_config().common.dashboard_placeholder.to_string();
    vs.matchers.matchers.retain(|m| !match_placeholder(m, &placeholder));
    vs.matchers.or_matchers.iter_mut().for_each(|vs| {
        vs.retain(|m| !match_placeholder(m, &placeholder));
    });
}
```

**设计意图**：
- 支持 Dashboard 模板变量的"全部选择"场景
- 当用户选择"全部"时，后端移除该标签的过滤条件
- 兼容 `=`, `!=`, `=~`, `!~` 四种匹配操作符

---

## 阶段二：查询执行引擎

### 2.1 集群查询调度

**文件**: `src/service/promql/search/mod.rs`

`search()` 函数 (第 75 行) 是集群级查询入口：

1. **工作组准入控制**
   - OSS 版本：分布式锁 `check_work_group()`
   - 企业版：`WorkGroup::Short` 短查询工作组
   - 节点级槽位准入控制

2. **查询节点发现**
   ```rust
   let nodes = crate::service::search::cluster::flight::get_online_querier_nodes(
       trace_id, &req.org_id, "metrics/", Some(RoleGroup::Interactive)
   ).await?;
   ```

3. **结果缓存检查**
   - 检查 `cache::get()` 命中情况
   - 计算缓存命中率，部分命中时只查询缺失时间范围
   - 缓存键：`(query, start, end, step)` 哈希

4. **时间分片分发**
   ```rust
   let partition_step = max(micros(DEFAULT_LOOKBACK) * 2, step);  // 10分钟或step
   let worker_dt = if nr_steps > nr_queriers {
       partition_step * ((nr_steps + nr_queriers - 1) / nr_queriers)
   } else {
       partition_step
   };
   ```

5. **结果合并**
   - `merge_matrix_query()` / `merge_vector_query()` 按标签签名合并
   - 按 `metrics_max_series_response` 限制返回序列数

### 2.2 单节点查询执行

**文件**: `src/service/promql/search/grpc/mod.rs`

`search_inner()` 函数 (第 309 行) 执行单节点查询：

1. **PromQL 解析**
   ```rust
   let prom_expr = parser::parse(&query.query).map_err(DataFusionError::Execution)?;
   let eval_stmt = parser::EvalStmt {
       expr: prom_expr,
       start: UNIX_EPOCH.checked_add(Duration::from_micros(query.start as _)).unwrap(),
       end: UNIX_EPOCH.checked_add(Duration::from_micros(query.end as _)).unwrap(),
       interval: Duration::from_micros(query.step as _),
       lookback_delta: DEFAULT_LOOKBACK,
   };
   ```

2. **执行上下文构建**
   ```rust
   let mut ctx = PromqlContext::new(
       query_ctx,
       StorageProvider { trace_id, need_wal: req.need_wal },
       query.label_selector.clone(),
   );
   ```

3. **内存分组策略**
   - `get_max_file_list()`：找出记录数最多的指标流
   - `generate_search_group()`：按内存预算分组查询
     - 每点预估 24 字节 (timestamp:8 + value:8 + hash:8)
     - 按 `datafusion_max_size` 切分查询组

### 2.3 表达式引擎

**文件**: `src/service/promql/engine.rs`

`Engine` 结构体是 PromQL 表达式求值的核心：

```rust
pub struct Engine {
    trace_id: String,
    ctx: Arc<PromqlContext>,
    eval_ctx: EvalContext,
    label_selector: HashSet<String>,
    disable_label_selector: bool,
    result_type: Option<String>,
}
```

#### 2.3.1 列裁剪优化

`extract_columns_from_prom_expr()` (第 121 行) 静态分析表达式，提取需要加载的标签列：

```rust
fn extract_columns_from_modifier(&mut self, modifier: &Option<LabelModifier>, op: &token::TokenType) {
    if let Some(label_modifier) = modifier {
        match op.id() {
            token::T_TOPK | token::T_BOTTOMK => self.label_selector.clear(),
            _ => {
                if let (label_selector, LabelModifier::Include(labels)) =
                    (&mut self.label_selector, label_modifier)
                {
                    label_selector.extend(labels.labels.iter().cloned());
                }
            }
        }
    }
}
```

**特殊处理**：
- `label_replace` / `label_join` 函数：`disable_label_selector = true`，加载所有标签列
- `topk` / `bottomk`：清空列选择，需要所有标签
- `group_left` / `group_right`：清空列选择

#### 2.3.2 递归求值

`exec_expr()` (第 217 行) 采用递归下降方式求值表达式：

| 表达式类型 | 处理方式 |
|-----------|---------|
| `AggregateExpr` | `aggregate_exprs()` 调用聚合函数 |
| `BinaryExpr` | 递归求值左右操作数，执行二元运算 |
| `VectorSelector` | `eval_vector_selector()` 即时向量选择 |
| `MatrixSelector` | `eval_matrix_selector()` 范围向量选择 |
| `Call` | `call_expr()` 函数调用 |
| `NumberLiteral` | 直接返回 `Value::Float` |
| `ParenExpr` / `UnaryExpr` / `Subquery` | 递归处理内部表达式 |

#### 2.3.3 向量选择器求值

**即时向量选择** (`eval_vector_selector()`，第 372 行)：
1. 调用 `selector_load_data_owned()` 加载数据
2. 对每个评估时间戳，二分查找最近的样本
3. 应用 lookback 窗口 (`DEFAULT_LOOKBACK = 5分钟`)
4. 处理 `offset` 修饰符

**范围向量选择** (`eval_matrix_selector()`，第 470 行)：
1. 加载指定时间范围内的所有样本
2. 应用 `time_window` 标记范围
3. 并行排序优化 (`par_sort_unstable_by`)

---

## 阶段三：存储读取与索引利用

### 3.1 TableProvider 抽象

**文件**: `src/service/promql/mod.rs:51-62`

```rust
#[async_trait]
pub trait TableProvider: Sync + Send + 'static {
    async fn create_context(
        &self,
        org_id: &str,
        stream_name: &str,
        time_range: (i64, i64),
        machers: Matchers,
        label_selector: HashSet<String>,
        filters: &mut [(String, Vec<String>)],
    ) -> Result<Vec<(SessionContext, Arc<Schema>, ScanStats, bool)>>;
}
```

### 3.2 StorageProvider 实现

**文件**: `src/service/promql/search/grpc/mod.rs:48-98`

```rust
struct StorageProvider {
    trace_id: String,
    need_wal: bool,
}

#[async_trait]
impl TableProvider for StorageProvider {
    async fn create_context(...) -> Result<Vec<Context>> {
        let mut ctxs = Vec::new();
        // 1. 列式存储上下文
        if let Some(ctx) = storage::create_context(...).await? {
            ctxs.push(ctx);
        }
        // 2. WAL 数据上下文（热数据）
        if self.need_wal {
            let wal_ctx_list = wal::create_context(...).await?;
            ctxs.extend(wal_ctx_list);
        }
        Ok(ctxs)
    }
}
```

### 3.3 列式存储上下文创建

**文件**: `src/service/promql/search/grpc/storage.rs`

`create_context()` (第 56 行) 是存储层核心：

#### 3.3.1 文件列表过滤

1. **分区过滤**
   ```rust
   let files = get_file_list(
       trace_id, org_id, stream_name, partition_time_level, time_range, filters
   ).await?;
   ```
   - 按 `PartitionTimeLevel` 进行时间分区裁剪
   - 按分区键值进行分区裁剪

2. **文件大小统计**
   ```rust
   let scan_stats = file_list::calculate_files_size(&files.to_vec()).await?;
   ```

#### 3.3.2 缓存预热

```rust
let (cache_type, cache_hits, cache_misses) = cache_files(
    trace_id,
    &files.iter().map(|f| (f.id, &f.account, &f.key, f.meta.compressed_size, f.meta.max_ts)).collect_vec(),
    &mut scan_stats,
    "parquet",
).await;
```

- 内存缓存 + 磁盘缓存二级缓存
- 缓存未命中时后台异步下载

#### 3.3.3 倒排索引过滤

```rust
let (index_condition, is_full_convert) = convert_matchers_to_index_condition(&matchers, &schema, &index_fields)?;
if !index_condition.conditions.is_empty() && cfg.common.inverted_index_enabled {
    (idx_took, is_add_filter_back, ..) = tantivy_search(query.clone(), &mut files, Some(index_condition), None).await?;
}
```

**索引转换逻辑** (`convert_matchers_to_index_condition()`，第 302 行)：
- 仅对索引字段且非时间/值字段的 matcher 转换
- 支持 `Equal` / `NotEqual` / `Regex` 三种操作
- `is_full_convert` 标记所有 matcher 是否都可下推到索引

#### 3.3.4 DataFusion 表注册

```rust
let ctx = register_metrics_table(&session, schema.clone(), stream_name, files).await?;
```

返回值 `(ctx, schema, scan_stats, keep_filters)` 中 `keep_filters` 决定是否需要在 DataFusion 层保留过滤条件。

### 3.4 DataFusion 数据加载

**文件**: `src/service/promql/engine.rs:1282`

`selector_load_data_from_datafusion()` 执行实际的数据加载：

#### 3.4.1 查询窗口优化

```rust
let use_optimization = start != end
    && step > 0
    && step >= lookback * OPTIMIZATION_STEP_LOOKBACK_MULTIPLIER  // step >= 5 * lookback
    && (((end - start) / step) + 1) < OPTIMIZATION_MAX_STEPS;  // 步数 < 30

if use_optimization {
    // 只加载每个评估点周围的时间窗口
    let conditions: Vec<Expr> = eval_timestamps.iter().map(|&eval_ts| {
        col(TIMESTAMP_COL_NAME).between(lit(eval_ts - lookback), lit(eval_ts))
    }).collect();
    df.filter(disjunction(conditions).unwrap())
} else {
    // 加载完整时间范围 + lookback
    df.filter(col(TIMESTAMP_COL_NAME).between(lit(start - lookback), lit(end)))
}
```

**优化效果**：当 step 远大于 lookback 时，避免加载大量无效数据。

#### 3.4.2 Matcher 下推

```rust
df_group = apply_matchers(df_group, &schema, &selector.matchers)?;
```

将 PromQL matchers 转换为 DataFusion 表达式下推到扫描层。

#### 3.4.3 列选择

```rust
df_group = apply_label_selector(df_group, &schema, &label_selector)?;
```

只加载需要的标签列，减少 I/O 和内存占用。

#### 3.4.4 两阶段加载

**阶段一 - 加载样本数据**：
```rust
let (mut metrics, timestamp_set) = load_samples_from_datafusion(
    &query_ctx.trace_id, hash_field_type, df_group.clone()
).await?;
```
- 加载 `hash`、`timestamp`、`value` 列
- 按 `hash` 分组构建 `HashMap<u64, RangeValue>`

**阶段二 - 加载标签数据**：
```rust
let series = df_group
    .filter(col(TIMESTAMP_COL_NAME).in_list(timestamp_set.iter().map(|&v| lit(v)).collect(), false))?
    .select(label_cols)?
    .collect().await?;
```
- 利用阶段一得到的时间戳集合，进一步过滤数据
- 只加载标签列，关联到对应的 hash

---

## 阶段四：结果序列化与返回

### 4.1 Value 类型系统

**文件**: `src/config/src/meta/promql/value.rs`

```rust
pub enum Value {
    None,
    Float(f64),
    String(String),
    Sample(Sample),
    Instant(InstantValue),
    Vector(Vec<InstantValue>),
    Range(RangeValue),
    Matrix(Vec<RangeValue>),
}
```

### 4.2 自定义序列化实现

#### 4.2.1 Sample 序列化

```rust
impl Serialize for Sample {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&(self.timestamp / 1_000_000))?;  // 转换为秒
        seq.serialize_element(&self.value.to_string())?;          // 值转字符串
        seq.end()
    }
}
```

**Prometheus 兼容格式**：`[timestamp_seconds, "value_string"]`

#### 4.2.2 RangeValue 序列化

```rust
impl Serialize for RangeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.exemplars.is_none() {
            // 普通矩阵结果
            let mut seq = serializer.serialize_struct("range_value", 2)?;
            seq.serialize_field("metric", &labels_map)?;
            seq.serialize_field("values", &self.samples)?;
            seq.end()
        } else {
            // Exemplars 结果
            let mut seq = serializer.serialize_struct("range_value", 2)?;
            seq.serialize_field("seriesLabels", &labels_map)?;
            seq.serialize_field("exemplars", &self.exemplars.as_ref().unwrap())?;
            seq.end()
        }
    }
}
```

### 4.3 API 响应包装

**文件**: `src/handler/http/request/promql/mod.rs:1293-1352`

```rust
match promql::search::search(...).await {
    Ok(data) if !req.query_exemplars => (
        StatusCode::OK,
        axum::Json(config::meta::promql::ApiFuncResponse::ok(
            config::meta::promql::QueryResult {
                result_type: data.get_type().to_string(),
                result: data,
            },
            Some(trace_id.to_string()),
        )),
    ).into_response(),
    // ...
}
```

**QueryResult 结构**：
```json
{
    "status": "success",
    "data": {
        "resultType": "matrix",
        "result": [...]
    }
}
```

---

## 关键协作流程

### 完整调用链

```
HTTP GET /api/{org_id}/prometheus/api/v1/query_range
    ↓
handler::http::request::promql::query_range()
  ├─ 参数解析与验证
  ├─ 权限校验 (企业版)
  └─ promql::search::search()
      ├─ 缓存检查
      ├─ 集群节点发现
      ├─ 工作组准入
      ├─ 时间分片分发到多个 querier
      │   └─ [gRPC] promql::search::grpc::search()
      │       ├─ parser::parse() 解析 PromQL
      │       ├─ PromqlContext::new() 创建上下文
      │       ├─ StorageProvider::create_context()
      │       │   ├─ storage::create_context()
      │       │   │   ├─ get_file_list() 分区过滤
      │       │   │   ├─ cache_files() 缓存预热
      │       │   │   ├─ tantivy_search() 倒排索引过滤
      │       │   │   └─ register_metrics_table() 注册表
      │       │   └─ wal::create_context() 热数据
      │       ├─ Engine::exec()
      │       │   ├─ extract_columns_from_prom_expr() 列裁剪
      │       │   └─ exec_expr() 递归求值
      │       │       └─ eval_vector_selector() / eval_matrix_selector()
      │       │           └─ selector_load_data_from_datafusion()
      │       │               ├─ DataFusion 查询计划
      │       │               ├─ load_samples_from_datafusion()
      │       │               └─ load series labels
      │       └─ add_value() 转换为 gRPC 响应
      ├─ merge_matrix_query() 合并多节点结果
      ├─ cache::set() 回写缓存
      └─ 返回 QueryResult
```

### 关键设计要点

1. **分层抽象**：
   - HTTP 层与查询引擎解耦
   - TableProvider 抽象屏蔽存储细节
   - Engine 专注于表达式求值

2. **索引利用**：
   - 分区裁剪 → 文件列表 → Tantivy 倒排索引 → DataFusion 谓词下推
   - 三级过滤机制大幅减少扫描数据量

3. **性能优化**：
   - 列裁剪只加载必要标签
   - 两阶段数据加载减少数据移动
   - 查询窗口优化避免加载无效数据
   - Rayon 并行处理 CPU 密集型任务

4. **可观测性**：
   - 全链路 trace_id 传递
   - 详细的 scan_stats 统计
   - 各阶段耗时日志

5. **企业版增强**：
   - 细粒度指标级权限控制
   - 超集群跨区域查询
   - 工作组与节点槽位准入控制

---

## 附录：标签条件传递链路深度分析

本章节详细分析 PromQL 查询中标签匹配条件（Label Matchers）从查询入口到索引过滤、再到数据过滤的完整传递链路，包括条件在各阶段的形态变化、被消费的节点，以及这个衔接对查询结果和性能的影响。

### A.1 条件形态演变总览

以查询 `http_requests_total{job="api", status=~"5.."}` 为例，标签条件在各阶段的形态变化如下：

```
用户输入 PromQL 字符串
    ↓ 解析
promql_parser::label::Matchers {
    matchers: [
        Matcher { name: "__name__", op: Equal, value: "http_requests_total" },
        Matcher { name: "job", op: Equal, value: "api" },
        Matcher { name: "status", op: Re, value: "5.." },
    ]
}
    ↓ 索引适配 (convert_matchers_to_index_condition)
IndexCondition {
    conditions: [
        Condition::Equal("job", "api"),
        Condition::Regex("status", "5.."),
    ]
    // 注意: __name__ 不参与索引过滤，用于确定 stream_name
}
    ↓ Tantivy 查询构建
BooleanQuery {
    clauses: [
        (Must, TermQuery("job:api")),
        (Must, RegexQuery("status:5..")),
        (Must, RangeQuery("_timestamp:[start, end)")),
    ]
}
    ↓ 索引查询结果
TantivyResult::RowIdsBitVec(num_rows, BitVec)
    ↓ DataFusion 谓词下推 (apply_matchers)
DataFusion Expr:
    col("job").eq(lit("api"))
    AND regexp_match(col("status"), "^5..$")
    AND col("_timestamp").between(start, end)
```

### A.2 关键节点与协作细节

#### A.2.1 Matcher 到 IndexCondition 的转换

**文件**: `src/service/promql/search/grpc/storage.rs:302-330`

`convert_matchers_to_index_condition()` 是第一个关键转换点：

```rust
fn convert_matchers_to_index_condition(
    matchers: &Matchers,
    schema: &Arc<Schema>,
    index_fields: &HashSet<String>,
) -> Result<(IndexCondition, bool)> {
    let mut index_condition = IndexCondition::default();
    let mut is_full_convert = true;
    for mat in matchers.matchers.iter() {
        // 过滤条件: 非时间/值字段 + 是索引字段 + schema 中存在
        if mat.name == TIMESTAMP_COL_NAME
            || mat.name == VALUE_LABEL
            || !index_fields.contains(&mat.name)
            || schema.field_with_name(&mat.name).is_err()
        {
            is_full_convert = false;
            continue;
        }
        // 支持的操作类型: Equal / NotEqual / Regex
        let condition = match &mat.op {
            MatchOp::Equal => Condition::Equal(mat.name.clone(), mat.value.clone()),
            MatchOp::NotEqual => Condition::NotEqual(mat.name.clone(), mat.value.clone()),
            MatchOp::Re(regex) => Condition::Regex(mat.name.clone(), regex.to_string()),
            _ => {
                is_full_convert = false;
                continue;
            }
        };
        index_condition.add_condition(condition);
    }
    Ok((index_condition, is_full_convert))
}
```

**关键输出**:
- `IndexCondition`: 可下推到索引的条件集合
- `is_full_convert`: 标记是否所有 matcher 都成功转换

**对结果的影响**:
- 如果某个 matcher 无法转换（如字段未建索引），`is_full_convert = false`，该条件会被保留到 DataFusion 层二次过滤
- 这确保了查询结果的正确性，但可能影响性能（索引过滤不彻底）

#### A.2.2 IndexCondition 到 Tantivy Query 的转换

**文件**: `src/service/search/index.rs:135-163`

`IndexCondition::to_tantivy_query()` 构建 Tantivy 可执行查询：

```rust
pub fn to_tantivy_query(
    &self,
    trace_id: &str,
    schema: Schema,
    default_field: Option<Field>,
) -> anyhow::Result<(Box<dyn Query>, bool)> {
    let mut has_skipped = false;
    let mut queries: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(self.conditions.len());
    for condition in &self.conditions {
        match condition.to_tantivy_query(&schema, default_field) {
            Ok(query) => {
                queries.push((Occur::Must, query));
            }
            Err(e) => {
                log::info!("... skipping condition due to error: {e}");
                has_skipped = true;  // 标记有条件被跳过
            }
        }
    }
    Ok((Box::new(BooleanQuery::from(queries)), has_skipped))
}
```

**关键输出**:
- `Box<dyn Query>`: Tantivy 查询对象
- `has_skipped`: 是否有条件在索引层被跳过

**触发 has_skipped = true 的场景**:
1. 新添加的索引字段在旧数据中不存在（schema 演化问题）
2. 索引文件损坏或格式不兼容
3. Regex 表达式语法错误

#### A.2.3 Tantivy 索引查询执行

**文件**: `src/service/search/grpc/storage.rs:724-900`

`search_tantivy_index()` 执行实际的索引查询：

```rust
async fn search_tantivy_index(
    trace_id: &str,
    time_range: (i64, i64),
    index_condition: Option<IndexCondition>,
    idx_optimize_rule: Option<IndexOptimizeMode>,
    parquet_file: &FileKey,
) -> anyhow::Result<(String, TantivyResult, bool)> {
    // ... 缓存检查、索引打开 ...

    // 1. 构建 Tantivy 查询
    let condition: IndexCondition = index_condition.ok_or(...)?;
    let (mut query, has_skipped_conditions) =
        condition.to_tantivy_query(trace_id, tantivy_schema.clone(), fts_field)?;

    // 2. 添加时间范围过滤（如果文件不完全在查询时间范围内）
    if !file_in_range && let Ok(ts_field) = tantivy_schema.get_field(TIMESTAMP_COL_NAME) {
        query = Box::new(BooleanQuery::new(vec![
            (Occur::Must, query),
            (Occur::Must, Box::new(ts_range)),
        ]));
    }

    // 3. 执行查询，返回匹配的行号位图
    let res = tokio::task::spawn_blocking(move || {
        TantivyResult::handle_matched_docs(&searcher, query)
    }).await??;

    Ok((parquet_file.key.to_string(), res, has_skipped_conditions))
}
```

**TantivyResult 类型**:
| 结果类型 | 说明 |
|---------|------|
| `RowIdsBitVec(num_rows, bitvec)` | 匹配行的位图，用于后续 Parquet 行组过滤 |
| `Count(count)` | 仅统计匹配行数 |
| `Histogram(histogram)` | 下推的直方图统计 |
| `TopN(top_n)` | 下推的 TopN 结果 |
| `Distinct(distinct)` | 下推的去重结果 |

**性能影响**:
- 索引过滤返回 `RowIdsBitVec` 时，Parquet 扫描时可以跳过不匹配的行组
- 当 `num_rows == 0` 时，整个 Parquet 文件被跳过，无需扫描
- 当匹配行数比例超过 `inverted_index_skip_threshold` 配置项（默认 35%），索引查询会被短路，直接回退到 DataFusion 过滤

#### A.2.4 is_add_filter_back 标志传递

**文件**: `src/service/search/grpc/storage.rs:420-703`

`tantivy_search()` 聚合所有文件的索引查询结果，管理 `is_add_filter_back` 标志：

```rust
pub async fn tantivy_search(
    query: Arc<super::QueryParams>,
    file_list: &mut Vec<FileKey>,
    index_condition: Option<IndexCondition>,
    idx_optimize_mode: Option<IndexOptimizeMode>,
) -> Result<(usize, bool, TantivyMultiResult), Error> {
    // 初始值: 如果有文件没有索引，需要回溯过滤
    let mut is_add_filter_back = file_list_map.len() != index_file_names.len();

    // 对每个文件执行索引查询
    while let Some(result) = tasks.try_next().await {
        match result {
            Ok((file_name, result, has_skipped_conditions)) => {
                // 任何文件有条件被跳过，都需要回溯过滤
                if has_skipped_conditions {
                    is_add_filter_back = true;
                }
                // 其他需要回溯的场景:
                // - 文件没有索引 (file_name.is_empty())
                // - 索引查询错误
                // - 匹配行数过多，索引查询被短路
            }
        }
    }

    Ok((took, is_add_filter_back, tantivy_result))
}
```

#### A.2.5 回溯过滤：DataFusion 层 apply_matchers

**文件**: `src/service/promql/utils.rs:127-155`

当 `is_add_filter_back = true` 或 `is_full_convert = false` 时，`apply_matchers()` 在 DataFusion 层添加过滤条件：

```rust
pub fn apply_matchers(df: DataFrame, schema: &Schema, matchers: &Matchers) -> Result<DataFrame> {
    let mut df = df;
    for mat in matchers.matchers.iter() {
        // 跳过时间和值字段，以及 schema 中不存在的字段
        if mat.name == TIMESTAMP_COL_NAME
            || mat.name == VALUE_LABEL
            || schema.field_with_name(&mat.name).is_err()
        {
            continue;
        }
        // 转换为 DataFusion 表达式
        match &mat.op {
            MatchOp::Equal => df = df.filter(col(mat.name.clone()).eq(lit(mat.value.clone())))?,
            MatchOp::NotEqual => df = df.filter(col(mat.name.clone()).not_eq(lit(mat.value.clone())))?,
            MatchOp::Re(regex) => {
                let regex = format!("^{}$", regex.as_str());
                df = df.filter(REGEX_MATCH_UDF.call(vec![col(mat.name.clone()), lit(regex)]))?
            }
            MatchOp::NotRe(regex) => {
                let regex = format!("^{}$", regex.as_str());
                df = df.filter(REGEX_NOT_MATCH_UDF.call(vec![col(mat.name.clone()), lit(regex)]))?
            }
        }
    }
    Ok(df)
}
```

**执行位置**: `src/service/promql/engine.rs:1350-1360` 的 `selector_load_data_from_datafusion()` 中调用。

### A.3 条件消费决策树

```
Matchers (原始条件集合)
    │
    ├─ __name__ → 确定 stream_name（不参与过滤）
    │
    ├─ 其他 Matchers
    │    │
    │    ├─ 字段是索引字段？
    │    │    ├─ 是 → 加入 IndexCondition
    │    │    └─ 否 → is_full_convert = false，留到 DataFusion
    │    │
    │    └─ 操作符支持？(Equal/NotEqual/Regex)
    │         ├─ 是 → 加入 IndexCondition
    │         └─ 否 → is_full_convert = false，留到 DataFusion
    │
    └─ IndexCondition
         │
         ├─ 所有 Condition 都能转换为 Tantivy Query？
         │    ├─ 是 → has_skipped = false
         │    └─ 否 → has_skipped = true
         │
         ├─ Tantivy 索引查询
         │    ├─ 匹配行数 = 0 → 跳过文件，无需后续处理
         │    ├─ 匹配比例 < inverted_index_skip_threshold → 返回 RowIdsBitVec，用于 Parquet 行过滤
         │    └─ 匹配比例 > inverted_index_skip_threshold → 短路，is_add_filter_back = true，回退到 DataFusion
         │
         └─ is_add_filter_back 标志
              ├─ true → apply_matchers() 在 DataFusion 层重新应用所有过滤条件
              └─ false → 索引过滤已精确，无需二次过滤
```

### A.4 对查询结果和性能的影响

#### A.4.1 正确性保障

**多级过滤的设计确保结果正确性**:
1. 索引层快速过滤掉大部分不匹配的文件和行
2. DataFusion 层作为"安全网"，重新应用所有过滤条件
3. 即使索引查询出错或被短路，DataFusion 过滤仍能保证结果正确

**潜在风险点**:
- 索引和数据之间的一致性问题（罕见，通常在写入失败时发生）
- Regex 表达式在 Tantivy 和 DataFusion 中的语义差异

#### A.4.2 性能权衡

| 场景 | is_add_filter_back | 性能特征 |
|------|-------------------|---------|
| 所有条件都可下推索引，且索引过滤精确 | false | **最佳性能**：索引过滤后只需扫描少量数据，无需二次过滤 |
| 部分条件可下推索引 | true | **中等性能**：索引减少扫描范围，但 DataFusion 仍需过滤 |
| 无索引或索引过滤被短路 | true | **最差性能**：全量扫描 DataFusion 过滤 |
| 索引查询返回空结果 | - | **最优**：直接跳过文件，零扫描成本 |

**性能优化关键点**:
1. **索引覆盖率**: 为常用过滤标签建立索引，提高 `is_full_convert` 比率
2. **索引选择性**: 高基数字段（如 `request_id`）的索引过滤效果最好
3. **避免全扫描**: 当查询没有任何标签过滤时，会回退到全表扫描
4. **索引结果缓存**: Tantivy 结果缓存（`tantivy_result_cache_enabled`）可以复用重复查询的索引过滤结果

#### A.4.3 内存开销

- **Tantivy 位图**: 每个文件的匹配行位图占用约 `num_rows / 8` 字节
- **短路阈值**: 由配置项 `inverted_index_skip_threshold` 控制（默认 35%），当匹配行数比例超过该阈值时，继续使用索引反而浪费资源，此时回退到全扫描更高效
- **配置位置**: `src/config/src/config.rs:1814`，环境变量 `ZO_INVERTED_INDEX_SKIP_THRESHOLD`

### A.5 特殊场景处理

#### A.5.1 历史数据的索引兼容

当新添加索引字段时，历史数据的索引文件中不存在该字段：
1. `to_tantivy_query()` 会返回 `Err`，标记 `has_skipped = true`
2. `is_add_filter_back = true`，DataFusion 层会应用该条件
3. 结果正确，但性能回退到无索引状态

#### A.5.2 WAL 热数据过滤

**文件**: `src/service/promql/search/grpc/wal.rs`

WAL 中的热数据尚未写入 Parquet，也没有建立索引：
- 所有过滤条件都在 DataFusion 层通过 `apply_matchers()` 应用
- WAL 数据量通常较小，全扫描开销可控
- 合并结果时，WAL 数据和 Parquet 数据的过滤条件是一致的

#### A.5.3 正则表达式性能

Regex 匹配在索引层和 DataFusion 层的性能差异：
- **Tantivy 层**: 基于倒排索引的快速匹配，适合前缀匹配和简单正则
- **DataFusion 层**: 基于 Rust regex crate 的完整正则支持，性能较低
- 优化建议：尽量使用 `=` 或 `IN` 操作符替代复杂正则

### A.6 可观测性指标

以下是代码中实际定义的与索引查询和缓存相关的 Prometheus 指标：

#### A.6.1 Tantivy 索引结果缓存指标

**文件**: `src/config/src/metrics.rs:1250-1292`

| 指标名称 | 类型 | 说明 |
|----------|------|------|
| `tantivy_result_cache_memory_usage` | Gauge | 索引结果缓存内存使用量（字节） |
| `tantivy_result_cache_gc_total` | Counter | 索引结果缓存 GC 总次数 |
| `tantivy_result_cache_requests_total` | Counter | 索引结果缓存请求总次数 |
| `tantivy_result_cache_hits_total` | Counter | 索引结果缓存命中次数 |

#### A.6.2 查询磁盘缓存指标

**文件**: `src/config/src/metrics.rs:363-1115`

| 指标名称 | 类型 | 说明 |
|----------|------|------|
| `query_disk_cache_limit_bytes` | Gauge | 磁盘缓存限制大小（字节） |
| `query_disk_cache_used_bytes` | Gauge | 磁盘缓存已使用大小（字节） |
| `query_disk_cache_files` | Gauge | 磁盘缓存中的文件数量 |
| `query_disk_cache_hit_count` | Counter | 磁盘缓存命中次数 |
| `query_disk_cache_miss_count` | Counter | 磁盘缓存未命中次数 |

#### A.6.3 Parquet 缓存比率指标

**文件**: `src/config/src/metrics.rs:427-441`

| 指标名称 | 类型 | 说明 |
|----------|------|------|
| `query_parquet_cache_ratio` | Histogram | Parquet 文件缓存比率分布 |
| `query_parquet_cache_ratio_node` | Histogram | 节点级 Parquet 缓存比率分布 |

#### A.6.4 日志可观测性

除了 metrics 指标外，关键信息也通过日志输出：

- **索引短路日志**: `search->tantivy: file: ..., result percent ...% is too large, back to datafusion`
- **索引结果日志**: `search->tantivy: total hits for index_condition: ... found ..., is_add_filter_back: ..., file_num: ..., took: ... ms`
- **索引加载日志**: `search->tantivy: stream ..., load tantivy index files ..., index size: ..., memory cached ..., disk cached ...`
- **条件跳过日志**: `to_tantivy_query: skipping condition due to error: ...`
