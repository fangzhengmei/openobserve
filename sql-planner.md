# OpenObserve SQL 查询规划器代码分析

## 一、整体架构

OpenObserve 的 SQL 查询规划器基于 Apache DataFusion 构建，采用分层架构设计：

```
用户 SQL → SQL 解析层 → 逻辑优化层 → 物理优化层 → 分布式执行层
```

### 核心模块位置

| 模块 | 文件路径 | 主要职责 |
|------|---------|---------|
| SQL 解析 | `src/service/search/sql/mod.rs` | SQL 语法解析、AST 重写、元信息提取 |
| 逻辑优化 | `src/service/search/datafusion/optimizer/logical_optimizer/` | 逻辑计划改写、直方图重写、排序限制 |
| 物理优化 | `src/service/search/datafusion/optimizer/physical_optimizer/` | 索引优化、远程扫描、下推策略 |
| 分布式计划 | `src/service/search/datafusion/distributed_plan/` | 分片调度、远程执行、结果合并 |
| 自定义函数 | `src/service/search/datafusion/udf/` | 内置 UDF、用户自定义 VRL 函数 |
| 执行上下文 | `src/service/search/datafusion/exec.rs` | Session 构建、UDF 注册、表提供器 |

### 关键数据结构：`Sql`

在 `src/service/search/sql/mod.rs:68-88` 定义了 SQL 解析后的核心元信息结构：

```rust
pub struct Sql {
    pub sql: String,                    // 重写后的 SQL
    pub is_complex: bool,               // 是否为复杂查询
    pub org_id: String,
    pub stream_type: StreamType,
    pub stream_names: Vec<TableReference>,
    pub has_match_all: bool,            // 是否包含全文检索
    pub equal_items: HashMap<...>,      // 分区键等值条件（非时间范围！）
    pub columns: HashMap<...>,          // 涉及的列
    pub time_range: Option<(i64, i64)>, // 时间范围（来自请求参数）
    pub histogram_interval: Option<i64>,// 直方图间隔
    pub sorted_by_time: bool,           // 是否仅按时间排序
    // ... 其他字段
}
```

---

## 二、自定义函数处理机制

### 2.1 函数分类

OpenObserve 支持三类自定义函数：

#### A. 内置 UDF（约 20+ 个）

在 `src/service/search/datafusion/udf/mod.rs:59-104` 中定义了默认函数列表：

| 函数名 | 用途 |
|--------|------|
| `match_all` / `fuzzy_match_all` | 全文检索 |
| `str_match` / `match_field` | 字段级字符串匹配 |
| `re_match` / `re_not_match` / `re_matches` | 正则匹配 |
| `histogram` | 时间直方图聚合 |
| `time_range` | 时间范围过滤 |
| `date_format` | 日期格式化 |
| `cast_to_timestamp` | 时间类型转换 |
| `arr_*` 系列 | 数组操作（count/index/join/sort/zip） |
| `transform` | VRL 脚本转换 |
| `cipher_*` | 加解密（企业版） |

**注册流程**（`src/service/search/datafusion/exec.rs:265-312`）：

```rust
pub fn register_udf(ctx: &SessionContext, org_id: &str) -> Result<()> {
    ctx.register_udf(STR_MATCH_UDF.clone());
    ctx.register_udf(FUZZY_MATCH_UDF.clone());
    ctx.register_udf(REGEX_MATCH_UDF.clone());
    // ... 约 20 个内置 UDF 注册
    let udf_list = get_all_transform(org_id)?; // 用户自定义函数
    for udf in udf_list {
        ctx.register_udf(udf.clone());
    }
    Ok(())
}
```

#### B. 用户自定义 VRL 函数

在 `src/service/search/datafusion/udf/transform_udf.rs` 中实现了 VRL（Vector Remap Language）脚本作为 UDF 的机制：

**核心实现**：
```rust
fn get_udf_vrl(fn_name: String, func: &str, params: &str, num_args: u8, org_id: &str)
    -> Result<ScalarUDF> {
    let vrl_calc = Arc::new(move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        for i in 0..len {
            // 1. 构造 VRL 输入对象
            let mut obj_str = String::from("");
            for (j, arg) in args.iter().enumerate() {
                obj_str.push_str(&format!(
                    " .{} = \"{}\" \n", in_params[j], col.value(i)
                ));
            }
            obj_str.push_str(&format!(" \n {}", &local_func));
            
            // 2. 编译并执行 VRL 程序
            match compile_vrl_function(&obj_str, &local_org_id) {
                Ok(res) => {
                    let result = apply_vrl_fn(&mut runtime, res.program);
                    res_data_vec.insert(i, json::get_string_value(&result));
                }
                Err(e) => { /* 错误处理 */ }
            }
        }
        Ok(ColumnarValue::from(Arc::new(result) as ArrayRef))
    });
    Ok(create_user_df(fn_name.as_str(), num_args, vrl_calc))
}
```

**特点**：
1. 逐行执行 VRL 脚本，性能开销较大
2. 支持动态加载（通过 `QUERY_FUNCTIONS` 全局配置）
3. 函数签名在注册时固定，运行时按组织隔离

#### C. 自定义聚合函数（UDAF）

- `summary_percentile`：百分位汇总统计
- `approx_topk` / `approx_topk_distinct`：近似 TopK（企业版）

---

## 三、分区键等值条件与时间范围的区别

### ⚠️ 关键纠正：`equal_items` 不是时间范围提取

之前的理解有误，`equal_items` 和 `time_range` 是**两个完全独立的提取路径**：

### 3.1 `equal_items` — 分区键等值条件提取

**提取位置**：`src/service/search/sql/visitor/partition_column.rs`

**Visitor 模式**：`PartitionColumnVisitor`

```rust
pub struct PartitionColumnVisitor<'a> {
    // table_name -> Vec<(field_name, value)>
    pub equal_items: HashMap<TableReference, Vec<(String, String)>>,
    schemas: &'a HashMap<TableReference, Arc<SchemaCache>>,
}
```

**提取逻辑**（`pre_visit_query`，第 46-151 行）：

1. 仅遍历最外层 Query 的 WHERE 子句
2. 通过 `split_conjunction(expr)` 将 AND 条件拆分
3. 匹配两种表达式：
   - **等值比较**：`field = value` 或 `value = field`
   - **IN 列表**（非否定）：`field IN (value1, value2, ...)`
4. 字段归属判断：
   - `Expr::Identifier`：字段名必须唯一存在于某个表 schema 中（`count == 1`）
   - `Expr::CompoundIdentifier`：`table.field` 形式，表名必须在 schemas 中
5. 提取后的值存入 `equal_items`，用于**文件列表的二次过滤**（分区剪枝）

**不支持**：
- NOT IN（`negated: true` 时跳过，第 103 行）
- 多表歧义字段（`count > 1` 时跳过，第 76 行）
- 别名表名（`table_name not in schemas` 时跳过，第 90 行）

### 3.2 `time_range` — 时间范围来源

**提取位置**：`src/service/search/sql/mod.rs:302`

```rust
time_range: Some((query.start_time, query.end_time)),
```

**关键点**：
- `time_range` **不来自 SQL 解析**，直接来自请求参数 `SearchQuery.start_time` 和 `SearchQuery.end_time`
- 即使 SQL 中写了 `_timestamp > xxx`，也不会修改 `time_range`
- `time_range` 用于**初次文件列表查询**（`file_list::query_ids` 调用）

### 3.3 两者关系与协作

| 维度 | `equal_items`（分区键等值条件） | `time_range`（时间范围） |
|------|------------------------------|------------------------|
| 来源 | SQL WHERE 子句解析 | 请求参数 |
| 字段 | 所有分区键（含自定义分区键） | 仅 `_timestamp` |
| 运算符 | `=`、`IN`（非否定） | `start_time`、`end_time` |
| 用途 | 文件列表二次过滤（`filter_source_by_partition_key`） | 文件列表初次查询 |
| 生效时机 | 拿到文件列表后，在内存中过滤 | 查询文件列表时，作为 S3 前缀条件 |

**典型执行流**：
```
1. 用 time_range = (start, end) 从 S3 查询文件列表 → 得到文件列表 A
2. 用 equal_items 对文件列表 A 做二次过滤 → 得到文件列表 B
3. 用文件列表 B 调度查询
```

---

## 四、时间字段处理机制

时间字段 `_timestamp` 是 OpenObserve 的核心分区字段，在查询规划中享有特殊待遇。

### 4.1 自动注入机制

**访问器模式**（`src/service/search/sql/rewriter/add_timestamp.rs`）：

```rust
impl VisitorMut for AddTimestampVisitor {
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            // 检查 SELECT 列表中是否已包含 _timestamp
            let mut has_timestamp = false;
            for item in select.projection.iter_mut() {
                // 遍历表达式检查是否引用 _timestamp
                // ...
            }
            // 如果不存在，自动添加到 SELECT 列表头部
            if !has_timestamp {
                select.projection.insert(
                    0,
                    SelectItem::UnnamedExpr(Expr::Identifier(
                        Ident::new(TIMESTAMP_COL_NAME.to_string())
                    )),
                );
            }
        }
        ControlFlow::Continue(())
    }
}
```

**注入时机**（`src/service/search/sql/mod.rs:274-281`）：
```rust
if !is_complex_query(&mut statement) {
    let mut add_timestamp_visitor = AddTimestampVisitor::new();
    let _ = statement.visit(&mut add_timestamp_visitor);
    // ... 还可能添加 _o2_id
}
```

**限制条件**：仅对**非复杂查询**自动注入。复杂查询包括：子查询、JOIN、GROUP BY、聚合函数、DISTINCT、UNION、SELECT *。

### 4.2 无 `_timestamp` 列触发单分片分支

**关键代码**：`src/service/search/mod.rs:687-704`

```rust
// if there is no _timestamp field or EXPLAIN in the query, return single partitions
let is_explain_query = is_explain_query(&req.sql);
let is_aggregate = is_aggregate_query(&req.sql).unwrap_or(false);
let ts_column = get_ts_col_order_by(&sql, TIMESTAMP_COL_NAME, is_aggregate)
    .map(|(v, _)| v);

let mut skip_get_file_list = ts_column.is_none() || apply_over_hits;
```

**触发条件**：
1. `ts_column.is_none()`：查询 ORDER BY 中不包含 `_timestamp` 列
2. `apply_over_hits`：需要在结果集上二次计算（如函数计算）
3. `is_explain_query`：EXPLAIN 查询（见下文）

**分支行为**：
- `skip_get_file_list = true`：不查询真实文件列表
- 改用统计信息估算（`stats.doc_num`、`stats.storage_size`）生成单个虚拟 FileId
- 后续 `PartitionGenerator` 看到只有一个文件，返回单分片 `[[start_time, end_time]]`

### 4.3 EXPLAIN 查询触发单分片分支

**关键代码**：`src/service/search/mod.rs:729-731`

```rust
// if http distinct, we should skip file list
if is_http_distinct || is_explain_query {
    skip_get_file_list = true;
}
```

**`is_explain_query` 判断**：匹配 SQL 开头为 `EXPLAIN` 或 `EXPLAIN ANALYZE`

**EXPLAIN 特殊处理**：
1. 不查询文件列表，用统计信息估算
2. 生成单分片，快速返回执行计划
3. `DistributeAnalyzeExec`（`src/service/search/datafusion/distributed_plan/distribute_analyze_exec.rs`）专门处理分布式 EXPLAIN ANALYZE，输出格式为 `phase, node_address, node_name, plan` 四列

### 4.4 排序优化

当查询仅按 `_timestamp` 降序排序时（`sorted_by_time` 标记），启用特殊优化：
- `split_file_groups_by_statistics = true`：按文件统计信息分割文件组
- `with_file_sort_order`：指定 Parquet 文件按 `_timestamp` 降序排序
- 避免全局排序，利用文件的已有排序性

### 4.5 直方图时间处理

`HistogramIntervalVisitor`（`src/service/search/sql/visitor/histogram_interval.rs`）负责：
1. 解析 `histogram(_timestamp, '1 hour')` 语法
2. 验证并调整时间间隔（避免过小的间隔导致性能问题）
3. 在逻辑优化阶段由 `RewriteHistogram` 重写为具体的时间分桶表达式

---

## 五、`match_all` 解析限制

**核心解析器**：`MatchVisitor`（`src/service/search/sql/visitor/match_all.rs`）

### 5.1 支持的场景 ✅

`match_all` 可以正常工作的场景：

1. **直接作用于流表**：
   ```sql
   SELECT * FROM logs WHERE match_all('error')
   ```

2. **子查询内部使用**（`pre_visit_query` 递归访问内层）：
   ```sql
   SELECT * FROM (SELECT * FROM logs WHERE match_all('error')) t
   ```

3. **IN 子查询内部使用**：
   ```sql
   SELECT * FROM logs WHERE id IN (
       SELECT id FROM trace WHERE match_all('slow')
   ) AND match_all('critical')
   ```

4. **CTE 内部使用**：
   ```sql
   WITH cte AS (SELECT id FROM logs WHERE match_all('error'))
   SELECT * FROM cte
   ```

### 5.2 不支持的场景 ❌

**`is_support_match_all = false`**，直接返回 SQL 错误：

```rust
if match_visitor.has_match_all && !match_visitor.is_support_match_all {
    return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
        "match_all() should directly apply to stream, FROM clause should not be join/subuqery/cte".to_string(),
    )));
}
```

具体限制场景：

| 场景 | 代码行 | 判断逻辑 | 示例 |
|------|--------|---------|------|
| **JOIN 查询** | 106-109 | `select.from.iter().any(\|from\| !from.joins.is_empty())` | `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE match_all('error')` |
| **多表 FROM** | 100-103 | `select.from.len() > 1` | `SELECT * FROM t1, t2 WHERE t1.id = t2.id AND match_all('error')` |
| **外层子查询** | 112-120 | `TableFactor::Derived` 且外层 WHERE 有 match_all | `SELECT * FROM (SELECT id FROM t1) t WHERE match_all('error')` |
| **外层 CTE** | 124-127 | `query.with.is_some()` 且外层 WHERE 有 match_all | `WITH cte AS (SELECT id FROM t1) SELECT * FROM cte WHERE match_all('error')` |

### 5.3 流没有 FTS 字段的限制 ❌

**`match_all_wrong_streams = true`**：

```rust
if has_match_all
    && let TableFactor::Table { name, .. } = &select.from[0].relation
    && let Ok(table) = object_name_to_table_reference(name.clone(), true)
    && let Some(has_fst_fields) = self.has_fst_fields.get(&table)
    && !*has_fst_fields
{
    self.match_all_wrong_streams = true;
}
```

返回错误：
```
match_all() should only apply to the stream that have full text search fields
```

---

## 六、分片查询策略

### 6.1 分片生成逻辑

`PartitionGenerator`（`src/service/search/partition.rs`）是分片策略的核心：

```rust
pub struct PartitionGenerator {
    min_step: i64,                    // 最小步长（微秒）
    mini_partition_duration_secs: u64, // 迷你分片时长（秒）
    is_histogram: bool,               // 是否为直方图查询
}

pub fn generate_partitions(
    &self,
    start_time: i64,
    end_time: i64,
    step: i64,
    order_by: OrderBy,
    is_aggregate: bool,
    add_mini_partition: bool,
) -> Vec<[i64; 2]> {
    if self.is_histogram {
        // 直方图对齐分片
        self.generate_partitions_aligned_with_histogram_interval(...)
    } else if is_aggregate {
        // 聚合查询不分片，单个分片
        vec![[start_time, end_time]]
    } else {
        // 带迷你分片的分片策略
        self.generate_partitions_with_mini_partition(...)
    }
}
```

### 6.2 单分片返回的完整触发条件汇总

| 触发条件 | 代码位置 | 说明 |
|---------|---------|------|
| 聚合查询 | `partition.rs` | `is_aggregate = true` 时返回单分片 |
| 无 `_timestamp` 列 | `mod.rs:688-704` | `ts_column.is_none()` → skip_get_file_list → 单虚拟文件 → 单分片 |
| EXPLAIN 查询 | `mod.rs:729-731` | `is_explain_query` → skip_get_file_list → 单分片 |
| HTTP DISTINCT | `mod.rs:729-731` | `is_http_distinct` → skip_get_file_list → 单分片 |
| `apply_over_hits` | `mod.rs:704` | 需要在结果集二次计算 → skip_get_file_list → 单分片 |

### 6.3 分片策略分类

| 查询类型 | 分片策略 | 设计意图 |
|---------|---------|---------|
| **简单查询** | 时间范围分片 + 迷你分片 | 快速返回首批结果，提升用户体验 |
| **聚合查询** | 不分片（单分片） | 避免多次聚合的开销 |
| **直方图查询** | 按直方图间隔对齐分片 | 保证每个分片可独立计算直方图桶 |

#### 迷你分片机制

`generate_partitions_with_mini_partition` 函数实现了渐进式分片：
1. 先创建一个小的时间分片（如最近几分钟）快速返回结果
2. 剩余时间按正常步长分片
3. 按排序顺序（DESC/ASC）排列分片优先级

### 6.4 文件到节点的分配

在 `src/service/search/datafusion/distributed_plan/node.rs` 中实现了文件分片到集群节点的分配：
- 基于一致性哈希或轮询策略
- 考虑节点的角色（querier / ingester）
- 支持超级集群跨区域调度

---

## 七、索引下推机制

### 7.1 下推生效前置条件

**全局开关**：`config.common.inverted_index_enabled`（`index.rs:92`）

**索引下推生效的完整条件链**：

```
配置开关开启
    ↓
1. 字段必须在 index_fields 集合中（建表时指定的索引字段）
    ↓
2. 表达式类型必须匹配（见下表）
    ↓
3. 所有非 _timestamp 过滤条件都能下推（is_only_timestamp_filter）
    ↓
4. 以下任一条件满足：
   a. can_remove_filter = true（功能开关 + 所有条件可下推）
   b. 无过滤条件但 optimizer_enabled = true（如 SELECT count(*)）
    ↓
5. 计划结构匹配 SimpleCount / SimpleSelect 等模式（Leader/Follower 优化器）
```

### 7.2 可下推的表达式类型

**`is_expr_valid_for_index`**（`index.rs:258-314`）：

| 表达式类型 | 支持情况 | 额外条件 |
|-----------|---------|---------|
| `column = value` / `value = column` | ✅ | column 必须在 index_fields 中 |
| `column != value` | ✅ | 同上 |
| `column IN (v1, v2, ...)` | ✅ | column 在 index_fields，所有值为常量 |
| `column NOT IN (...)` | ❌ | negated = true 不支持 |
| `match_all('text')` | ✅ | 参数必须是字符串，且分词后非空（如 'c' 只有单字符不支持） |
| `fuzzy_match_all('text', n)` | ✅ | 必须有 2 个参数 |
| `str_match(col, 'text')` | ✅ | col 必须在 index_fields 中 |
| `match_field(col, 'text')` | ✅ | 同上 |
| `expr1 AND expr2` | ✅ | 两边都可下推 |
| `expr1 OR expr2` | ✅ | 两边都可下推 |
| `NOT expr` | ✅ | 内部表达式可下推 |
| 范围比较 (>, <, >=, <=) | ❌ | 不在支持列表中 |
| 其他函数调用 | ❌ | 仅支持白名单内的函数 |

**`match_all` 空 token 检查**（第 293-298 行）：
```rust
MATCH_ALL_UDF_NAME => {
    expr.args().len() == 1
        && extract_string_literal(&expr.args()[0])
            .map(|s| !o2_collect_search_tokens(&s).is_empty())
            .unwrap_or(false)
}
```
例如 `match_all('c')` 分词后为空 → **不可下推**。

### 7.3 索引优化模式

`LeaderIndexOptimizerRule` 和 `FollowerIndexOptimizerRule` 配合实现两级优化：

| 模式 | 匹配算子 | SQL 示例 |
|------|---------|----------|
| `SimpleCount` | AggregateExec | `SELECT count(*) FROM t WHERE name = 'oo'` |
| `SimpleSelect` | SortPreservingMergeExec | `SELECT * FROM t ORDER BY _timestamp DESC LIMIT 10` |
| `SimpleTopN` | Sort + Limit | `SELECT name, count(*) GROUP BY name ORDER BY cnt DESC LIMIT 10` |
| `SimpleDistinct` | Distinct | `SELECT DISTINCT name FROM t LIMIT 10` |
| `SimpleHistogram` | AggregateExec + histogram | `SELECT histogram(_timestamp, '1h'), count(*) GROUP BY 1` |

**Follower 优化器限制**（注释，第 55-56 行）：
> NOTE: use this optimizer in follower only when all filter can be extract to index condition(except _timestamp filter)

### 7.4 Filter 移除逻辑

**`construct_filter_exec`**（`index.rs:221-255`）：

```rust
// check if we can remove the filter
let is_remove_filter = self.is_remove_filter || index_conditions.can_remove_filter();

if is_remove_filter {
    // 构造新的 FilterExec，仅保留不可下推的条件
    let plan = construct_filter_exec(filter, other_conditions)?;
    return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
}
```

当 `other_conditions.is_empty()` 时，FilterExec 被完全移除，替换为 ProjectionExec 或直接返回输入。

### 7.5 下推执行流程

```
SQL → 逻辑计划 → PushDownFilter → PushDownLimit → 物理计划
                                              ↓
                                  IndexRule（提取索引条件）
                                              ↓
                                  检查 can_optimize（全部可下推？）
                                              ↓
                                  LeaderIndexOptimizer（全局优化）
                                              ↓
                                  RemoteScanRule（分布式下推）
```

---

## 八、分布式执行与回退机制

### 8.1 远程扫描执行（RemoteScanExec）

`RemoteScanExec`（`src/service/search/datafusion/distributed_plan/remote_scan_exec.rs`）是分布式执行的核心算子：

#### 核心属性：
```rust
pub struct RemoteScanExec {
    input: Arc<dyn ExecutionPlan>,          // 需要远程执行的子计划
    remote_scan_node: RemoteScanNode,       // 远程节点信息
    partitions: usize,                      // 输出分区数 = 节点数
    pub scan_stats: Arc<Mutex<ScanStats>>,  // 扫描统计
    pub partial_err: Arc<Mutex<String>>,    // 部分错误
    pub peak_memory: Arc<AtomicUsize>,      // 内存峰值
    // ...
}
```

### 8.2 RemoteScan 回退失败边界

**核心逻辑**：`get_remote_batch` 函数（第 273-423 行）

#### 可回退场景 ✅（返回空流 + 记录 partial_err）

| 阶段 | 错误类型 | 代码位置 | 处理方式 |
|------|---------|---------|---------|
| 连接建立 | 任何错误 | 347-361 | `get_empty_stream(empty_stream.with_error(e))` |
| RPC 调用 | `Cancelled` | 367-371 | 同上 |
| RPC 调用 | `DeadlineExceeded` | 367-371 | 同上 |
| RPC 调用 | `Internal` + Parquet 文件缺失 | 367-371 | 同上（`is_parquet_file_not_found` 检测） |
| 流读取超时 | `DeadlineExceeded` | 397-419 | 仅记录 partial_err，break 循环 |

**`is_parquet_file_not_found` 检测**（第 425-437 行）：
```rust
pub fn is_parquet_file_not_found(e: &tonic::Status) -> bool {
    e.code() == tonic::Code::Internal && {
        let msg = e.message();
        msg.find('{')
            .and_then(|start| msg.rfind('}').map(|end| &msg[start..=end]))
            .and_then(|json_part| infra::errors::ErrorCodes::from_json(json_part).ok())
            .map(|err_code| {
                err_code.get_code()
                    == infra::errors::ErrorCodes::SearchParquetFileNotFound.get_code()
            })
            .unwrap_or(false)
    }
}
```

#### 不可回退场景 ❌（返回错误，导致整个查询失败）

| 阶段 | 错误类型 | 代码位置 | 处理方式 |
|------|---------|---------|---------|
| RPC 调用 | 除上述 3 种外的所有错误 | 367-378 | `return Err(DataFusionError::Execution(e.to_string()))` |
| 流解码 | 任何错误 | 403-405 | `yield batch.map_err(\|e\| DataFusionError::Internal(...))` |
| 计划序列化 | 任何错误 | execute 函数 | 直接返回 Err |

**不可回退的错误码示例**：
- `PermissionDenied`：权限不足
- `InvalidArgument`：参数错误
- `Unauthenticated`：认证失败
- `NotFound`：资源不存在
- `AlreadyExists`：资源已存在
- `FailedPrecondition`：前置条件不满足
- `OutOfRange`：超出范围
- `Unimplemented`：未实现
- `Internal`（非 Parquet 文件缺失）：其他内部错误

#### 空文件列表快速返回

**非 super cluster 模式下的优化**（第 326-333 行）：
```rust
// fast return for empty file list querier node
if !is_super
    && is_querier
    && !is_ingester
    && !enrich_mode
    && remote_scan_node.is_file_list_empty(partition)
{
    return Ok(get_empty_stream(empty_stream));
}
```
querier 节点无文件时，直接返回空流，不发起 RPC 调用。

### 8.3 分布式计划重写

`RemoteScanRule`（`src/service/search/datafusion/optimizer/physical_optimizer/remote_scan.rs`）负责将单机物理计划转换为分布式计划：

#### 重写规则：

| 匹配算子 | 重写方式 |
|---------|---------|
| `RepartitionExec` | 子计划前插入 RemoteScanExec |
| `SortPreservingMergeExec` | 子计划前插入 RemoteScanExec |
| `UnionExec` | 每个子分支独立插入 RemoteScanExec |
| `HashJoinExec` | 左右子树独立插入 RemoteScanExec |
| 单节点优化 | 整个计划作为 RemoteScanExec 输入 |

### 8.4 错误处理逻辑

**`process_partial_err`**（`src/service/search/datafusion/distributed_plan/common.rs:164-172`）：

```rust
pub fn process_partial_err(partial_err: Arc<Mutex<String>>, e: tonic::Status) {
    let mut guard = partial_err.lock();
    let partial_err = guard.clone();
    if partial_err.is_empty() {
        guard.push_str(e.to_string().as_str());
    } else {
        guard.push_str(format!(" \n {e}").as_str());
    }
}
```

多个节点错误用 ` \n ` 分隔拼接。

### 8.5 执行流程图

```
用户请求
    ↓
Sql::new_from_req() → 解析 SQL、提取元信息、自动注入 _timestamp
    ↓
创建 SessionContext → 注册 UDF、配置优化规则
    ↓
create_logical_plan() → DataFusion 逻辑计划
    ↓
逻辑优化 → 直方图重写、排序限制、Filter/Limit 下推
    ↓
create_physical_plan() → 转换为物理计划
    ↓
物理优化 → IndexRule 提取索引条件 → 检查 can_optimize
    ↓
RemoteScanRule 插入 RemoteScanExec
    ↓
RemoteScanExec.execute() → 分发到各节点
    ├─ 节点1 → 正常执行 → 返回结果
    ├─ 节点2 → Cancelled/Deadline/ParquetNotFound → 返回空流 + partial_err
    ├─ 节点3 → PermissionDenied → 返回错误 → 查询失败
    └─ 节点4 → 流读取超时 → 记录 partial_err，继续返回已有结果
    ↓
结果合并 → 排序、聚合、分页
    ↓
返回给用户 → 标记 is_partial=true（如有部分失败）
```

---

## 九、复杂查询判定

`is_complex_query` 函数（`src/service/search/sql/visitor/utils.rs:47-129`）是查询规划的重要分支点：

```rust
pub fn is_complex_query(statement: &mut Statement) -> bool {
    let mut visitor = ComplexQueryVisitor::new();
    let _ = statement.visit(&mut visitor);
    visitor.is_complex
}

// 判定条件：
// 1. 子查询 (Subquery / Exists / InSubquery)
// 2. JOIN (from.len() > 1 或存在 joins)
// 3. GROUP BY
// 4. 聚合函数 / 窗口函数
// 5. 集合操作 (UNION / EXCEPT / INTERSECT)
// 6. DISTINCT
// 7. SELECT * 通配符
```

**影响**：
- 非复杂查询：自动注入 `_timestamp`、启用排序优化、索引下推更激进
- 复杂查询：保持标准 DataFusion 执行路径，更多计算在 Leader 节点完成

---

## 十、关键设计决策总结

### 1. 时间优先策略
- `_timestamp` 是一等公民，自动注入、自动排序、自动分片
- 利用时间局部性，迷你分片快速返回首批结果
- 无 `_timestamp` 列时回退到单分片统计估算

### 2. 索引激进下推
- 全局开关 + 字段白名单 + 表达式白名单三层检查
- 成功提取索引条件后可移除原 Filter 算子
- 支持全文检索、精确匹配、IN 列表等多种下推形式
- `match_all` 空 token 检测避免无效下推

### 3. 优雅降级机制（带边界）
- **可回退**：连接失败、取消、超时、Parquet 文件缺失
- **不可回退**：权限错误、参数错误、认证失败等
- 超时返回部分结果而非空错误
- 通过 `is_partial` 和 `partial_err` 告知用户数据完整性

### 4. 分层优化架构
- SQL 层重写（Visitor 模式）→ 逻辑优化 → 物理优化 → 分布式优化
- 每一层都有独立的规则集，可独立开关和扩展
- 企业版功能通过 feature flag 无缝插入

### 5. 用户函数隔离
- VRL 函数在运行时逐行解释执行
- 按组织隔离函数注册
- 内置函数与用户函数统一注册机制

### 6. `match_all` 语义限制
- 必须直接作用于物理流表，不能作用于中间结果（子查询/CTE/JOIN 的外层）
- 内部子查询/CTE 使用不受限
- 流必须配置 FTS 字段

---

## 十一、代码优化建议

### 1. VRL UDF 性能优化
当前实现是逐行解释执行 VRL，可考虑：
- 添加向量化执行支持
- 缓存编译后的 VRL 程序
- 对于简单函数提供原生 Rust 实现路径

### 2. 回退机制增强
当前仅对特定错误码回退，可考虑：
- 添加可配置的回退错误码列表
- 对于可重试错误（如网络波动）添加重试机制
- 支持动态调整超时时间

### 3. 分片策略自适应
当前分片策略相对固定，可考虑：
- 根据查询复杂度动态调整分片大小
- 基于历史执行数据的学习型分片策略

### 4. 索引下推扩展
当前仅支持等值和全文检索，可扩展：
- 支持范围查询下推到 zonemap 索引
- 支持布隆过滤器下推
- 支持 `NOT IN` 下推（当前仅支持 `IN`）

### 5. `equal_items` 与 `time_range` 统一
当前两条路径独立，可考虑：
- 统一分区条件提取逻辑，从 SQL 中自动提取 `_timestamp` 范围
- 避免请求参数与 SQL 条件不一致导致的查询范围扩大

### 6. `match_all` 限制提示
当前错误提示较笼统，可改进：
- 明确告知用户是 JOIN/CTE/子查询外层哪种场景
- 提供改写建议（如将 match_all 移入子查询内部）
