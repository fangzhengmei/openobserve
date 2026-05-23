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

### 3.4 `equal_items` 在各阶段的作用边界

⚠️ **关键纠正**：`equal_items` 不是只在 `search_partition` 阶段使用，而是在 `search_partition` → `match_file` → `match_source` → `filter_source_by_partition_key` 的完整链路上传递和转换。

#### 完整链路与数据格式转换

```
SQL WHERE 子句解析
    ↓
【PartitionColumnVisitor】
    ↓ 提取结果：HashMap<TableReference, Vec<(String, String)>>
    ↓ 例如：{logs: [("service", "api-gateway"), ("service", "worker")]}
    ↓
【search_partition 阶段】→ 文件列表查询后，在节点分配时传入
    ↓ 转换：调用 generate_filter_from_equal_items()
    ↓ 输入：&[(String, String)] 如 [("service", "api-gateway"), ("service", "worker")]
    ↓ 输出：Vec<(String, Vec<String>)> 如 [("service", ["api-gateway", "worker"])]
    ↓
【match_file 阶段】(mod.rs:1454-1489)
    ↓ 首先检查 fast path：
    ↓   • partition_keys.is_empty() ？
    ↓   • !source.key.contains('=') ？（文件路径不含分区键）
    ↓   • stream_type == EnrichmentTables ？
    ↓   → 任一满足则 return true（不过滤）
    ↓ slow path：
    ↓   • 对 equal_items 中的值应用 partition_key.get_partition_value() 转换
    ↓   • 转换为文件系统实际存储的分区值格式（如哈希、日期格式等）
    ↓
【match_source 阶段】(mod.rs:1507-1551)
    ↓ 检查 1：org_id/stream_type/stream_name 匹配
    ↓ 检查 2：调用 filter_source_by_partition_key()
    ↓ 检查 3：时间范围匹配
    ↓
【filter_source_by_partition_key】(config/src/utils/schema.rs:316-325)
    ↓ 核心逻辑：遍历所有过滤器，只要有一个不匹配就返回 false
    ↓ 对于每个 (field, values)：
    ↓   • 文件路径包含 "/field=" 吗？→ 不包含则继续下一个过滤器
    ↓   • 文件路径包含任一 "/field=value/" 吗？→ 都不包含则返回 false（过滤掉）
    ↓ 返回 true（所有过滤器都匹配或不相关）
```

#### 各阶段的具体作用

| 阶段 | 代码位置 | 输入格式 | 主要职责 |
|------|---------|---------|---------|
| **SQL 解析** | `sql/visitor/partition_column.rs` | `HashMap<TableReference, Vec<(String, String)>>` | 从 WHERE 子句提取等值和 IN 条件 |
| **格式转换** | `mod.rs:1493-1504` | `&[(String, String)]` → `Vec<(String, Vec<String>)>` | 将多个同名字段值合并为数组，便于后续匹配 |
| **match_file** | `mod.rs:1454-1489` | `&[(String, String)]` + `&[StreamPartition]` | 1. fast path 检查（无分区键直接跳过）<br>2. 应用分区键的 `get_partition_value()` 转换值格式 |
| **match_source** | `mod.rs:1507-1551` | `&[(String, Vec<String>)]` | 1. 检查 org/stream 匹配<br>2. 调用分区键过滤<br>3. 检查时间范围 |
| **filter_source_by_partition_key** | `config/src/utils/schema.rs:316-325` | `&str` + `&[(String, Vec<String>)]` | 基于文件路径字符串的精确匹配，决定单个文件是否保留 |

#### `filter_source_by_partition_key` 核心算法

```rust
pub fn filter_source_by_partition_key(source: &str, filters: &[(String, Vec<String>)]) -> bool {
    !filters.iter().any(|(k, v)| {
        // 1. 构造字段键，如 "service="
        let field = format_partition_key(&format!("{k}="));
        // 2. 检查文件路径是否包含该字段分区目录
        find(source, &format!("/{field}"))
            // 3. 如果包含，检查是否包含任一允许的值
            && !v.iter().any(|v| {
                let value = format_partition_key(&format!("{k}={v}"));
                find(source, &format!("/{value}/"))
            })
    })
}
```

**文件路径示例**：
```
files/default/logs/access_log/service=api-gateway/date=2025/year=2025/month=05/day=20/...
```

**匹配逻辑示例**：
- 过滤器：`[("service", ["api-gateway", "worker"])]`
- 文件路径包含 `/service=` ✅
- 文件路径包含 `/service=api-gateway/` ✅ → 保留
- 文件路径包含 `/service=database/` ❌ → 过滤掉

#### `generate_filter_from_equal_items` 格式转换

**代码**：`mod.rs:1493-1504`

```rust
/// before [("a", "3"), ("b", "5"), ("a", "4"), ("b", "6")]
/// after [("a", ["3", "4"]), ("b", ["5", "6"])]
pub fn generate_filter_from_equal_items(
    equal_items: &[(String, String)],
) -> Vec<(String, Vec<String>)> {
    let mut filters: HashMap<String, Vec<String>> = HashMap::new();
    for (field, value) in equal_items {
        filters.entry(field.to_string())
            .or_default()
            .push(value.to_string());
    }
    filters.into_iter().collect()
}
```

#### fast path 跳过条件

`match_file` 函数开头的三个条件，任一满足则跳过分区键过滤：

1. `partition_keys.is_empty()`：流没有配置分区键
2. `!source.key.contains('=')`：文件路径中不包含 `=` 字符，说明不是分区存储
3. `stream_type == StreamType::EnrichmentTables`：富集表类型不做分区过滤

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

### 4.2 排序优化

当查询仅按 `_timestamp` 降序排序时（`sorted_by_time` 标记），启用特殊优化：
- `split_file_groups_by_statistics = true`：按文件统计信息分割文件组
- `with_file_sort_order`：指定 Parquet 文件按 `_timestamp` 降序排序
- 避免全局排序，利用文件的已有排序性

### 4.3 直方图时间处理

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

### ⚠️ 关键纠正：`skip_get_file_list` 与 `PartitionGenerator` 的关系

**之前的错误理解**：
> `skip_get_file_list = true`：不查询真实文件列表，改用统计信息估算生成单个虚拟 FileId，后续 `PartitionGenerator` 看到只有一个文件，返回单分片。

**正确的理解**（来自 `mod.rs:842-850`）：

```rust
if skip_get_file_list {
    let mut response = search::SearchPartitionResponse::default();
    response.partitions.push([req.start_time, req.end_time]); // 直接硬编码单分片
    response.max_query_range = max_query_range_in_hour;
    response.histogram_interval = sql.histogram_interval;
    response.is_histogram_eligible = is_histogram_eligible;
    log::info!("[trace_id {trace_id}] search_partition: returning single partition");
    return Ok(response); // 直接返回，完全绕过 PartitionGenerator！
};
```

**核心区别**：

| 路径 | `skip_get_file_list` 值 | 是否经过 PartitionGenerator | 处理位置 |
|------|------------------------|--------------------------|---------|
| **多分片路径** | `false` | ✅ 是 | `mod.rs:1062` 创建 PartitionGenerator → `generate_partitions()` |
| **单分片路径** | `true` | ❌ 否，完全绕过 | `mod.rs:842` 直接硬编码 `[[start, end]]` 并返回 |

### 6.1 单分片返回的完整触发条件汇总

| 触发条件 | 代码位置 | 触发 `skip_get_file_list` | 是否经过 PartitionGenerator |
|---------|---------|------------------------|--------------------------|
| 聚合查询 | `partition.rs:83-84` | `false`（正常流程） | ✅ 是，`is_aggregate=true` 时内部返回单分片 |
| 无 `_timestamp` 列 | `mod.rs:688-704` | `true`（`ts_column.is_none()`） | ❌ 否，提前返回 |
| EXPLAIN 查询 | `mod.rs:729-731` | `true`（`is_explain_query`） | ❌ 否，提前返回 |
| HTTP DISTINCT | `mod.rs:729-731` | `true`（`is_http_distinct`） | ❌ 否，提前返回 |
| `apply_over_hits` | `mod.rs:704` | `true`（结果集二次计算） | ❌ 否，提前返回 |

### 6.1.1 `get_ts_col_order_by` 在 enterprise 与非 enterprise 下的分支差异

⚠️ **关键分支**：`get_ts_col_order_by` 的实现在 enterprise 和非 enterprise 版本下完全不同，直接影响 `skip_get_file_list` 的判定结果。

**代码位置**：`service/search/cache/cacher.rs:626-678`

#### 函数签名
```rust
pub fn get_ts_col_order_by(
    parsed_sql: &Sql,
    _ts_col: &str,
    _is_aggregate: bool,
) -> Option<(String, bool)> // (时间列名, 是否降序)
```

#### 非 enterprise 版本实现

```rust
#[cfg(not(feature = "enterprise"))]
{
    let mut ts_col = String::new();
    
    // 步骤 1：从 aliases 中查找 _timestamp 或 histogram 的别名
    for (original, alias) in &parsed_sql.aliases {
        if original == _ts_col || original.contains("histogram") {
            ts_col = alias.clone();
        }
    }
    
    // 步骤 2：非聚合查询时，检查 columns 或 order_by 是否包含 _timestamp
    if !_is_aggregate
        && (parsed_sql
            .columns
            .iter()
            .any(|(_, v)| v.contains(&_ts_col.to_owned()))
            || parsed_sql.order_by.iter().any(|v| v.0.eq(&_ts_col)))
    {
        ts_col = _ts_col.to_string();
    }
    ts_col
}
```

**非 enterprise 版本逻辑**：
1. 优先从 `aliases` 中查找时间列或 histogram 表达式的别名
2. 非聚合查询时，如果 `columns` 或 `order_by` 包含 `_timestamp`，则直接使用 `_timestamp`
3. 最后检查 `order_by` 中是否包含该时间列，确定排序方向
4. 如果 `ts_col` 为空则返回 `None`

#### enterprise 版本实现

```rust
#[cfg(feature = "enterprise")]
{
    match o2_enterprise::enterprise::search::cache_ts_util::get_timestamp_column_name(
        &parsed_sql.sql,
    ) {
        Some(result) => result,
        None => "".to_string(),
    }
}
```

**enterprise 版本逻辑**：
1. 调用企业版专用函数 `get_timestamp_column_name`
2. 传入**原始 SQL 字符串**（而非解析后的结构）
3. 由企业版内部实现更复杂的时间列检测逻辑
4. 无结果时返回空字符串

#### 核心差异对比

| 维度 | 非 enterprise | enterprise |
|------|---------------|-----------|
| **输入** | `parsed_sql` 结构（aliases, columns, order_by） | `parsed_sql.sql` 原始字符串 |
| **检测方式** | 简单的别名和字段名匹配 | 企业版专用复杂解析 |
| **别名处理** | 显式遍历 `aliases` 匹配 | 内部实现，对用户不可见 |
| **histogram 支持** | 仅检查 original.contains("histogram") | 内部实现 |

#### 对 `skip_get_file_list` 的影响

**调用链**：
```
mod.rs:690
    ↓
let ts_column = get_ts_col_order_by(&sql, TIMESTAMP_COL_NAME, is_aggregate).map(|(v, _)| v);
    ↓
mod.rs:704
    ↓
let mut skip_get_file_list = ts_column.is_none() || apply_over_hits;
```

**影响路径**：

| 场景 | 非 enterprise 行为 | enterprise 行为 | 对 `skip_get_file_list` 的影响 |
|------|------------------|-------------|-----------------------------|
| SQL: `SELECT histogram(_timestamp, '1h') as h, count(*) FROM t GROUP BY h ORDER BY h DESC` | ✅ 从 aliases 中匹配到 `histogram` → `ts_col = "h"` → `ts_column.is_some()` | ✅ 企业版解析原始 SQL → 返回时间列名 → `ts_column.is_some()` | `skip_get_file_list = false`，正常多分片 |
| SQL: `SELECT service, count(*) FROM t GROUP BY service ORDER BY count(*) DESC` | ❌ aliases 无时间相关 → columns/order_by 也无 → `ts_col = ""` → `ts_column.is_none()` | ❌ 企业版也可能返回空 → `ts_column.is_none()` | `skip_get_file_list = true`，单分片提前返回 |
| SQL: `SELECT * FROM t ORDER BY log_level ASC` | ❌ order_by 是 log_level → `ts_column.is_none()` | ❌ 同上 | `skip_get_file_list = true` |
| SQL: `SELECT * FROM t ORDER BY _timestamp DESC` | ✅ order_by 有 _timestamp → `ts_col = "_timestamp"` | ✅ 企业版也能识别 | `skip_get_file_list = false` |

#### 排序方向确定逻辑（两个版本共用）

```rust
if !order_by.is_empty() && !result_ts_col.is_empty() {
    for (field, order) in order_by {
        if is_timestamp_field(field, &result_ts_col) {
            is_descending = order == &OrderBy::Desc;
            break;
        }
    }
};
```

遍历 `order_by`，找到与时间列匹配的字段，确定是升序还是降序。

### 6.2 PartitionGenerator 分片生成逻辑

`PartitionGenerator`（`src/service/search/partition.rs`）是分片策略的核心，**仅在 `skip_get_file_list = false` 时被调用**：

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

### 6.4 多分片路径完整流程（`skip_get_file_list = false`）

```
1. 用 time_range 查询 S3 文件列表 → 得到文件列表 A
2. 用 equal_items 二次过滤 → 得到文件列表 B
3. 计算分片参数：
   - total_secs = original_size / base_speed / cpu_cores
   - part_num = max(1, total_secs / query_partition_by_secs)
   - step = (end_time - start_time) / part_num
4. 创建 PartitionGenerator
5. 调用 generate_partitions() 生成分片列表
```

---

## 七、索引下推机制

### 7.1 `can_optimize` 与 FilterExec 保留/移除的真实关系

⚠️ **关键纠正**：之前混淆了 `is_remove_filter`、`can_optimize` 和 Filter 最终形态的关系。这是三个独立但相关的决策点。

**代码位置**：`optimizer/physical_optimizer/index.rs:195-255`

#### 核心决策逻辑

```rust
// 决策点 1：是否尝试修改 Filter
let is_remove_filter = self.is_remove_filter || index_conditions.can_remove_filter();

// 决策点 2：提取索引条件（无论是否移除 Filter）
if !index_conditions.is_empty() {
    *self.index_condition.lock() = Some(index_conditions);
}

if is_remove_filter {
    // 决策点 3：是否开启高级优化（SimpleCount/SimpleSelect 等模式）
    if self.optimizer_enabled
        && is_only_timestamp_filter(&other_conditions.iter().collect::<Vec<_>>())
    {
        self.can_optimize = true;  // 独立标志，用于后续 Leader/Follower 优化器
    }
    // 决策点 4：构造新 Filter（或完全移除）
    let plan = construct_filter_exec(filter, other_conditions)?;
    return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
} else {
    // 不修改 Filter，索引条件仍会被提取但 Filter 保持原样
    return Ok(Transformed::new(node, false, TreeNodeRecursion::Stop));
}
```

#### 三个决策变量的含义

| 变量 | 含义 | 控制因素 |
|------|------|---------|
| `is_remove_filter` | 是否**尝试**修改 Filter 的谓词 | 配置 `feature_query_remove_filter_with_index` **或** `index_conditions.can_remove_filter()`（所有非时间条件都可下推） |
| `can_optimize` | 是否启用**高级优化模式**（SimpleCount/SimpleSelect 等） | `is_remove_filter = true` **且** `optimizer_enabled` **且** `other_conditions` 仅含时间过滤 |
| `other_conditions.is_empty()` | Filter 是否被**完全移除** | 所有谓词都可下推，没有不可下推的条件残留 |

#### FilterExec 的 4 种最终形态

| 场景 | `is_remove_filter` | `other_conditions` | `can_optimize` | FilterExec 最终形态 |
|------|-------------------|-------------------|---------------|-------------------|
| ✅ 完美下推 | `true` | 空 | `true` | **完全移除**，替换为 ProjectionExec 或直接返回 input |
| ⚠️ 部分下推 | `true` | 非空（仅时间过滤） | `true` | **被替换**，新 Filter 仅保留时间过滤条件 |
| ⚠️ 部分下推 | `true` | 非空（含其他字段） | `false` | **被替换**，新 Filter 保留不可下推的条件，索引条件仍被提取 |
| ❌ 不下推 | `false` | 全部 | `false` | **保持原样**，索引条件仍被提取但不用于后续高级优化 |

#### `construct_filter_exec` 逻辑（`index.rs:221-255`）

```rust
fn construct_filter_exec(...) -> Result<Arc<dyn ExecutionPlan>> {
    if exprs.is_empty() {
        // 完全移除 Filter
        let plan = match filter.projection() {
            Some(projection_indices) => Arc::new(ProjectionExec::try_new(...)?),
            None => filter.input().clone(),
        };
        if let Some(fetch) = filter.fetch() {
            plan = Arc::new(LocalLimitExec::new(plan, fetch));
        }
        Ok(plan)
    } else {
        // 替换 Filter，仅保留不可下推的条件
        let plan = FilterExecBuilder::new(conjunction(exprs), filter.input().clone())
            .apply_projection_by_ref(filter.projection().as_ref())?
            .with_fetch(filter.fetch())
            .build()?;
        Ok(Arc::new(plan))
    }
}
```

#### 索引下推生效的完整条件链

```
配置开关开启 (inverted_index_enabled)
    ↓
1. 字段必须在 index_fields 集合中（建表时指定的索引字段）
    ↓
2. 表达式类型必须匹配（见下表）
    ↓
3. 索引条件被提取并写入 IndexCondition（无论 Filter 是否被修改）
    ↓
4. 【修改 Filter】is_remove_filter = true 进入修改分支
    ↓
5. 【启用高级优化】can_optimize = true 需要同时满足：
   a. is_remove_filter = true
   b. optimizer_enabled = true
   c. other_conditions 仅含 _timestamp 过滤（is_only_timestamp_filter）
    ↓
6. 【Leader/Follower 优化器】计划结构匹配 SimpleCount / SimpleSelect 等模式
```

### 7.2 索引下推的两个层次

⚠️ **重要区别**：索引下推分为两个独立层次：

| 层次 | 效果 | 触发条件 |
|------|------|---------|
| **基础下推** | 提取 `IndexCondition` 注入 TableScan，利用索引减少数据扫描 | 条件 1-3 满足即可，不需要 `is_remove_filter` |
| **高级优化** | 启用 `SimpleCount`/`SimpleSelect` 等模式，进一步优化执行计划 | 需要 `can_optimize = true` + 计划结构匹配 |

这意味着：
- 即使 `is_remove_filter = false`（Filter 保持原样），索引条件仍会被提取和使用
- 但只有 `can_optimize = true` 时，才会启用更激进的执行计划优化模式

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

## 十、完整执行链路场景拆解

以下用 3 个代表性 SQL 场景，逐步拆解从解析校验到分片决策再到索引下推与 RemoteScan 回退的完整链路。

---

### 场景 1：简单日志查询（多分片 + 索引下推 + 部分回退）

**SQL**：
```sql
SELECT log_level, message 
FROM logs 
WHERE service = 'api-gateway' 
  AND _timestamp BETWEEN 1717209600000000 AND 1717296000000000
ORDER BY _timestamp DESC 
LIMIT 100
```

**请求参数**：
- `start_time = 1717209600000000`
- `end_time = 1717296000000000`
- `stream_type = logs`

---

#### 阶段 1：SQL 解析与元信息提取（`Sql::new_from_req`）

**步骤 1.1：SQL 语法解析**
- 代码：`sql/mod.rs:156-159`
- 触发条件：有效的 SQL 语法 ✅
- 结果：成功解析为 `Statement::Query`

**步骤 1.2：各类 Visitor 遍历提取元信息**

| Visitor | 命中分支 | 结果 | 代码位置 |
|---------|---------|------|---------|
| `ColumnVisitor` | 命中 ORDER BY + LIMIT | `columns = {logs: [log_level, message, service, _timestamp]}`, `order_by = [(_timestamp, DESC)]`, `limit = 100` | `sql/visitor/` |
| `MatchVisitor` | 未命中 match_all | `has_match_all = false` | `sql/visitor/match_all.rs` |
| `PartitionColumnVisitor` | 命中 `service = 'api-gateway'` | `equal_items = {logs: [("service", "api-gateway")]}` | `sql/visitor/partition_column.rs:46-151` |
| `HistogramIntervalVisitor` | 未命中直方图 | `histogram_interval = None` | - |
| `ComplexQueryVisitor` | 未命中（无聚合/JOIN等） | `is_complex = false` | `sql/visitor/utils.rs:47-129` |

**步骤 1.3：自动注入 `_timestamp`**
- 代码：`sql/mod.rs:274-281`
- 触发条件：`is_complex = false` ✅
- 结果：SELECT 列表被重写为 `SELECT _timestamp, log_level, message FROM ...`

**步骤 1.4：`time_range` 来源**
- 代码：`sql/mod.rs:302`
- 来源：`query.start_time` 和 `query.end_time`（请求参数，**不是 SQL 解析**）
- 结果：`time_range = Some((1717209600000000, 1717296000000000))`

**步骤 1.5：`sorted_by_time` 判定**
- 触发条件：仅按 `_timestamp` 排序 ✅
- 结果：`sorted_by_time = true`

---

#### 阶段 2：分片决策（`search_partition`）

**步骤 2.1：检查单分片触发条件**
- 代码：`mod.rs:687-704`
- `is_explain_query = false`
- `is_aggregate = false`
- `ts_column = get_ts_col_order_by(...)` → `Some("_timestamp")`（ORDER BY 包含 `_timestamp`）
- `apply_over_hits = false`
- **结果**：`skip_get_file_list = false` → 进入多分片分支

**步骤 2.2：查询文件列表**
- 代码：`mod.rs:760-765`
1. 用 `time_range` 从 S3 查询文件列表 → 得到文件列表 A（100 个文件）
2. 用 `equal_items` 对文件列表 A 做二次过滤 → 得到文件列表 B（30 个文件）

**步骤 2.3：计算分片参数**
- 代码：`mod.rs:944-999`
- `original_size = 3GB`（30 个文件总大小）
- `cpu_cores = 16`（集群 querier 节点 CPU 总和）
- `base_speed = 100MB/s`
- `total_secs = 3GB / (100MB/s * 16) = 1.875` 秒
- `part_num = 4`（根据配置调整）
- `step = (end_time - start_time) / 4 = 21600_000_000 微秒 = 6 小时`

**步骤 2.4：创建 PartitionGenerator 并生成分片**
- 代码：`mod.rs:1062-1066` + `partition.rs:56-88`
- `generator = PartitionGenerator::new(min_step=1s, mini_partition_duration_secs=60s, is_histogram=false)`
- 调用 `generate_partitions(start, end, step, DESC, is_aggregate=false, add_mini_partition=false)`
- 命中分支：`else`（非直方图、非聚合）→ `generate_partitions_with_mini_partition`
- 结果（按 DESC 排序，优先执行最近的分片）：
  ```
  [
    [1717285200000000, 1717296000000000],  // 分片 1：最近 3 小时（迷你分片）
    [1717274400000000, 1717285200000000],  // 分片 2：前 3 小时
    [1717252800000000, 1717274400000000],  // 分片 3：前 6 小时
    [1717209600000000, 1717252800000000],  // 分片 4：最早 12 小时
  ]
  ```

---

#### 阶段 3：索引下推优化

**步骤 3.1：创建 SessionContext 并注册 UDF**
- 代码：`exec.rs:265-312`
- 注册约 20 个内置 UDF + 用户自定义 VRL 函数

**步骤 3.2：生成逻辑计划与逻辑优化**
- DataFusion 内置优化：`PushDownFilter`、`PushDownLimit`
- OpenObserve 自定义优化：`SortLimitRule`（命中）

**步骤 3.3：生成物理计划**
- 初始结构：
  ```
  SortPreservingMergeExec: [_timestamp DESC], fetch=100
    FilterExec: service = 'api-gateway' AND _timestamp BETWEEN ...
      NewEmptyExec: name="logs"
  ```

**步骤 3.4：IndexRule 提取索引条件与 Filter 决策**
- 代码：`optimizer/physical_optimizer/index.rs:86-255`
- 全局开关：`inverted_index_enabled = true` ✅

**索引条件提取**：
遍历 FilterExec 谓词：
  1. `service = 'api-gateway'`：
     - `column = "service"` 在 `index_fields` 中 ✅
     - 是等值比较 ✅
     - 生成 `Condition::Equal("service", "api-gateway")`，加入 `index_conditions`
  2. `_timestamp BETWEEN ...`：
     - 是范围比较 ❌
     - 归为 `other_conditions`

**决策点 1：是否尝试修改 Filter**
- `index_conditions.can_remove_filter()` → 所有非时间条件都可下推 ✅
- `is_remove_filter = self.is_remove_filter || true` → `true`

**决策点 2：是否启用高级优化**
- `optimizer_enabled = true` ✅
- `is_only_timestamp_filter(&other_conditions)` → true ✅
- `can_optimize = true` → 启用 SimpleSelect 等高级优化模式

**决策点 3：FilterExec 最终形态**
- `other_conditions` 非空（含时间过滤）❌
- 调用 `construct_filter_exec(filter, other_conditions)`
- **结果**：FilterExec **被替换**，新 Filter 仅保留时间过滤条件，索引条件已提取到 `IndexCondition`

**索引下推的两个层次**：
1. **基础下推** ✅：`IndexCondition` 已提取，TableScan 可利用索引减少扫描
2. **高级优化** ✅：`can_optimize = true`，后续 LeaderIndexOptimizer 可匹配 SimpleSelect 模式

**步骤 3.5：LeaderIndexOptimizerRule 匹配优化模式**
- 代码：`optimizer/physical_optimizer/index_optimizer/mod.rs:119-151`
- 检查 `is_complex_plan` → false ✅
- 匹配 `SortPreservingMergeExec`：
  - 调用 `is_simple_select(plan)` → `Some(SimpleSelect(100, false))` ✅
- **结果**：`index_optimizer_mode = Some(IndexOptimizeMode::SimpleSelect(100, false))`

---

#### 阶段 4：分布式计划重写

**步骤 4.1：RemoteScanRule 插入 RemoteScanExec**
- 代码：`optimizer/physical_optimizer/remote_scan.rs`
- 匹配 `SortPreservingMergeExec` → 子计划前插入 `RemoteScanExec`
- 4 个分片分配到 4 个 querier 节点

---

#### 阶段 5：RemoteScan 回退处理

**节点 1（正常）**：
- `make_flight_client` 成功 ✅
- `client.do_get(request)` 成功 ✅
- 返回 30 条记录

**节点 2（正常）**：
- 正常执行，返回 25 条记录

**节点 3（流读取超时）**：
- 连接建立成功 ✅
- `client.do_get(request)` 成功 ✅
- 流读取时 `tokio::select!` 触发 `DeadlineExceeded`
- 代码：`remote_scan_exec.rs:397-419`
- 调用 `process_partial_err(partial_err, e)` → 记录错误
- break 循环，仅返回已收到的 20 条记录

**节点 4（Parquet 文件缺失）**：
- `client.do_get(request)` 返回 `Internal` 错误
- `is_parquet_file_not_found(e)` → true ✅
- 代码：`remote_scan_exec.rs:367-371`
- 返回 `get_empty_stream(empty_stream.with_error(e))`
- 记录 `partial_err`，不返回任何数据

**最终结果**：
- 总记录数：30 + 25 + 20 + 0 = 75 条
- `is_partial = true`
- `partial_err = "DeadlineExceeded: timeout \n Internal: Parquet file not found"`
- 按 `_timestamp DESC` 排序后取前 100 条（实际只有 75 条）

---

### 场景 2：带 match_all 的 TopN 聚合查询（单分片 + 索引下推 + 失败回退）

**SQL**：
```sql
SELECT service, count(*) as cnt 
FROM logs 
WHERE match_all('error timeout') 
  AND _timestamp BETWEEN 1717209600000000 AND 1717296000000000
GROUP BY service 
ORDER BY cnt DESC 
LIMIT 10
```

**请求参数**：
- `start_time = 1717209600000000`
- `end_time = 1717296000000000`

---

#### 阶段 1：SQL 解析与元信息提取

**步骤 1.1：各类 Visitor 遍历**

| Visitor | 命中分支 | 结果 |
|---------|---------|------|
| `ColumnVisitor` | 命中 GROUP BY + 聚合 | `columns = {logs: [service, _timestamp]}`, `group_by = [service]`, `has_agg_function = true`, `order_by = [(cnt, DESC)]` |
| `MatchVisitor` | 命中 match_all 且直接作用于流表 | `has_match_all = true`, `is_support_match_all = true` |
| `PartitionColumnVisitor` | 未命中（无等值条件） | `equal_items = {}` |
| `ComplexQueryVisitor` | 命中（GROUP BY + 聚合） | `is_complex = true` |

**步骤 1.2：自动注入 `_timestamp`**
- 触发条件：`is_complex = true` ❌
- **结果**：不自动注入 `_timestamp`

**步骤 1.3：`sorted_by_time` 判定**
- ORDER BY 是 `cnt DESC`，不是 `_timestamp` ❌
- **结果**：`sorted_by_time = false`

---

#### 阶段 2：分片决策

**步骤 2.1：检查单分片触发条件**
- 代码：`mod.rs:687-704`
- `is_aggregate = true`（有 GROUP BY + 聚合）
- `ts_column = get_ts_col_order_by(&sql, "_timestamp", is_aggregate=true).map(|(v, _)| v)`：
  - **非 enterprise 版本**：aliases 无时间/histogram → columns/order_by 也无 `_timestamp` → `ts_col = ""` → `ts_column.is_none()` ✅
  - **enterprise 版本**：调用企业版解析原始 SQL，也无法识别 `cnt` 为时间列 → `ts_column.is_none()` ✅
- **结果**：`skip_get_file_list = true` → 提前返回单分片

**步骤 2.2：提前返回（完全绕过 PartitionGenerator）**
- 代码：`mod.rs:842-850`
- **不查询文件列表，不创建 PartitionGenerator**
- 直接硬编码返回：`partitions = [[1717209600000000, 1717296000000000]]`

---

#### 阶段 3：索引下推优化

**步骤 3.1：IndexRule 提取索引条件与 Filter 决策**
- 代码：`optimizer/physical_optimizer/index.rs:86-255`
- 全局开关：`inverted_index_enabled = true` ✅

**索引条件提取**：
- 谓词：`match_all('error timeout') AND _timestamp BETWEEN ...`
- `match_all('error timeout')`：
  - 函数名匹配 ✅
  - 参数是字符串 ✅
  - `o2_collect_search_tokens("error timeout") = ["error", "timeout"]` → 非空 ✅
  - 生成 `Condition::MatchAll("error timeout")`，加入 `index_conditions`
- `_timestamp BETWEEN ...` → 归为 `other_conditions`

**决策点 1：是否尝试修改 Filter**
- `index_conditions.can_remove_filter()` → true ✅
- `is_remove_filter = true`

**决策点 2：是否启用高级优化**
- `optimizer_enabled = true` ✅
- `is_only_timestamp_filter(&other_conditions)` → true ✅
- `can_optimize = true` → 启用 SimpleTopN 高级优化模式

**决策点 3：FilterExec 最终形态**
- `other_conditions` 非空（含时间过滤）❌
- 调用 `construct_filter_exec(filter, other_conditions)`
- **结果**：FilterExec **被替换**，新 Filter 仅保留时间过滤条件

**索引下推的两个层次**：
1. **基础下推** ✅：`IndexCondition` 已提取，TableScan 可利用索引
2. **高级优化** ✅：`can_optimize = true`，后续 LeaderIndexOptimizer 可匹配 SimpleTopN 模式

**步骤 3.2：LeaderIndexOptimizerRule 匹配优化模式**
- 计划结构：
  ```
  Sort: [cnt DESC], fetch=10
    AggregateExec: groupBy=[service], aggr=[count(*)]
      FilterExec: match_all('error timeout') AND _timestamp BETWEEN ...
        TableScan: logs
  ```
- 匹配 `is_simple_topn(plan)` → `Some(SimpleTopN(10, ["service"], false))` ✅
- **结果**：`index_optimizer_mode = Some(IndexOptimizeMode::SimpleTopN(...))`

---

#### 阶段 4：RemoteScan 执行与回退

**节点 1（PermissionDenied）**：
- `make_flight_client` 成功 ✅
- `client.do_get(request)` 返回 `PermissionDenied` 错误
- 检查：`Cancelled? ❌`, `DeadlineExceeded? ❌`, `ParquetFileNotFound? ❌`
- **不可回退** → `return Err(DataFusionError::Execution("PermissionDenied: ..."))`
- **整个查询失败**，错误返回给用户

---

### 场景 3：EXPLAIN 或无时间列排序（单分片分支 + 跳过 PartitionGenerator）

#### 场景 3a：EXPLAIN ANALYZE

**SQL**：
```sql
EXPLAIN ANALYZE SELECT * FROM logs WHERE service = 'api-gateway' ORDER BY _timestamp DESC LIMIT 100
```

**阶段 1：SQL 解析**
- `is_explain_query = true`（SQL 以 `EXPLAIN` 开头）

**阶段 2：分片决策**
- 代码：`mod.rs:729-731`
- `is_explain_query = true` → `skip_get_file_list = true`
- 代码：`mod.rs:842-850` → 直接返回单分片
- **完全绕过 PartitionGenerator**

**阶段 3：特殊执行计划**
- 最外层包装 `DistributeAnalyzeExec`
- 输出 schema：`phase, node_address, node_name, plan`
- 执行时收集各节点的执行计划和 metrics

---

#### 场景 3b：无时间列排序

**SQL**：
```sql
SELECT log_level, count(*) 
FROM logs 
WHERE service = 'api-gateway' 
  AND _timestamp BETWEEN 1717209600000000 AND 1717296000000000
GROUP BY log_level 
ORDER BY log_level ASC
```

**阶段 1：SQL 解析**
- `is_aggregate = true`
- ORDER BY 是 `log_level ASC`，不是 `_timestamp`
- `PartitionColumnVisitor` 命中 `service = 'api-gateway'` → `equal_items = {logs: [("service", "api-gateway")]}`

**阶段 2：分片决策**
- 代码：`mod.rs:688-704`
- `ts_column = get_ts_col_order_by(&sql, "_timestamp", is_aggregate=true).map(|(v, _)| v)`：
  - **非 enterprise 版本**：
    - aliases 无时间/histogram 别名 ✅
    - is_aggregate = true，跳过 columns/order_by 检查 ✅
    - `ts_col = ""` → `ts_column.is_none()` ✅
  - **enterprise 版本**：
    - 解析原始 SQL，ORDER BY 是 `log_level ASC`，无法识别为时间列 ✅
    - `ts_column.is_none()` ✅
- `skip_get_file_list = true`
- 代码：`mod.rs:842-850` → 直接返回单分片
- **完全绕过 PartitionGenerator，不查询文件列表**

**阶段 3：equal_items 作用链路**
虽然 `skip_get_file_list = true` 绕过了文件列表查询，但 `equal_items` 仍会在后续执行阶段使用：

| 阶段 | 作用 |
|------|------|
| **search_partition** | 由于 `skip_get_file_list = true`，不查询文件列表，因此 `equal_items` 在此阶段不生效 |
| **match_file** | 如果后续有文件匹配需求（如 WAL 查询），`equal_items` 会被转换为 filters |
| **filter_source_by_partition_key** | 如果有文件路径需要检查，会基于 `/service=api-gateway/` 进行字符串匹配 |

**阶段 4：索引下推**
- 谓词：`service = 'api-gateway' AND _timestamp BETWEEN ...`
- `service = 'api-gateway'` 可下推 → `Condition::Equal("service", "api-gateway")`
- `_timestamp BETWEEN ...` → 归为 `other_conditions`

**决策点**：
1. `is_remove_filter = true`（所有非时间条件可下推）✅
2. `optimizer_enabled = true` ✅
3. `is_only_timestamp_filter(&other_conditions)` → true ✅
4. `can_optimize = true` ✅
5. `other_conditions` 非空 → FilterExec **被替换**，保留时间过滤

**索引下推的两个层次**：
1. **基础下推** ✅：`IndexCondition` 已提取
2. **高级优化** ❌：计划结构是聚合 + 非时间排序，不匹配 SimpleCount/SimpleSelect 等模式
   - `is_simple_count(plan)` → false（有 GROUP BY）
   - `is_simple_select(plan)` → false（有聚合）
   - `is_simple_topn(plan)` → false（ORDER BY 是 `log_level ASC`，不是聚合结果）

---

## 十一、关键设计决策总结

### 1. 时间优先策略
- `_timestamp` 是一等公民，自动注入、自动排序、自动分片
- 利用时间局部性，迷你分片快速返回首批结果
- 无 `_timestamp` 列时回退到单分片统计估算
- `get_ts_col_order_by` 区分 enterprise/非 enterprise 实现，直接影响分片决策

### 2. 索引激进下推（双层次）
- **三层检查**：全局开关 + 字段白名单 + 表达式白名单
- **两个独立层次**：
  - 基础下推：提取 `IndexCondition` 注入 TableScan，减少数据扫描
  - 高级优化：`can_optimize = true` 时启用 SimpleCount/SimpleSelect 等模式
- **三个独立决策点**：
  - `is_remove_filter`：是否尝试修改 Filter
  - `can_optimize`：是否启用高级优化模式
  - `other_conditions.is_empty()`：Filter 是否被完全移除
- 即使 `is_remove_filter = false`，索引条件仍会被提取和使用
- `match_all` 空 token 检测避免无效下推

### 3. `equal_items` 全链路传递
- 从 SQL WHERE 子句提取分区键等值条件
- 在 `search_partition` → `match_file` → `match_source` → `filter_source_by_partition_key` 完整链路上传递和转换
- `generate_filter_from_equal_items` 格式转换：`Vec<(String, String)>` → `Vec<(String, Vec<String>)>`
- `match_file` 支持 fast path 跳过（无分区键/无 `=` 字符/富集表类型）
- 最终基于文件路径字符串匹配，不依赖元数据

### 4. 优雅降级机制（带边界）
- **可回退**：连接失败、取消、超时、Parquet 文件缺失
- **不可回退**：权限错误、参数错误、认证失败等
- 超时返回部分结果而非空错误
- 通过 `is_partial` 和 `partial_err` 告知用户数据完整性

### 5. 分层优化架构
- SQL 层重写（Visitor 模式）→ 逻辑优化 → 物理优化 → 分布式优化
- 每一层都有独立的规则集，可独立开关和扩展
- 企业版功能通过 feature flag 无缝插入
- `get_ts_col_order_by` 非 enterprise 用解析结构，enterprise 用原始 SQL 字符串

### 6. 用户函数隔离
- VRL 函数在运行时逐行解释执行
- 按组织隔离函数注册
- 内置函数与用户函数统一注册机制

### 7. `match_all` 语义限制
- 必须直接作用于物理流表，不能作用于中间结果（子查询/CTE/JOIN 的外层）
- 内部子查询/CTE 使用不受限
- 流必须配置 FTS 字段

### 8. 单分片双路径设计
- **路径 A（聚合查询）**：在 PartitionGenerator 内部返回单分片，经过完整文件列表查询
- **路径 B（无时间列/EXPLAIN 等）**：在 mod.rs 中提前返回，完全绕过 PartitionGenerator，不查询文件列表
- `get_ts_col_order_by` 的返回值是路径 B 的关键触发条件

---

## 十二、代码优化建议

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

### 4. 索引下推决策清晰化
当前 `is_remove_filter`、`can_optimize`、`other_conditions.is_empty()` 三个决策点分散在代码中，可考虑：
- 将决策逻辑封装为独立的决策函数，提高可读性
- 添加枚举类型表示 Filter 的最终形态（保留/替换/移除）
- 统一索引下推的两个层次（基础下推/高级优化）的判断逻辑

### 5. 索引下推功能扩展
当前仅支持等值和全文检索，可扩展：
- 支持范围查询下推到 zonemap 索引
- 支持布隆过滤器下推
- 支持 `NOT IN` 下推（当前仅支持 `IN`）

### 6. `equal_items` 链路优化
当前 `equal_items` 在多个阶段传递和转换，可考虑：
- 统一数据格式，避免多次转换
- 提前在 SQL 解析阶段完成 `get_partition_value()` 转换
- 添加缓存机制避免重复的分区键值转换

### 7. `equal_items` 与 `time_range` 统一
当前两条路径独立，可考虑：
- 统一分区条件提取逻辑，从 SQL 中自动提取 `_timestamp` 范围
- 避免请求参数与 SQL 条件不一致导致的查询范围扩大

### 8. `get_ts_col_order_by` 代码简化
当前 enterprise 与非 enterprise 实现差异较大，可考虑：
- 统一接口，将差异封装在内部实现中
- 非 enterprise 版本也可以支持更复杂的时间列检测
- 减少 `#[cfg]` 分支对代码可读性的影响

### 9. `match_all` 限制提示
当前错误提示较笼统，可改进：
- 明确告知用户是 JOIN/CTE/子查询外层哪种场景
- 提供改写建议（如将 match_all 移入子查询内部）

### 10. 单分片路径文档化
当前 `skip_get_file_list` 的分支逻辑较隐蔽，建议：
- 添加更清晰的代码注释说明两种单分片路径的区别
- 考虑统一单分片返回逻辑，减少分支复杂度
- 补充 `equal_items` 在 `skip_get_file_list = true` 场景下的使用说明
