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
    pub equal_items: HashMap<...>,      // 分区键等值条件
    pub columns: HashMap<...>,          // 涉及的列
    pub time_range: Option<(i64, i64)>, // 时间范围
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

## 三、时间字段处理机制

时间字段 `_timestamp` 是 OpenObserve 的核心分区字段，在查询规划中享有特殊待遇。

### 3.1 自动注入机制

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

### 3.2 时间范围过滤与分区剪枝

#### A. 时间范围提取

通过 `PartitionColumnVisitor`（`src/service/search/sql/visitor/partition_column.rs`）提取 WHERE 子句中的分区键等值条件，用于文件列表过滤。

#### B. 时间过滤器特殊处理

在 `src/service/search/datafusion/optimizer/physical_optimizer/utils.rs:125-151` 中定义了时间过滤器识别逻辑：

```rust
pub fn is_only_timestamp_filter(expr: &[&Arc<dyn PhysicalExpr>]) -> bool {
    expr.iter().all(|expr| is_timestamp_filter(expr))
}

fn is_timestamp_filter(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if let Some(expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
        match expr.op() {
            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
                // 检查操作数是否为 _timestamp 列与常量值的比较
                let column = if is_value(expr.left()) && is_column(expr.right()) {
                    get_column_name(expr.right())
                } else if is_value(expr.right()) && is_column(expr.left()) {
                    get_column_name(expr.left())
                } else { return false; };
                column == TIMESTAMP_COL_NAME
            }
            _ => false,
        }
    } else { false }
}
```

#### C. 排序优化

当查询仅按 `_timestamp` 降序排序时（`sorted_by_time` 标记），启用特殊优化：
- `split_file_groups_by_statistics = true`：按文件统计信息分割文件组
- `with_file_sort_order`：指定 Parquet 文件按 `_timestamp` 降序排序
- 避免全局排序，利用文件的已有排序性

### 3.3 直方图时间处理

`HistogramIntervalVisitor`（`src/service/search/sql/visitor/histogram_interval.rs`）负责：
1. 解析 `histogram(_timestamp, '1 hour')` 语法
2. 验证并调整时间间隔（避免过小的间隔导致性能问题）
3. 在逻辑优化阶段由 `RewriteHistogram` 重写为具体的时间分桶表达式

---

## 四、分片查询策略

### 4.1 分片生成逻辑

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

### 4.2 分片策略分类

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

### 4.3 文件到节点的分配

在 `src/service/search/datafusion/distributed_plan/node.rs` 中实现了文件分片到集群节点的分配：
- 基于一致性哈希或轮询策略
- 考虑节点的角色（querier / ingester）
- 支持超级集群跨区域调度

---

## 五、下推优化机制

下推优化是 OpenObserve 性能的关键，分为多个层次：

### 5.1 索引下推（Index Pushdown）

**核心优化器**：`IndexRule`（`src/service/search/datafusion/optimizer/physical_optimizer/index.rs`）

#### 工作原理：

```
原始 FilterExec
    ├── 谓词: name = 'openobserve' AND _timestamp > 1715395200000
    └── 输入: TableScan

优化后：
    ├── FilterExec（仅保留 _timestamp 过滤，可完全移除）
    └── 索引条件: IndexCondition { Equal("name", "openobserve") }
```

#### 可下推的表达式类型（`is_expr_valid_for_index`）：

| 表达式类型 | 支持情况 | 示例 |
|-----------|---------|------|
| 列 = 常量 | ✅ | `name = 'test'` |
| 列 != 常量 | ✅ | `name != 'test'` |
| 列 IN (常量列表) | ✅ | `status IN (200, 201)` |
| match_all('text') | ✅ | `match_all('error')` |
| str_match(col, 'text') | ✅ | `str_match(log, 'error')` |
| AND / OR 组合 | ✅ | `a=1 AND b=2` |
| NOT 取反 | ✅ | `NOT (name = 'test')` |
| 范围比较 (>, <) | ❌ | `age > 18` |
| 函数调用（除上述） | ❌ | `lower(name) = 'test'` |

#### 索引优化模式

`LeaderIndexOptimizerRule` 和 `FollowerIndexOptimizerRule` 配合实现两级优化：

| 模式 | 适用场景 | 优化效果 |
|------|---------|---------|
| `SimpleCount` | `SELECT count(*) FROM t` | 直接从索引计数，无需扫描数据 |
| `SimpleSelect` | `SELECT * FROM t ORDER BY _timestamp DESC LIMIT N` | 按时间倒序取 TopN，利用索引排序性 |
| `SimpleTopN` | `SELECT name, count(*) GROUP BY name ORDER BY cnt DESC LIMIT N` | 下推 TopN 到各节点，减少数据传输 |
| `SimpleDistinct` | `SELECT DISTINCT name FROM t LIMIT N` | 下推去重逻辑，减少数据传输 |
| `SimpleHistogram` | `SELECT histogram(_timestamp, '1h'), count(*) GROUP BY 1` | 下推直方图计算 |

### 5.2 过滤下推（Filter Pushdown）

使用 DataFusion 内置的 `PushDownFilter` 规则，将 WHERE 条件尽可能下推到数据源层。

配置开关（`src/service/search/datafusion/exec.rs:92-93`）：
```rust
config.options_mut().execution.parquet.pushdown_filters =
    cfg.common.feature_pushdown_filter_enabled;
```

### 5.3 限制下推（Limit Pushdown）

1. **逻辑层**：`PushDownLimit` 规则将 LIMIT 下推到子查询
2. **物理层**：`LimitPushdown` 规则将 LIMIT 下推到各分片执行
3. **特殊处理**：`AddSortAndLimitRule` 为无 LIMIT 的查询添加默认限制

### 5.4 下推执行流程

```
SQL → 逻辑计划 → PushDownFilter → PushDownLimit → 物理计划
                                              ↓
                                  IndexRule（提取索引条件）
                                              ↓
                                  LeaderIndexOptimizer（全局优化）
                                              ↓
                                  RemoteScanRule（分布式下推）
```

---

## 六、分布式执行与回退机制

### 6.1 远程扫描执行（RemoteScanExec）

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

#### 执行流程：

```rust
fn execute(&self, partition: usize, context: Arc<TaskContext>)
    -> Result<SendableRecordBatchStream> {
    // 1. 序列化子计划为字节
    let proto = get_physical_extension_codec();
    let physical_plan_bytes =
        physical_plan_to_bytes_with_extension_codec(input.clone(), &proto)?;
    
    // 2. 通过 Flight gRPC 发送到远程节点
    let (mut client, request) = make_flight_client(...).await?;
    let stream = client.do_get(request).await?.into_inner();
    
    // 3. 解码远程结果流
    let mut stream = FlightDecoderStream::new(stream, schema, metrics, query_context);
    
    // 4. 超时控制与回退
    let stream = async_stream::stream! {
        loop {
            tokio::select! {
                batch = stream.next() => { /* 正常处理 */ }
                _ = &mut timeout => {
                    process_partial_err(partial_err, e);
                    break;  // 超时不失败，返回部分结果
                }
            }
        }
    };
}
```

### 6.2 分布式计划重写

`RemoteScanRule`（`src/service/search/datafusion/optimizer/physical_optimizer/remote_scan.rs`）负责将单机物理计划转换为分布式计划：

#### 重写规则：

| 匹配算子 | 重写方式 |
|---------|---------|
| `RepartitionExec` | 子计划前插入 RemoteScanExec |
| `SortPreservingMergeExec` | 子计划前插入 RemoteScanExec |
| `UnionExec` | 每个子分支独立插入 RemoteScanExec |
| `HashJoinExec` | 左右子树独立插入 RemoteScanExec |
| 单节点优化 | 整个计划作为 RemoteScanExec 输入 |

#### 代码示例（UnionExec 处理）：
```rust
} else if node.name() == "UnionExec" {
    let mut visitor = TableNameVisitor::new();
    node.visit(&mut visitor)?;
    if !visitor.has_remote_scan {
        let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
        for child in node.children() {
            // SortExec 需要先加 SortPreservingMergeExec
            if child.name() == "SortExec" {
                let sort = child.as_any().downcast_ref::<SortExec>().unwrap();
                let sort_merge = Arc::new(SortPreservingMergeExec::new(...));
                let remote_scan = Arc::new(RemoteScanExec::new(sort_merge, ...)?);
                new_children.push(remote_scan);
            } else {
                let remote_scan = Arc::new(RemoteScanExec::new(child.clone(), ...)?);
                new_children.push(remote_scan);
            }
        }
        let new_node = node.with_new_children(new_children)?;
        return Ok(Transformed::yes(new_node));
    }
}
```

### 6.3 回退执行（Partial Failure Handling）

OpenObserve 采用**部分失败不中断整体查询**的优雅降级策略。

#### 回退触发场景：

1. **节点连接失败**：`make_flight_client` 返回错误
2. **RPC 调用失败**：`client.do_get` 返回错误
3. **查询超时**：`tokio::time::sleep` 触发
4. **Parquet 文件缺失**：`is_parquet_file_not_found` 检测

#### 错误处理逻辑（`src/service/search/datafusion/distributed_plan/common.rs`）：

```rust
pub fn process_partial_err(partial_err: Arc<Mutex<String>>, e: tonic::Status) {
    let mut guard = partial_err.lock();
    if guard.is_empty() {
        guard.push_str(e.to_string().as_str());
    } else {
        guard.push_str(format!(" \n {e}").as_str());
    }
}

pub fn get_empty_stream(empty_stream: EmptyStream) -> SendableRecordBatchStream {
    let EmptyStream { trace_id, schema, grpc_addr, partial_err, e, ... } = empty_stream;
    if let Some(e) = e && e.code() != tonic::Code::Ok {
        log::error!("[trace_id {trace_id}] flight->search error: {e:?}");
        process_partial_err(partial_err, e);
    }
    // 返回空流而非错误
    let stream = futures::stream::empty::<Result<RecordBatch>>();
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}
```

#### 可回退的错误类型：

| 错误码 | 处理方式 | 说明 |
|-------|---------|------|
| `Cancelled` | 回退 | 用户取消操作 |
| `DeadlineExceeded` | 回退 | 查询超时 |
| `Internal` + Parquet 文件缺失 | 回退 | 文件已被删除或合并 |
| 其他错误 | 失败 | 如权限错误、SQL 语法错误等 |

#### 结果标记：

查询结果中的 `is_partial` 字段标记是否存在部分失败，`partial_err` 字段记录具体错误信息。

### 6.4 执行流程图

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
物理优化 → 索引条件提取、RemoteScan 插入
    ↓
RemoteScanExec.execute() → 分发到各节点
    ├─ 节点1 → 本地执行 → 返回结果
    ├─ 节点2 → 连接失败 → 返回空流 + partial_err
    └─ 节点3 → 正常执行 → 返回结果
    ↓
结果合并 → 排序、聚合、分页
    ↓
返回给用户 → 标记 is_partial=true（如有部分失败）
```

---

## 七、复杂查询判定

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

## 八、关键设计决策总结

### 1. 时间优先策略
- `_timestamp` 是一等公民，自动注入、自动排序、自动分片
- 利用时间局部性，迷你分片快速返回首批结果

### 2. 索引激进下推
- 尽可能将过滤条件转化为索引查询
- 成功提取索引条件后可移除原 Filter 算子
- 支持全文检索、精确匹配、IN 列表等多种下推形式

### 3. 优雅降级机制
- 部分节点失败不导致整体查询失败
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

---

## 九、代码优化建议

### 1. VRL UDF 性能优化
当前实现是逐行解释执行 VRL，可考虑：
- 添加向量化执行支持
- 缓存编译后的 VRL 程序
- 对于简单函数提供原生 Rust 实现路径

### 2. 回退机制增强
当前仅记录错误但不重试，可考虑：
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
