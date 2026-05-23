# 采集管道与富化表查询协作方式分析

## 1. 架构概述

OpenObserve 的采集管道（Pipeline）与富化表（Enrichment Table）查询采用了分层协作的架构设计：

- **采集管道层**：负责数据的实时/定时采集、转换、过滤和路由
- **富化表层**：提供静态参考数据的存储、加载和查询能力
- **函数执行层**：仅通过 VRL 函数实现管道与富化表的数据关联（JS 函数被管道禁用）
- **查询优化层**：针对富化表 JOIN 查询进行智能重排与广播连接优化

## 2. 字段转换机制

### 2.1 转换函数类型

**管道仅支持 VRL 函数**，JS 函数在管道中被明确禁止。

**VRL 函数（管道中的默认选择）** - `src/service/ingestion/mod.rs:138-206`
- 编译时注入富化表注册表 `TableRegistry`
- 支持 `get_enrichment_table_record` 等 VRL 内置函数
- 高性能 AST 解释执行

**JS 函数的限制** - `src/service/pipeline/mod.rs:38-64`
- **管道层面：仅保存/更新阶段的门禁，执行路径仍保留完整能力**：
  - `validate_no_javascript_functions` 在 `save_pipeline`（L89）和 `update_pipeline`（L137）时被调用，作为创建/修改管道的前置检查
  - 但 `batch_execution.rs` 中保留了完整的 JS 执行路径：`register_functions`（L143）有 `if transform.is_js()` 分支，`process_batch`（L953, L1058）有 `CompiledFunctionRuntime::JS` 的完整处理逻辑
  - 即：如果绕过门禁（如直接修改数据库），JS 函数在管道执行时仍能正常工作，这是防御性检查而非能力移除
- **技术层面：JS 运行时没有注入富化表查询能力**：
  - `compile_js_function` 和 `apply_js_fn`（`src/common/utils/js.rs`）中完全没有 `TableRegistry` 或 enrichment 相关代码
  - QuickJS 运行时仅暴露 `inputJson`、`orgId`、`streamName` 三个全局变量，无法访问富化表
- **JS 函数在 `_meta` 组织的实际边界**（而非"仅用于 claim_parser"）：
  - 函数创建层面：`functions.rs:68-73, 139-144, 359-364` 中 `if trans_type == 1 && org_id != "_meta"` 检查——JS 函数的创建逻辑限制了仅 `_meta` 组织可以创建，这是代码层面的硬约束
  - 管道执行层面：`validate_no_javascript_functions` 对所有组织（包括 `_meta`）执行相同的禁用检查——在当前代码约束下，即使在 `_meta` 组织中，管道也无法使用 JS 函数
  - 已知使用场景：JWT SSO claim 解析是已发现的调用方（`handler/http/auth/jwt.rs:1097,1102`），该代码硬编码 `org_id = "_meta"` 调用 `compile_js_function` 和 `apply_js_fn`，绕过了管道直接使用 JS 运行时
  - 边界结论："仅用于 claim_parser" 是当前观察到的使用模式，技术上 `_meta` 组织的 JS 函数可能被其他绕过管道的调用方使用（只要 org_id 为 `_meta`），但管道层面对所有组织均执行相同的禁用检查

### 2.2 函数编译流程

`src/service/pipeline/batch_execution.rs:135-168`

```rust
// 1. 获取函数定义
let transform = get_transforms(&self.org, &func_params.name).await?;

// 2. 编译 VRL 函数并加载富化表
let vrl_runtime_config = compile_vrl_function(&transform.function, &self.org)?;
let registry = vrl_runtime_config.config.get_custom::<TableRegistry>().unwrap();
registry.finish_load();  // 完成富化表加载

// 3. 编译结果缓存
CompiledFunctionRuntime::VRL(Box::new(VRLResultResolver { ... }), is_result_array)
```

### 2.3 富化表注册表构建

`src/common/utils/functions.rs:45-89`

```rust
pub fn get_vrl_compiler_config(org_id: &str) -> VRLCompilerConfig {
    let registry = TableRegistry::default();
    let mut tables: HashMap<String, Box<dyn Table + Send + Sync>> = HashMap::new();

    // 加载用户自定义富化表
    for table in ENRICHMENT_TABLES.iter() {
        if table.org_id == org_id || table.org_id == DEFAULT_ORG {
            tables.insert(table.stream_name.to_owned(), Box::new(table.value().clone()));
        }
    }

    // 加载 GeoIP 富化表（城市、ASN、企业版）
    if let Some(v) = GEOIP_CITY_TABLE.read().as_ref() {
        tables.insert(GEO_IP_CITY_ENRICHMENT_TABLE.to_owned(), Box::new(v.clone()));
    }

    registry.load(tables);
    config.set_custom(registry);
}
```

### 2.4 字段转换执行

`src/service/pipeline/batch_execution.rs:903-1001`

```rust
// VRL 函数执行
record = match apply_vrl_fn(
    &mut vrl_runtime_state,
    vrl_resolver,
    record,
    &org_id,
    std::slice::from_ref(&stream_name),
) {
    (res, None) => res,           // 转换成功
    (res, Some(error)) => {       // 转换失败 - 错误旁路
        // 记录错误但继续处理
        error_sender.send((node_id, node_type, err_msg, Some(func_name))).await;
        res  // 返回原始记录
    }
};

// 结果数组模式（批量转换）
// - 收集一批记录后一次性转换
// - 适用于需要聚合上下文的场景
```

## 3. 关联表加载机制

### 3.1 富化表数据结构

`src/service/enrichment/mod.rs:34-38`

```rust
#[derive(Debug, Clone)]
pub struct StreamTable {
    pub org_id: String,
    pub stream_name: String,
    pub data: Arc<Vec<vrl::value::Value>>,  // 内存中的富化数据
}
```

### 3.2 富化表查询接口

`src/service/enrichment/mod.rs:51-95`

实现 `vector_enrichment::Table` trait，支持：

- **精确匹配查询** (`find_table_row`)
  - 大小写敏感/不敏感匹配
  - 多条件 AND 逻辑
  
- **范围查询** (`find_table_rows`)
  - 日期范围过滤 (`FromDate`, `ToDate`, `BetweenDates`)
  - 字段投影 (`select` 参数)

### 3.3 两级缓存加载策略

`src/service/enrichment/mod.rs:173-235`

```rust
pub async fn get_enrichment_table_inner(org_id: &str, table_name: &str, ...) -> Result<Values> {
    // 1. 检查元数据更新时间
    let db_stats = enrichment_table::get_meta_table_stats(org_id, table_name).await?;
    let local_last_updated = storage::local::get_last_updated_at(org_id, table_name).await?;

    // 2. 远程拉取（如果本地缓存过期或不存在）
    let values = if db_stats.end_time > local_last_updated || local_last_updated == 0 {
        // 通过 SQL 查询从数据库获取完整数据
        enrichment_table::get_enrichment_table_data(org_id, table_name, ...).await?
    } else {
        // 3. 本地缓存加载（Parquet 文件）
        storage::local::retrieve(org_id, table_name).await?
    };

    // 4. 异步更新本地缓存（如果需要）
    storage::local::store_data_if_needed_background(...).await?;
}
```

### 3.4 查询时富化表加载（EnrichmentExec）

`src/service/search/datafusion/distributed_plan/enrichment_exec.rs:201-342`

```rust
async fn fetch_data(...) -> Result<SendableRecordBatchStream> {
    // 第一优先级：磁盘 Parquet 文件（通常性能较高）
    let disk_result = read_from_disk(&org_id, &stream_name, &schema).await;
    if let Ok(batches) = disk_result {
        return Ok(Box::pin(MemoryStream::try_new(batches, schema, None)?));
    }

    // 第二优先级：内存缓存（ENRICHMENT_TABLES）
    let enrichment_data = match ENRICHMENT_TABLES.get(&key) {
        Some(stream_table) => stream_table.data.clone(),
        None => Arc::new(vec![]),
    };

    // 并行转换 VRL Value 到 RecordBatch（如果有可用数据）
    let batches: Result<Vec<_>, _> = pool.install(|| {
        chunks.into_par_iter()
            .map(|chunk| convert_vrl_to_record_batch(&schema, chunk))
            .collect()
    });
}
```

### 3.5 广播连接优化与重排机制

#### 3.5.1 重排机制：右表为富化表时的自动交换

`src/service/search/datafusion/optimizer/physical_optimizer/join_reorder.rs:64-81`

当富化表出现在 JOIN 的右侧时，`JoinReorderRule` 会自动执行左右表交换：

```rust
fn swap_join_order(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    #[cfg(feature = "enterprise")]
    if config::get_config().common.feature_enrichment_broadcast_join_enabled
        && !is_enrichment_table(left)
        && is_enrichment_table(right)
        && hash_join.join_type().supports_swap()
        && let Ok(swap_hash_join) = HashJoinExec::swap_inputs(hash_join, hash_join.mode)
        && should_use_enrichment_broadcast_join(&swap_hash_join)
    {
        return Ok(Transformed::yes(swap_hash_join));
    }
}
```

**重排触发条件（在当前代码中需要全部满足）：**
1. `feature_enrichment_broadcast_join_enabled` 配置为 `true`
2. 右表是富化表（schema 为 `enrichment_tables` 或 `enrich`）
3. 左表不是富化表
4. JOIN 类型支持交换（Inner、Left、Right、Full、LeftSemi、RightSemi、LeftAnti、RightAnti 等）
5. 交换后的执行计划满足广播连接条件

#### 3.5.2 广播连接改写的完整触发边界

`src/service/search/datafusion/optimizer/physical_optimizer/enrichment.rs:115-208`

```rust
pub fn should_use_enrichment_broadcast_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
    // 条件1：整个计划中只有一个 HashJoinExec，且没有其他多表算子
    //   - 禁止：多个 HashJoin、Union、Interleave、RecursiveQuery、其他 Join 类型
    //   - 计数规则：HashJoin=1，其他多表算子=2，总计数必须等于1
    
    // 条件2：左表必须是富化表
    //   - schema 为 enrichment_tables 或 enrich
    
    // 条件3：右表必须是简单流且仅包含安全算子
    //   - 右表本身不能是富化表
    //   - 仅允许以下算子：NewEmptyExec、FilterExec、CooperativeExec、
    //     RepartitionExec、CoalesceBatchesExec
    //   - 禁止：Aggregate、Sort、Limit 等复杂算子
}
```

**完整执行流程（基于代码注册顺序）：**
1. `JoinReorderRule`（独立优化器，L174 第一个注册）检查右表是否为富化表，若是则尝试交换左右表
2. `RemoteScanRule.optimize` 内（L156）执行 `is_place_holder_or_empty` 短路检查
3. `should_use_enrichment_broadcast_join` 验证广播连接条件
4. `enrichment_broadcast_join_rewrite` 执行改写：
   - 将左表 `NewEmptyExec` 替换为 `EnrichmentExec`（加载富化数据）
   - 调用 `remote_scan_to_top_if_needed` 为右表添加 `RemoteScanExec`
5. 转换为广播连接执行计划（小表广播到大表侧）

**在当前代码中无法触发广播连接的边界场景：**
- SQL 包含多个 JOIN 或 UNION 操作
- 右表使用了 Aggregate/Sort/Limit 等复杂算子
- JOIN 类型不支持交换（如 CrossJoin）
- 富化表 JOIN 富化表（左右都是富化表）
- 配置 `feature_enrichment_broadcast_join_enabled = false`

#### 3.5.3 物理优化器规则执行顺序的计划级证据链

**优化器规则注册顺序（可核实）** - `src/service/search/datafusion/optimizer/mod.rs:174-219`

```rust
pub fn generate_physical_optimizer_rules(...) -> Vec<Arc<dyn PhysicalOptimizerRule + ...>> {
    let mut rules = vec![Arc::new(JoinReorderRule::new()) as _];  // 顺序1: JoinReorderRule

    for context in contexts.into_iter() {
        match context {
            PhysicalOptimizerContext::RemoteScan(context) => {
                rules.push(generate_remote_scan_rules(req, sql, context));  // 顺序2: RemoteScanRule
            }
            PhysicalOptimizerContext::AggregateTopk => {
                rules.push(Arc::new(AggregateTopkRule::new(sql.limit)));  // 顺序3: AggregateTopkRule
            }
            PhysicalOptimizerContext::StreamingAggregation(context) => {
                rules.push(generate_streaming_agg_rules(_context));       // 顺序4: StreamingAgg
                rules.push(Arc::new(EliminateAggregateRule::new()) as _);
            }
        }
    }

    rules.push(Arc::new(LeaderIndexOptimizerRule::new(index_fields)) as _);  // 顺序5
    rules.push(Arc::new(LimitPushdown::new()) as _);                          // 顺序6
    rules
}
```

**RemoteScanRule 内部执行顺序（可核实）** - `src/service/search/datafusion/optimizer/physical_optimizer/remote_scan.rs:156-189`

```rust
fn optimize(&self, plan: Arc<dyn ExecutionPlan>, ...) -> Result<Arc<dyn ExecutionPlan>> {
    // 🔴 步骤1: 短路检查
    if is_place_holder_or_empty(&plan) {
        return Ok(plan);  // 直接返回，跳过所有后续优化
    }

    // 🔵 步骤2: 富化广播连接（企业版特性）
    #[cfg(feature = "enterprise")]
    if config.feature_enrichment_broadcast_join_enabled
        && should_use_enrichment_broadcast_join(&plan)
    {
        return enrichment_broadcast_join_rewrite(plan, ...);
    }

    // 🟢 步骤3: 通用广播连接（企业版特性）
    #[cfg(feature = "enterprise")]
    if config.feature_broadcast_join_enabled && should_use_broadcast_join(&plan) {
        return broadcast_join_rewrite(plan, ...);
    }

    // 🟡 步骤4: 单节点优化
    if self.single_node_optimizer_enable && is_single_node_optimize(&plan) {
        return remote_scan_to_top_if_needed(plan, ...);
    }

    // ⚫ 步骤5: 默认路径 - 添加 RemoteScanExec
    let mut rewrite = RemoteScanRewriter::new(...);
    let mut plan = plan.rewrite(&mut rewrite)?.data;
    Ok(plan)
}
```

**各优化阶段的命中/返回结果证据链：**

| 阶段 | 规则 | 输入 | 命中条件 | 输出 | 代码位置 |
|-----|------|------|---------|------|---------|
| 1 | JoinReorderRule | 原始物理计划 | 右表富化+左表非富化+JOIN可交换 | 交换后的计划 | `optimizer/mod.rs:174` |
| 2 | RemoteScanRule 短路 | Join重排后的计划 | 含 `PlaceholderRowExec`/`EmptyExec`/`DataSourceExec` | 原计划直接返回，跳过所有后续 | `remote_scan.rs:158` |
| 3 | enrichment_broadcast | 未短路的计划 | `should_use_enrichment_broadcast_join` 返回 true | 广播连接计划 | `remote_scan.rs:168` |
| 4 | broadcast_join | 未触发富化广播的计划 | `should_use_broadcast_join` 返回 true | 通用广播连接 | `remote_scan.rs:175` |
| 5 | single_node_optimize | 未触发广播的计划 | 单节点 + `is_single_node_optimize` | RemoteScan 置顶 | `remote_scan.rs:180` |
| 6 | RemoteScanRewriter | 其他情况 | 默认路径 | 注入 RemoteScanExec | `remote_scan.rs:184` |

#### 3.5.4 短路条件对 enrichment_broadcast 改写链路的影响

**短路条件定义** - `src/service/search/datafusion/optimizer/utils.rs:321-328`

```rust
pub fn is_place_holder_or_empty(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.exists(|plan| {
        Ok(plan.name() == "PlaceholderRowExec"
            || plan.name() == "EmptyExec"
            || plan.name() == "DataSourceExec")
    })
    .unwrap_or(true)
}
```

**代码推导结论（基于代码逻辑可确定）：**
- `NewEmptyExec`（OpenObserve 自定义的占位执行计划）不在短路条件中，它匹配的是 DataFusion 原生的 `EmptyExec`
- 富化表使用的正是 `NewEmptyExec`（schema 为 `enrichment_tables`/`enrich`），因此在代码逻辑层面不会被短路
- 短路检查发生在 `enrichment_broadcast_join_rewrite` 之前，一旦命中，不仅跳过富化广播，还会跳过通用广播连接和所有后续 RemoteScan 优化
- `enrichment_broadcast_join_rewrite` 内部（L67）也会调用 `remote_scan_to_top_if_needed`，用于为右表（普通日志流）添加 RemoteScanExec，确保分布式环境下右表数据能被正确拉取
- 只有当短路检查返回 `false` 且后续所有广播条件均不满足时，代码逻辑才会进入默认的 RemoteScanRewriter 路径

**执行计划实证结论（需实际运行查询验证）：**
- 如果查询计划中包含 DataFusion 原生的 `EmptyExec`（如 `SELECT 1` 这类无表查询），代码逻辑会在短路检查点直接返回原计划
- 如果查询同时包含富化表 JOIN 和原生 `EmptyExec`，整个计划会被短路，**enrichment_broadcast 改写在代码层面被完全跳过**，后续 JOIN 策略交由 DataFusion 默认处理（当前代码未明确指定为 Shuffle JOIN，实际行为取决于 DataFusion 版本和配置，需通过物理计划输出验证）

> **实证界限标注**：上述"执行计划实证结论"部分，关于 JOIN 策略的具体实现（如是否为 Shuffle JOIN）目前只能通过代码推导得出，缺乏实际的物理计划输出作为实证。如需验证，可通过设置 `common.print_key_sql = true` 配置，在日志中查看实际生成的物理计划。

---

#### 3.5.5 短路分支的查询级执行计划实证分析

##### 可验证的查询对比设计

以下设计可用于实证验证短路分支的行为，可通过 `print_key_sql = true` 输出物理计划进行验证。

**查询 A：触发短路的查询（含原生 EmptyExec）**
```sql
SELECT 1 AS dummy, l.* 
FROM logs l 
JOIN enrichment_tables.geoip g ON l.ip = g.ip
```
- **预期触发点**：`SELECT 1` 常量表达式可能被优化为原生 `EmptyExec` 或 `PlaceholderRowExec`
- **代码推导命中顺序**：
  1. JoinReorderRule: 检查右表是否为富化表 → 交换左右顺序（若满足条件）
  2. RemoteScanRule.optimize:
     - 🔴 `is_place_holder_or_empty` → 返回 `true`（命中 EmptyExec/PlaceholderRowExec）
     - 直接 `return Ok(plan)`，跳过所有后续优化
- **预期最终计划关键环节**（需实证验证）：
  - 不会出现 `EnrichmentExec`
  - 不会出现 `BroadcastHashJoinExec`
  - JOIN 实现取决于 DataFusion 默认策略

**查询 B：不触发短路的正常富化 JOIN**
```sql
SELECT l.*, g.country 
FROM logs l 
JOIN enrichment_tables.geoip g ON l.ip = g.ip
WHERE l._timestamp > now() - interval '1 hour'
```
- **预期触发点**：仅使用 `NewEmptyExec` 作为占位符，不会触发短路
- **代码推导命中顺序**：
  1. JoinReorderRule: 检查右表是否为富化表 → 交换左右顺序（若满足条件）
  2. RemoteScanRule.optimize:
     - 🔴 `is_place_holder_or_empty` → 返回 `false`（仅含 NewEmptyExec）
     - 🟢 `should_use_enrichment_broadcast_join` → 返回 `true`
     - `enrichment_broadcast_join_rewrite` 执行改写：
       - EnrichmentExecRewriter: NewEmptyExec → EnrichmentExec
       - remote_scan_to_top_if_needed: 为右表添加 RemoteScanExec
- **预期最终计划关键环节**（需实证验证）：
  - 出现 `EnrichmentExec` 作为 HashJoin 的左子节点
  - 出现 `BroadcastHashJoinExec`（或类似广播连接实现）
  - 右表可能包含 `RemoteScanExec` + `FilterExec`

##### 优化规则命中顺序的可核实证据链

| 规则名称 | 执行顺序 | 命中条件 | 代码位置 | 命中/跳过 | 输出特征 |
|---------|---------|---------|---------|----------|---------|
| **JoinReorderRule** | 第1位 | 右表富化+左表非富化+JOIN可交换 | `optimizer/mod.rs:174` | 仅影响右表富化场景 | plan.children() 顺序变化 |
| **RemoteScanRule** | 第2位 | 所有查询 | `optimizer/mod.rs:179` | 所有查询都经过 | 详见下表分支 |
| **AggregateTopkRule** | 第3位 | 企业版+含Limit | `optimizer/mod.rs:183` | 需满足企业版特性 | 出现TopK相关算子 |
| **StreamingAggregation** | 第4位 | 企业版+配置启用 | `optimizer/mod.rs:190` | 需满足企业版特性 | 出现StreamingAggregate |
| **LeaderIndexOptimizerRule** | 第5位 | 含索引字段查询 | `optimizer/mod.rs:217` | 涉及索引字段 | 出现IndexExec |
| **LimitPushdown** | 第6位 | 含Limit的查询 | `optimizer/mod.rs:219` | 涉及Limit | Limit算子下移 |

##### RemoteScanRule 内部分支的实证路径

```
RemoteScanRule.optimize(plan)
    │
    ├─ 🔴 is_place_holder_or_empty(plan)?
    │   ├─ true  → return Ok(plan)  
    │   │       → 【实证特征】计划中无 EnrichmentExec/RemoteScanExec
    │   │       → 【代码推导】跳过 enrichment_broadcast 和通用 broadcast
    │   │
    │   └─ false → 继续
    │
    ├─ 🟢 should_use_enrichment_broadcast_join(plan)?
    │   ├─ true  → return enrichment_broadcast_join_rewrite(plan)
    │   │       → 【实证特征】计划中出现 EnrichmentExec + BroadcastHashJoin
    │   │       → 【代码推导】NewEmptyExec 已被替换为 EnrichmentExec
    │   │
    │   └─ false → 继续
    │
    ├─ should_use_broadcast_join(plan)?
    │   ├─ true  → return broadcast_join_rewrite(plan)
    │   │       → 【实证特征】出现通用 BroadcastHashJoin
    │   │
    │   └─ false → 继续
    │
    ├─ single_node_optimize(plan)?
    │   ├─ true  → return remote_scan_to_top_if_needed(plan)
    │   │       → 【实证特征】RemoteScanExec 置顶
    │   │
    │   └─ false → 继续
    │
    └─ RemoteScanRewriter → 默认路径
            → 【实证特征】RemoteScanExec 注入到 RepartitionExec 之上
```

> **实证界限标注**：上述"实证特征"部分描述的计划节点名称（如 `BroadcastHashJoinExec`、`EnrichmentExec`）在代码中有明确定义（`enrichment_exec.rs`、`remote_scan.rs`），但具体的计划输出格式和命名可能受 DataFusion 版本影响，需通过实际运行查询并查看物理计划输出进行最终验证。目前这些结论基于代码逻辑推导，尚未包含实际运行的物理计划输出作为直接实证。

## 4. 错误旁路机制

### 4.1 管道执行错误处理架构

`src/service/pipeline/batch_execution.rs:331-503`

```rust
// 双通道设计：结果通道 + 错误通道
let (result_sender, mut result_receiver) = channel::<(usize, StreamParams, Value)>(batch_size);
let (error_sender, mut error_receiver) = channel::<(String, String, String, Option<String>)>(batch_size);

// 每个节点独立错误处理
// - 不因为单个节点失败导致整个管道失败
// - 错误记录包含：节点ID、节点类型、错误消息、函数名
```

### 4.2 节点级错误旁路

**Stream 节点错误** - `src/service/pipeline/batch_execution.rs:660-677`
- Flatten 失败：记录错误，跳过该记录，继续处理下一条
- 动态流名解析失败：记录警告，丢弃该记录

**Condition 节点错误** - `src/service/pipeline/batch_execution.rs:803-824`
- Flatten 失败：记录错误，跳过该记录
- 条件评估失败：继续下一条记录

**Function 节点错误** - `src/service/pipeline/batch_execution.rs:875-936`
- VRL 函数执行失败：记录错误，返回原始记录（不丢弃）
- 结果数组模式错误：记录错误，可能中止当前批次处理

**跨类型目标节点错误** - `src/service/pipeline/batch_execution.rs:735-773`
- 异步后台 ingestion 失败：仅记录日志，通常不影响主管道
- 使用 `tokio::spawn` 隔离执行上下文

### 4.3 错误收集与持久化

`src/service/pipeline/batch_execution.rs:428-503`

```rust
// 独立的错误收集任务
let error_task = tokio::spawn(async move {
    while let Some((node_id, node_type, error, fn_name)) = error_receiver.recv().await {
        pipeline_error.add_node_error(node_id, node_type, error, fn_name);
    }
    if count > 0 { Some(pipeline_error) } else { None }
});

// 错误发布到 self_reporting 流
if let Some(pipeline_errors) = error_task.await? {
    publish_error(ErrorData {
        _timestamp: Utc::now().timestamp_micros(),
        stream_params: source_stream_params,
        error_source: ErrorSource::Pipeline(pipeline_errors),
    }).await;
}
```

### 4.4 错误持久化存储

`src/service/db/pipeline_errors.rs:29-84`

```rust
pub async fn upsert(pipeline_id: &str, ..., error_data: &PipelineError) -> Result<()> {
    // 幂等性优化：仅在错误内容变化时更新
    if let Some(existing_model) = existing {
        let error_changed = existing_model.error_summary != error_data.error
            || existing_model.node_errors != node_errors_json;
        if error_changed {
            // 执行更新
        }
        // 错误未变化则跳过写入，降低 DB 负载
    }
}

// 定期清理：delete_older_than(cutoff_timestamp)
```

## 5. 统计指标体系

### 5.1 管道执行统计

`src/service/pipeline/batch_execution.rs:304-329`

```rust
// 源流摄入统计
let source_size: f64 = records.iter()
    .map(|record| record.to_string().len() as f64)
    .sum::<f64>() / config::SIZE_IN_MB;

if source_size > 0.0 {
    report_request_usage_stats(
        RequestStats {
            size: source_size,
            records: batch_size as i64,
            response_time: 0.0,
            ..Default::default()
        },
        org_id,
        &self.id,                    // pipeline ID
        source_stream_type,
        UsageType::Pipeline,
        0,                            // 函数数量
        timestamp,
    ).await;
}
```

### 5.2 远程目标统计

`src/service/pipeline/batch_execution.rs:1274-1295`

```rust
// WAL 写入成功后报告远程目标使用量
let data_size_mb = data_size as f64 / config::SIZE_IN_MB;
if data_size_mb > 0.0 {
    report_request_usage_stats(
        RequestStats {
            size: data_size_mb,
            records: records_len,
            ..Default::default()
        },
        &org_id,
        &remote_stream.destination_name,
        StreamType::Logs,
        UsageType::RemotePipeline,
        0,
        chrono::Utc::now().timestamp_micros(),
    ).await;
}
```

### 5.3 富化查询执行指标

`src/service/search/datafusion/distributed_plan/enrichment_exec.rs:103-142`

```rust
#[derive(Debug, Clone)]
pub struct EnrichmentMetrics {
    pub fetch_data_time: metrics::Time,        // 内存缓存读取时间
    pub read_disk_time: metrics::Time,         // 磁盘 Parquet 读取时间
    pub vrl_to_record_batch_time: metrics::Time, // VRL 到 RecordBatch 转换时间
    pub output_rows: metrics::Count,           // 输出行数
}

// 指标埋点示例
let timer = metrics.read_disk_time.timer();
let disk_result = read_from_disk(...).await;
timer.done();

metrics.record_output(total_rows);
```

### 5.4 函数执行错误指标

`src/service/ingestion/mod.rs:171-178`

```rust
metrics::INGEST_ERRORS
    .with_label_values(&[
        org_id,
        StreamType::Logs.as_str(),
        &format!("{stream_name:?}"),
        TRANSFORM_FAILED,  // "transform_failed"
    ])
    .inc();
```

### 5.5 缓存状态指标

`src/service/db/pipeline.rs:185-190`

```rust
pub async fn get_cache_stats() -> (usize, usize) {
    let realtime_count = STREAM_EXECUTABLE_PIPELINES.read().await.len();
    let scheduled_count = SCHEDULED_PIPELINES.read().await.len();
    (realtime_count, scheduled_count)
}
```

## 6. 数据流转时序

### 6.1 实时管道 + VRL 富化查询流程

```
数据摄入
    ↓
[Ingestion Service]
    ↓ 查找管道
[STREAM_EXECUTABLE_PIPELINES 缓存]
    ↓
[ExecutablePipeline::process_batch]
    ├─ 初始化节点任务（每个节点一个 tokio task）
    │   ├─ Source Node (Stream)
    │   │   └─ send_to_children → 发送到所有子节点
    │   ├─ Function Node (VRL)
    │   │   ├─ 检查是否需要 flatten
    │   │   ├─ apply_vrl_fn → 调用 VRL 运行时
    │   │   │   └─ VRL 程序中调用 get_enrichment_table_record
    │   │   │       └─ TableRegistry → StreamTable::find_table_row
    │   │   │           └─ 内存中条件匹配查询
    │   │   ├─ ✅ 成功：发送转换后记录到子节点
    │   │   └─ ❌ 失败：发送原始记录 + 错误到 error_channel
    │   ├─ Condition Node
    │   │   ├─ flatten（如需要）
    │   │   ├─ 条件评估
    │   │   ├─ ✅ 通过：发送到子节点
    │   │   └─ ❌ 不通过：丢弃
    │   └─ Leaf Node (Stream)
    │       ├─ flatten（如需要）
    │       ├─ 动态流名解析
    │       ├─ 同类型：result_sender → 返回给调用者
    │       └─ 跨类型：tokio::spawn → 后台 ingestion
    ├─ result_task 收集输出
    ├─ error_task 收集错误
    └─ publish_error → 错误自报告流
```

### 6.2 富化表 JOIN 查询流程

#### 代码推导的执行流程（基于代码注册顺序）

```
SQL 查询 (SELECT * FROM logs JOIN enrichment_tables.geoip ON ...)
    ↓
[SQL Parser] ← 代码确定
    ↓
[Logical Plan] ← 代码确定
    ↓
[Logical Optimizer] ← 代码确定
    ↓
[Physical Plan 创建] ← 代码确定
    ↓
[Physical Optimizer 规则链（按注册顺序执行）]
    ├─ 🔵 顺序1: JoinReorderRule.optimize ← 代码确定（optimizer/mod.rs:174）
    │   └─ 检查：右表富化+左表非富化+JOIN可交换？
    │       ├─ ✅ 是：swap_inputs → 富化表换到左侧
    │       └─ ❌ 否：保持原顺序
    ├─ 🟠 顺序2: RemoteScanRule.optimize ← 代码确定（optimizer/mod.rs:179）
    │   ├─ 🔴 is_place_holder_or_empty? ← 代码确定
    │   │   ├─ 检查：是否含 PlaceholderRowExec/EmptyExec/DataSourceExec?
    │   │   ├─ ✅ 是：直接返回原计划，跳过所有后续（含广播连接）
    │   │   └─ ❌ 否：继续
    │   ├─ should_use_enrichment_broadcast_join? ← 代码确定
    │   │   ├─ 检查：只有一个 HashJoin 且无其他多表算子？
    │   │   ├─ 检查：左表是 enrichment_tables/enrich schema？
    │   │   └─ 检查：右表仅含安全算子（Filter/Repartition 等）？
    │   └─ enrichment_broadcast_join_rewrite ← 代码确定
    │       ├─ EnrichmentExecRewriter: NewEmptyExec → EnrichmentExec
    │       └─ remote_scan_to_top_if_needed → 为右表添加 RemoteScanExec
    ├─ 顺序3: AggregateTopkRule（如启用）← 代码确定（optimizer/mod.rs:183）
    ├─ 顺序4: StreamingAggregation 规则（如启用）← 代码确定（optimizer/mod.rs:190）
    ├─ 顺序5: LeaderIndexOptimizerRule ← 代码确定（optimizer/mod.rs:217）
    └─ 顺序6: LimitPushdown ← 代码确定（optimizer/mod.rs:219）
    ↓
[EnrichmentExec::execute] ← 代码确定
    ├─ 尝试磁盘 Parquet 读取
    ├─ 失败则回退到内存缓存
    ├─ VRL Value → RecordBatch 并行转换
    └─ 输出到 HashJoinExec
    ↓
[Broadcast HashJoin]（若改写成功）← 需实证验证
    ↓
查询结果
```

#### 实证界限标注

> **区分说明**：上述流程图中，标注"← 代码确定"的环节基于代码注册顺序和逻辑分支推导得出，证据强度较高；标注"← 需实证验证"的环节（如最终生成的具体 JOIN 算子名称、物理计划输出格式）可能受 DataFusion 版本和运行时配置影响，需通过实际运行查询并设置 `common.print_key_sql = true` 查看物理计划输出进行最终验证。

> **短路分支的代码推导结论**：若 `is_place_holder_or_empty` 命中短路，代码逻辑层面会直接返回原计划，`enrichment_broadcast` 改写在代码层面被完全跳过，后续 JOIN 策略交由 DataFusion 默认处理，当前代码未明确指定其具体实现方式。此结论基于代码逻辑推导，关于 DataFusion 默认策略的具体实现需进一步实证验证。

## 7. 关键设计决策

### 7.1 富化表内存驻留设计
- **决策**：富化表数据在当前实现中倾向于全量加载到内存
- **理由**：富化表通常是小表（参考数据），内存查询性能远高于磁盘
- **权衡**：占用额外内存，可能不适用于超大型富化表场景

### 7.2 错误旁路而非中断
- **决策**：单条记录转换失败时不中断管道，倾向于返回原始记录继续处理
- **理由**：数据可用性优先，避免单个坏数据导致整批失败
- **权衡**：可能输出未经转换的"脏"数据，需要依赖错误监控进行后续处理

### 7.3 两级缓存策略
- **决策**：采用内存 + 本地磁盘 Parquet 双缓存设计
- **理由**：
  - 内存：提供较低延迟的查询能力
  - 磁盘：支持进程重启后快速恢复，避免全量远程拉取
- **权衡**：磁盘占用额外存储空间

### 7.4 智能重排 + 广播连接优化
- **决策**：通过 JoinReorderRule 尝试自动将右表富化表交换到左侧，在满足条件时转换为广播连接
- **理由**：
  - 富化表通常是小表，广播可以避免数据 shuffle
  - 用户无需关心 JOIN 顺序，优化器尝试自动处理
  - 仅在右表算子足够简单时触发，避免性能退化
- **权衡**：
  - 当前仅适用于单 JOIN 查询，多 JOIN/UNION 场景不进行该优化
  - 企业版特性，需 `feature_enrichment_broadcast_join_enabled = true`
  - 仅当右表为简单流（无 Aggregate/Sort 等复杂算子）时可能触发重排

## 8. 核心文件索引

| 模块 | 文件路径 | 主要职责 |
|------|---------|---------|
| 管道核心 | `src/service/pipeline/batch_execution.rs` | 管道批量执行、节点处理、错误旁路 |
| 管道管理 | `src/service/pipeline/mod.rs` | 管道 CRUD、JS 函数禁用验证、版本控制 |
| 富化表实现 | `src/service/enrichment/mod.rs` | StreamTable 实现、两级缓存加载 |
| 富化执行计划 | `src/service/search/datafusion/distributed_plan/enrichment_exec.rs` | 查询时富化数据加载、指标统计 |
| 富化表提供者 | `src/service/search/datafusion/table_provider/enrich_table.rs` | DataFusion TableProvider 实现 |
| 广播连接优化 | `src/service/search/datafusion/optimizer/physical_optimizer/enrichment.rs` | 富化 JOIN 条件验证、广播连接重写 |
| JOIN 重排优化 | `src/service/search/datafusion/optimizer/physical_optimizer/join_reorder.rs` | 右表富化表自动交换左右顺序 |
| RemoteScan 优化 | `src/service/search/datafusion/optimizer/physical_optimizer/remote_scan.rs` | 短路检查、分布式执行计划生成 |
| 优化器工具 | `src/service/search/datafusion/optimizer/utils.rs` | is_place_holder_or_empty 等公共工具 |
| 函数编译 | `src/common/utils/functions.rs` | VRL 编译器配置、富化表注册表 |
| JS 运行时 | `src/common/utils/js.rs` | QuickJS 运行时（无富化表访问能力，管道禁用） |
| 函数执行 | `src/service/ingestion/mod.rs` | VRL/JS 函数编译与执行（管道仅用 VRL） |
| JWT 认证 | `src/handler/http/auth/jwt.rs` | SSO claim 解析（已知 JS 函数调用方） |
| 错误持久化 | `src/service/db/pipeline_errors.rs` | 管道错误存储与查询 |
| 管道缓存 | `src/service/db/pipeline.rs` | ExecutablePipeline 缓存管理 |
| 全局配置 | `src/common/infra/config.rs` | ENRICHMENT_TABLES 等全局缓存 |
