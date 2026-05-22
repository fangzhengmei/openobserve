# 可视化看板查询组件执行流程解析

本文档梳理 OpenObserve 可视化看板中查询组件从**面板配置**转译为**执行计划**，最终完成**结果回填**的完整协作流程。

---

## 一、整体架构概览

看板查询系统采用「前端编排 + 后端执行」的分层架构，核心协作流程分为三大阶段：

```
面板配置(Panel Schema)
       ↓
[阶段一] 面板模型解析
  • 字段配置 → SQL/PromQL 生成
  • 变量依赖分析
  • 自动查询构建
       ↓
[阶段二] 查询编排
  • 变量就绪四步判定
  • 变量替换 (固定变量 + 自定义变量 + 动态过滤器)
  • SQL AST 改写链 (后端)
  • 查询执行器选择 (SQL/PromQL)
  • 后端分区串行执行计划
  • 非时间排序 Top-K 全局合并
       ↓
[阶段三] 结果回填
  • 分块方向检测与 time_offset 修正
  • 分块响应处理 (Chunking)
  • 数据合并与排序
  • 图表数据转换
  • 渲染与缓存
```

---

## 二、阶段一：面板模型解析

### 2.1 面板数据模型 (Panel Schema)

面板配置的核心数据结构由 `useDashboardPanelDefaults.ts:16-100` 定义，包含：

| 层级 | 关键字段 | 说明 |
|------|---------|------|
| 面板层 | `type` | 图表类型 (line/bar/table/metric 等) |
| | `queryType` | 查询类型 (sql/promql) |
| | `config` | 图表配置 (坐标轴、图例、颜色等) |
| 查询层 | `queries[]` | 多查询支持，每个查询独立配置 |
| 查询字段 | `fields.stream` | 数据源 (流名称) |
| | `fields.x/y/z/breakdown` | 坐标轴字段配置 |
| | `fields.filter` | 过滤条件组 |
| | `customQuery` | 是否使用自定义查询模式 |

每个查询的字段配置支持以下结构 (`dashboardAutoQueryBuilder.ts:337-492`)：
```typescript
fields: {
  stream: "stream_name",
  stream_type: "logs",
  x: [{ name: "_timestamp", functionName: "histogram", alias: "time" }],
  y: [{ name: "count", functionName: "count", alias: "count" }],
  breakdown: [{ name: "service", alias: "service" }],
  filter: {
    filterType: "group",
    logicalOperator: "AND",
    conditions: [{ column: "status", operator: "=", value: "200" }]
  }
}
```

### 2.2 自动查询构建器 (Auto Query Builder)

当 `customQuery = false` 时，系统通过 `dashboardAutoQueryBuilder.ts:337-492` 的 `buildSQLChartQuery()` 自动生成 SQL。

**构建步骤**：

1. **字段表达式构建** (`buildFieldExpression`, L200-203)
   - 遍历所有坐标轴字段 (x/y/z/breakdown)
   - 调用 `buildSQLQueryFromInput()` 生成聚合函数表达式
   - 处理嵌套函数、字段别名、流别名

2. **JOIN 子句构建** (`buildSQLJoinsFromInput`, L130-192)
   - 支持多流 JOIN
   - 处理 `LEFT JOIN`/`INNER JOIN` 等类型
   - 构建连接条件

3. **WHERE 子句构建** (`buildWhereClause`, L780-802)
   - 递归处理过滤条件组 (`buildCondition`, L617-772)
   - 支持 `IN`、`LIKE`、`IS NULL`、正则匹配等多种运算符
   - 根据字段类型自动格式化值 (字符串加引号、数值保持原值)

4. **GROUP BY 子句构建** (L398-433)
   - 图表类型特定逻辑：
     - Heatmap: GROUP BY x_axis, y_axis
     - 带 Breakdown: GROUP BY x_axis, breakdown
     - 其他: GROUP BY x_axis
   - Table 类型仅 x 字段时跳过 GROUP BY

5. **HAVING/ORDER BY/LIMIT 子句** (L435-489)
   - 支持 y/z 轴字段的 HAVING 条件
   - 应用字段级别的排序配置
   - 应用查询限制

**代码映射**：
```typescript
// 入口：useDashboardPanel.ts:965-1003
const makeAutoSQLQuery = async () => {
  if (chartType === "geomap") {
    query = geoMapChart(dashboardPanelData);
  } else if (chartType === "sankey") {
    query = sankeyChartQuery(dashboardPanelData);
  } else if (chartType === "maps") {
    query = mapChart(dashboardPanelData);
  } else {
    query = buildSQLChartQuery({ queryData, chartType, dashboardPanelData });
  }
};
```

### 2.3 自定义查询模式 (Custom Query)

当 `customQuery = true` 时，用户直接编写 SQL/PromQL，系统执行：

1. **SQL 解析与字段提取** (`useDashboardPanel.ts:1088-1209`)
   - 使用 SQL Parser 解析 AST
   - 通过 `extractFields()` 提取 SELECT 子句字段
   - 自动识别 FROM 子句中的流名称

2. **字段匹配与同步** (`updateXYFieldsForCustomQueryMode`, L445-744)
   - 将解析出的字段同步到 x/y/z/breakdown 配置
   - 保持字段别名与 SELECT 别名一致
   - 切换自动/自定义模式时清理派生字段

3. **变量语法验证** (`validateQuery`, L1027-1068)
   - 递归替换变量占位符（支持字符串/数值两种形式）
   - 验证替换后的 SQL 语法正确性

---

## 三、阶段二：查询编排

### 3.1 数据加载编排器 (usePanelDataLoader)

`usePanelDataLoader.ts:47-854` 是整个查询流程的调度中心，负责协调所有子模块。

**加载触发条件** (Watcher 配置)：
- 时间范围变化 (`selectedTimeObj`)
- 强制刷新 (`forceLoad`)
- 面板配置变化（通过 `checkIfConfigChangeRequiredApiCallOrNot` 判断是否需要重新查询）
- 变量值变化 (`variablesData`)

**加载流程** (`loadData()`, L323-449)：

```
1. 防抖等待 (50ms)
   ↓
2. 缓存检查 (首次加载尝试读取缓存)
   ↓
3. 可见性等待 (IntersectionObserver)
   ↓
4. 变量就绪四步判定 (ifPanelVariablesCompletedLoading)
   ↓
5. 查询类型分派
   ├─ SQL → executeSQL()
   └─ PromQL → executePromQL()
```

### 3.2 变量就绪四步判定

`usePanelVariableSubstitution.ts:219-330` 实现了严格的变量就绪判定逻辑，确保查询仅在所有依赖准备完毕后才发起。

#### 3.2.1 核心判定函数 `ifPanelVariablesCompletedLoading()`

```typescript
const ifPanelVariablesCompletedLoading = () => {
  // ── Step 1 ───────────────────────────────────────
  // 检查动态变量 (dynamic_filters) 是否仍在加载
  log("Step1: checking if dynamic variables are loading...");
  const newDynamicVariablesData = getDynamicVariablesData();
  if (areDynamicVariablesStillLoading()) {
    log("Step1: dynamic variables still loading..., returning false");
    return false;
  }

  // ── Step 2 ───────────────────────────────────────
  // 检查普通依赖变量是否仍在加载
  log("Step2: checking if dependent variables are loading...");
  const newDependentVariablesData = getDependentVariablesData();
  if (areDependentVariablesStillLoadingWith(newDependentVariablesData)) {
    log("Step2: regular variables still loading..., returning false");
    return false;
  }

  return true;
};
```

#### 3.2.2 变量变更触发追加判定 (`variablesDataUpdated()`)

变量值变化时，额外执行两步深度比较：

```typescript
const variablesDataUpdated = () => {
  // Step 1 & 2: 同上 (加载状态检查)

  // ── Step 3 ───────────────────────────────────────
  // 检查变量数量变化（新增或删除）
  log("Step3: checking if variable count has changed...");
  if (newDependentVariablesData?.length !== currentDependentVariablesData?.length ||
      newDynamicVariablesData?.length !== currentDynamicVariablesData?.length) {
    return true;  // 数量变化 → 触发查询
  }

  // ── Step 4 ───────────────────────────────────────
  // 深度比较变量值
  log("Step4: checking if variable values have changed...");
  if (!isAllRegularVariablesValuesSameWith(newDependentVariablesData) ||
      !isAllDynamicVariablesValuesSameWith(newDynamicVariablesData)) {
    return true;  // 值变化 → 触发查询
  }

  return false;
};
```

#### 3.2.3 依赖变量加载判定细节

`areDependentVariablesStillLoadingWith()` (L139-168) 的核心逻辑：

| 条件 | hasNullValue | hasEmptyArray | isVariablePartialLoaded | 结果 | 说明 |
|------|-------------|---------------|-------------------------|------|------|
| 1 | true/false | true/false | **true** | ❌ 不阻塞 | 已加载过，即使值为空也有效 |
| 2 | **true** | - | **false** | ✅ 阻塞 | 从未加载且值为 null |
| 3 | - | **true** | **false** | ✅ 阻塞 | 从未加载且值为空数组 |
| 4 | false | false | false | ❌ 不阻塞 | 有值且未加载过（异常路径） |

> **关键设计**：仅当 `(null || 空数组) && 从未加载` 时才阻塞。如果变量曾加载过但现在返回空值（查询无结果），不阻塞查询，避免永久等待。

### 3.3 变量替换系统 (usePanelVariableSubstitution)

`usePanelVariableSubstitution.ts:35-751` 实现了三层变量替换机制。

#### 3.3.1 变量分类

| 变量类型 | 示例 | 处理方式 |
|---------|------|---------|
| 固定变量 | `$__interval`, `$__range` | 基于时间范围和图表宽度自动计算 |
| 自定义变量 | `$service`, `${status:csv}` | 来自看板变量配置，支持多格式输出 |
| 动态过滤器 | Ad-hoc filters | 全局应用，自动追加到 WHERE 子句 |

#### 3.3.2 固定变量计算 (`replaceQueryValue`, L443-672)

```typescript
// 固定变量值计算
const __interval = (endTime - startTime) / chartWidth / 1000;
const __interval_ms = formatInterval(__interval) * 1000;
const __rate_interval = Math.max(interval + scrapeInterval, 4 * scrapeInterval);
const __range = endTime - startTime; // 支持 s/ms 单位
```

#### 3.3.3 自定义变量替换

支持多种占位符格式和输出格式：
- 标准格式：`$var`, `${var}`, `{{var}}`
- CSV 格式：`${var:csv}` → `value1,value2`
- 管道格式：`${var:pipe}` → `value1|value2`
- 单引号：`${var:singlequote}` → `'value1','value2'`
- 双引号：`${var:doublequote}` → `"value1","value2"`

SQL 与 PromQL 的默认行为不同：
- SQL: 多选值默认转为 `'v1','v2'` 格式
- PromQL: 多选值默认转为 `v1|v2` 格式（正则匹配）

#### 3.3.4 动态变量应用 (`applyDynamicVariables`, L674-725)

- **PromQL**: 通过 `addLabelToPromQlQuery()` 追加标签匹配器
- **SQL**: 通过 `addLabelsToSQlQuery()` 自动追加 WHERE 条件

#### 3.3.5 变量依赖分析

```typescript
// 检测面板查询依赖的变量
const regex = /(?:\$\{?\s*varName\s*(?::\s*\w+\s*)?\}?)|(?:\{\{\s*varName\s*(?::\s*\w+\s*)?\}\})/;
const isDependent = panelSchema.queries.some(q => regex.test(q.query));
```

**渐进式加载优化**：每个面板只等待自己依赖的变量加载完成，而非等待所有变量。

### 3.4 查询执行器 (Executors)

#### 3.4.1 SQL 执行器 (usePanelSQLExecutor)

`usePanelSQLExecutor.ts:32-698` 处理 SQL 类型查询。

**核心流程** (`executeSQL()`, L211-695)：

```
1. 初始化状态 (清空 data/metadata/annotations)
   ↓
2. 遍历所有查询 (for...of panelSchema.queries)
   ├─ 2.1 变量替换 (replaceQueryValue)
   ├─ 2.2 动态变量应用 (applyDynamicVariables)
   ├─ 2.3 Timestamp 别名校验
   │
   ├─ [Time Shift 分支] 配置了时间偏移时
   │   ├─ 生成多个偏移查询 (0, -1h, -24h 等)
   │   ├─ 并行发起批量 HTTP/2 流请求
   │   └─ 按 query_index 分发响应
   │
   └─ [普通分支] 无时间偏移
       ├─ 缓存命中检查 (searchResponse)
       └─ 发起 HTTP/2 流请求
```

**时间偏移 (Time Shift) 处理** (L233-549)：
- 支持多时间范围对比查询
- 批量查询通过 `per_query_response: true` 标记
- 响应按 `query_index` 路由到对应数据槽位

#### 3.4.2 PromQL 执行器 (usePanelPromQLExecutor)

`usePanelPromQLExecutor.ts:20-301` 处理 PromQL 类型查询。

**核心特性**：
- **并行执行**：所有查询通过 `Promise.all` 同时发起
- **分块处理**：`createPromQLChunkProcessor()` 高效合并 metric 数据
- **限流保护**：`max_dashboard_series` 限制最大序列数 (默认 100)
- **Step 值**：优先使用查询级 step_value，其次面板级 step_value，最后默认为 0

### 3.5 后端查询编排 (Rust)

#### 3.5.1 SQL AST 改写链 (Visitor Pattern)

`search::sql::mod.rs:161-282` 实现了基于访问者模式的 SQL AST 改写流水线，共包含 5 个改写器按顺序执行：

```rust
// ********************Change the sql start*********************************//
// 2. rewrite track_total_hits
if query.track_total_hits {
    let mut trace_total_hits_visitor = TrackTotalHitsVisitor::new();
    let _ = statement.visit(&mut trace_total_hits_visitor);
}

// 3. rewrite all filter that include DASHBOARD_ALL with true
let mut remove_dashboard_all_visitor = RemoveDashboardAllVisitor::new();
let _ = statement.visit(&mut remove_dashboard_all_visitor);

// 4. rewrite match_all_raw and match_all_raw_ignore_case to match_all
let mut match_all_raw_visitor = MatchAllRawVisitor::new();
let _ = statement.visit(&mut match_all_raw_visitor);
// ********************Change the sql end*********************************//

// ... 中间步骤: 字段提取、Schema 分析 ...

// ********************Change the sql start*********************************//
// 11. add _timestamp and _o2_id if need
if !is_complex_query(&mut statement) {
    let mut add_timestamp_visitor = AddTimestampVisitor::new();
    let _ = statement.visit(&mut add_timestamp_visitor);
    if o2_id_is_needed(&used_schemas, &search_event_type) {
        let mut add_o2_id_visitor = AddO2IdVisitor::new();
        let _ = statement.visit(&mut add_o2_id_visitor);
    }
}
// ********************Change the sql end************************************//
```

**各改写器职责**：

| 改写器 | 文件 | 功能 |
|--------|------|------|
| `TrackTotalHitsVisitor` | `rewriter/track_total_hits.rs` | 将 SELECT DISTINCT 包装为子查询 + COUNT(*)，支持精确总数统计 |
| `RemoveDashboardAllVisitor` | `rewriter/remove_dashboard_placeholder.rs` | 将 `col = '_o2_all_'` 占位符替换为 `col = col OR true`，实现"全选"语义 |
| `MatchAllRawVisitor` | `rewriter/match_all_raw.rs` | 将 `match_all_raw()` / `match_all_raw_ignore_case()` 重写为标准 `match_all()` |
| `AddTimestampVisitor` | `rewriter/add_timestamp.rs` | 非复杂查询时自动追加 `_timestamp` 字段到 SELECT 列表 |
| `AddO2IdVisitor` | `rewriter/add_o2_id.rs` | 特定场景下自动追加 `_o2_id` 内部字段 |

> **设计特点**：每个改写器实现 `VisitorMut` trait，通过 `pre_visit_expr` / `pre_visit_query` 等钩子递归遍历并修改 AST，职责单一且可组合。

#### 3.5.2 分区执行计划 - **串行执行而非并行**

`search::streaming::execution.rs:154-357` 是核心执行逻辑，**分区查询实际为逐分区串行执行**，而非并行。

```rust
// execution.rs:154 —— 注意这是一个普通的 for 循环，按顺序逐个执行
for (idx, &[start_time, end_time]) in partitions.iter().enumerate() {
    let mut req = req.clone();
    req.query.start_time = start_time;
    req.query.end_time = end_time;

    // 调整 size/from 参数 (L159-171)
    if is_non_ts_order_by {
        // 非时间排序: 每个分区取 from+size 条，后续全局合并
        req.query.size = (original_from + original_size) as i64;
        req.query.from = 0;
    } else {
        // 时间排序: 逐分区累计跳过已获取的条数
        if req_size != -1 && !is_streaming_aggs {
            req.query.size -= curr_res_size;
        }
        req.query.size += *hits_to_skip;
        req.query.from = 0;
    }

    // ── 串行点 ───────────────────────────────
    // 每个分区调用 do_search().await 后才继续下一个
    let mut search_res = do_search(
        &trace_id, org_id, stream_type, &req,
        user_id, use_cache, is_multi_stream_search,
    ).await?;  // 在这里 await，阻塞直到当前分区完成
    // ─────────────────────────────────────────

    // 结果后处理 (跳过、裁剪、排序) ...
    // 发送分块响应 ...
    // 进度更新 ...

    // 提前终止条件 (已获取足够结果)
    if req_size != -1 && req_size != 0 && curr_res_size >= req_size && !is_streaming_aggs {
        log::info!("[HTTP2_STREAM] Reached requested result size, stopping search");
        break;  // 可以提前终止后续分区
    }
}
```

**串行执行的优势**：
1. **内存可控**：同一时间只有一个分区的结果在处理中
2. **提前终止**：时间排序场景下，当已累计足够结果即可 `break` 循环，不再处理后续分区
3. **顺序保证**：分区按排序策略依次处理，结果自然有序
4. **资源隔离**：避免多分区并行对存储层造成突发压力

#### 3.5.2.1 分区串行调度 vs 分区内执行并发的边界

必须明确区分两个层级的并发边界，避免混淆：

| 层级 | 执行模式 | 代码位置 | 说明 |
|------|---------|---------|------|
| **分区间调度** | **串行** | `execution.rs:154-421` | `for` 循环 + `do_search().await?`，一个分区完整执行完毕才开始下一个 |
| **分区内执行** | **可并行** | `do_search()` 内部 | 单个分区内的多文件/多批次处理可使用 rayon 并行 |

**分区内并行的具体体现**：

1. **文件组级并行** (`listing_adapter.rs:246`)：
```rust
// 单个分区内的多个文件组使用 rayon 并行处理
let new_file_groups: Vec<_> = file_groups
    .into_par_iter()        // rayon 并行迭代器
    .map(|file_group| { /* 处理单个文件组 */ })
    .collect();
```

2. **Record Batch 级并行** (`wal.rs:385`, `wal.rs:424`)：
```rust
// WAL 内存数据的多批次并行适配
let record_batches = merge_groupes
    .into_par_iter()        // rayon 并行迭代器
    .map(|mut group| { /* 合并排序单个批次组 */ })
    .collect();
```

3. **数据处理并行** (`enrichment_exec.rs:297`)：
```rust
// 富化表查询的数据块并行处理
let result = chunks
    .into_par_iter()        // rayon 并行迭代器
    .map(|chunk| { /* 转换单个数据块 */ })
    .collect();
```

**边界总结**：
- 串行点在 **调度层**（控制分区执行顺序和结果返回顺序）
- 并行点在 **数据层**（单个分区内的数据处理）
- 优势：既保证了结果的有序性和内存可控，又在数据处理层充分利用多核 CPU

#### 3.5.3 分区排序与发现

完整执行流程 (`do_partitioned_search()`, L49-357)：

```
1. 最大查询范围限制 (max_query_range)
   ↓
2. 分区发现 (get_partitions)
   ├─ 按时间切分的文件列表
   ├─ 检测 streaming_aggs 支持
   └─ 非时间排序字段检测 (is_non_ts_order_by)
   ↓
3. 分区排序策略
   ├─ Dashboard/Histogram: 按时间降序 (最新数据优先)
   └─ UI 搜索: 保持原始分区顺序
   ↓
4. [非时间排序分支] 初始化 Top-K 最小堆
   └─ 容量: original_from + original_size
   ↓
5. 分区串行执行 (for 循环 + .await)
   ├─ 时间排序: 逐分区流式返回 + 累计跳过
   └─ 非时间排序: 逐分区结果入堆 + 不立即返回
   ↓
6. [非时间排序分支] 全局 Top-K 合并
   └─ 堆排序 + 应用 from 分页
   ↓
7. 发送最终结果
```

**分区排序优化** (`execution.rs:131-142`)：
```rust
if search_type == SearchEventType::Dashboards 
    || (req.query.size == -1 && search_type != SearchEventType::UI) {
    partitions.sort_by(|a, b| b[0].cmp(&a[0]));  // 时间降序
    partition_order_by = &OrderBy::Desc;
}
```

#### 3.5.4 非时间排序 Top-K 全局合并

当 `ORDER BY` 不是时间字段（或包含非时间字段）时，执行两阶段 Top-K 合并策略：

**阶段一：分区内局部 Top-K** (execution.rs:159-163)
```rust
if is_non_ts_order_by {
    // 每个分区 fetch local top-(from+size); leader merges globally after all
    req.query.size = (original_from + original_size) as i64;
    req.query.from = 0;
}
```
每个分区独立计算自己的 Top-K（K = from + size），避免传送到下游的数据量过大。

**阶段二：分区间全局 Top-K** (execution.rs:239-246)
```rust
// Feed directly into the heap — never stores more than k hits in memory
if search_res.is_partial {
    non_ts_has_partial = true;
}
if let Some(ref mut heap) = topk_heap {
    heap.push_hits(std::mem::take(&mut search_res.hits));  // 消耗掉当前 hits
}
```
使用 `BinaryHeap`（最小堆）维护全局 Top-K，内存占用固定为 K。

**最终结果提取** (execution.rs:359-369)
```rust
// For non-ts ORDER BY: drain the heap (already bounded to k elements) into the final result
if is_non_ts_order_by {
    let merged = topk_heap
        .take()
        .map(|h| h.into_sorted_vec(original_from))  // 应用 from 分页
        .unwrap_or_default();

    let mut final_res = Response::default();
    final_res.hits = merged;
    final_res.total = final_res.hits.len();
    final_res.size = final_res.total as i64;
    // ... 发送最终结果
}
```

> **算法复杂度**：设分区数为 N，每个分区 M 条记录，取 Top-K。
> - 分区内：每个分区 O(M log K)，总计 O(N * M log K)
> - 全局合并：每次入堆 O(log K)，总计 O(N * K log K)
> - 总内存：O(K)，与数据总量无关

### 3.6 流式请求构建

**HTTP/2 流请求 payload 结构** (`usePanelSQLExecutor.ts:137-181`)：

```typescript
const payload = {
  queryReq: {
    query: { sql, start_time, end_time, size: -1, histogram_interval },
    regions, clusters
  },
  type: "histogram" | "promql",
  traceId: "uuid",
  org_id: "org_identifier",
  searchType: "dashboards" | "ui" | "insights",  // 见下方不同场景说明
  pageType: "logs",  // 来自 fields.stream_type
  meta: {
    dashboard_id, panel_id, tab_id,
    fallback_order_by_col,
    is_ui_histogram,    // Logs→Visualize 场景标记
    timeShiftQueries    // 时间偏移查询元数据
  },
  clear_cache: boolean
};
```

**不同场景下 searchType 的实际取值**（代码校正）：

| 场景 | 传递链路 | searchType 实际值 |
|------|---------|------------------|
| **标准看板页面** | `ViewDashboard.vue:550` → `route.query.searchtype` 或 `RenderDashboardCharts.vue` 透传 | `"dashboards"` |
| **Logs→Visualize** | `VisualizeLogsQuery.vue:23` (`pageType="logs"`) → `PanelEditor.vue:997` (`case "logs": return "ui"`) | **`"ui"`** |
| **Traces 分析** (logs 流) | `TracesAnalysisDashboard.vue:336` → `streamType === 'logs' ? 'insights' : 'dashboards'` | `"insights"` |
| **Traces 指标** | `TracesMetricsDashboard.vue:33` → 硬编码 | `"dashboards"` |

> 注意：`searchType` 不等于 `pageType`。`pageType` 描述页面来源，`searchType` 描述后端查询路由类型。

---

## 四、阶段三：结果回填

### 4.1 流式响应处理 (usePanelSearchHandlers)

`usePanelSearchHandlers.ts:30-406` 处理 HTTP/2 分块响应。

#### 4.1.1 响应事件类型

| 事件类型 | 处理函数 | 说明 |
|---------|---------|------|
| `search_response_metadata` | `handleStreamingHistogramMetadata` | 分区元数据，含 time_offset、scan_stats |
| `search_response_hits` | `handleStreamingHistogramHits` | 实际数据分块 |
| `search_response` | `handleHistogramResponse` | 完整单块响应 (兼容旧格式) |
| `event_progress` | 内联处理 | 进度百分比 |
| `end` | 内联处理 | 查询结束 |
| `error` | 内联处理 | 查询错误 |

#### 4.1.2 分块方向检测与 time_offset 修正

**基础检测逻辑** (`chunkingDirection.ts:27-40`)：
根据第一个分区的 `time_offset` 与用户请求时间范围的距离判断流向：

```typescript
export const detectChunkingDirection = (
  firstChunkStart: number,
  firstChunkEnd: number,
  userStart: number,
  userEnd: number,
): boolean | null => {
  return Math.abs(firstChunkStart - userStart) <= Math.abs(firstChunkEnd - userEnd);
  // true = LTR (左到右: 首块起始靠近用户起始)
  // false = RTL (右到左: 首块结束靠近用户结束)
};
```

**Logs→Visualize 场景下的修正逻辑**：

当从 Logs 页面切换到 Visualize 视图时，`searchType` 的实际取值传递链路为：

1. **searchType 实际取值链**（代码校正）：
   - `VisualizeLogsQuery.vue:23` 传入 `pageType="logs"` 给 `PanelEditor`
   - `PanelEditor.vue:991-1004` 的 switch 映射：`case "logs": return "ui"`
   - 最终 **`searchType = "ui"`**（而不是 "dashboards" 或 "logs"）
   - `is_ui_histogram = true` 通过 props 传入

2. **场景特征** (`usePanelSQLExecutor.ts:227`, `:164-178`)：
   - `pageType` 来自 `panelSchema.value.queries[0]?.fields?.stream_type`，通常为 `"logs"`
   - `searchType = "ui"`（由 PanelEditor 计算得到）
   - `is_ui_histogram = true`（Logs→Visualize 场景标记）
   - 后端会自动将原始 SQL 包装为直方图聚合（`search_stream.rs:337-342`）

3. **原始问题**：
   后端自动转换后的直方图查询，第一个返回的分块 `time_offset` 可能与用户实际请求的时间范围存在偏差（例如直方图区间对齐导致），此时直接使用 `time_offset` 判断方向可能出错。

4. **修正逻辑** (`usePanelSQLExecutor.ts:412-431`, `usePanelSearchHandlers.ts:138-162`)：
   - **第一优先级**：使用 `state.metadata.queries[queryIndex]` 中存储的**请求时的 startTime/endTime** 作为判断基准
   - **第二优先级**：如果 queryIndex 对应元数据不存在，降级使用 `queries[0]` 的时间范围
   - **time_offset 缺失处理**：如果 `time_offset` 字段为 0 或不存在，`detectChunkingDirection` 返回 `null`，此时**不设置方向标记**，由后续默认逻辑处理

```typescript
// usePanelSearchHandlers.ts:138-162 中的判断优先级
const direction = detectChunkingDirection(
  // time_offset 取值路径优先: results.time_offset → content.time_offset → 0
  searchRes?.content?.results?.time_offset?.start_time ??
    searchRes?.content?.time_offset?.start_time ?? 0,
  searchRes?.content?.results?.time_offset?.end_time ??
    searchRes?.content?.time_offset?.end_time ?? 0,
  // 用户请求时间优先: 当前 query → query 0 → 0
  state.metadata?.queries?.[queryIndex]?.startTime ??
    state.metadata?.queries?.[0]?.startTime ?? 0,
  state.metadata?.queries?.[queryIndex]?.endTime ??
    state.metadata?.queries?.[0]?.endTime ?? 0,
);
```

**分块合并策略** (`chunkingDirection.ts:54-59`，`usePanelSearchHandlers.ts:164-167`)：

```
 isLTR XOR orderAsc → shouldPrepend

 ┌───────────┬──────────┬─────────────┬─────────────┐
 │ chunk 方向 │ 块内排序 │ 合并位置     │ 结果顺序     │
 ├───────────┼──────────┼─────────────┼─────────────┤
 │ LTR (旧→新)│ ASC      │ append(尾部) │ 正确 旧→新  │
 │ LTR (旧→新)│ DESC     │ prepend(头)  │ 正确 新→旧  │
 │ RTL (新→旧)│ ASC      │ prepend(头)  │ 正确 旧→新  │
 │ RTL (新→旧)│ DESC     │ append(尾部) │ 正确 新→旧  │
 └───────────┴──────────┴─────────────┴─────────────┘

 布尔表达式: shouldPrepend = isLTR !== orderAsc
```

**time_offset 缺失时的行为校正**（原描述"固定追加"错误）：

当 `detectChunkingDirection` 返回 `null`（time_offset 为 0 或缺失），代码不会调用 `chunkingLeftToRight.set()`，此时：

```typescript
// usePanelSearchHandlers.ts:164 —— 注意默认值是 false (RTL)
const isLTR = chunkingLeftToRight.get(queryIndex) ?? false;
const orderAsc = searchRes?.content?.results?.order_by?.toLowerCase() === "asc";
const shouldPrepend = shouldPrependChunk(isLTR, orderAsc);
```

实际合并行为由 `orderAsc` 决定：

| time_offset 状态 | isLTR 默认值 | orderAsc | shouldPrepend | 合并行为 |
|-----------------|-------------|----------|---------------|---------|
| ✅ 正常返回 | true/false | true/false | 由 XOR 决定 | 正确方向 |
| ❌ 缺失/为 0 | **false (RTL)** | **true** | `false !== true` → **true** | **prepend (头插)** |
| ❌ 缺失/为 0 | **false (RTL)** | **false** | `false !== false` → **false** | **append (尾插)** |

> 关键结论：time_offset 缺失时**不是固定追加**，而是**默认假设 RTL 方向**，最终合并行为由每个分块的 `order_by` 排序决定。

#### 4.1.3 Hit 批处理优化

为避免频繁状态更新导致 UI 卡顿，使用微任务批处理：

```typescript
// hitsBuffer 按 queryIndex 缓存 hits
// queueMicrotask 确保同 macrotask 内的多次 hits 合并为一次状态更新
function scheduleFlush() {
  if (!flushScheduled) {
    flushScheduled = true;
    queueMicrotask(flushHitsBuffer);
  }
}
```

### 4.2 状态更新与合并

#### 4.2.1 多查询数据结构

```typescript
state.data = [
  [...],  // queryIndex 0 的数据
  [...],  // queryIndex 1 的数据 (time shift 或多查询)
  ...
];

state.resultMetaData = [
  [/* partition 0 meta */, /* partition 1 meta */, ...],  // query 0
  [...],  // query 1
];
```

#### 4.2.2 Streaming Aggregations 模式

当 `streaming_aggs = true` 时（聚合查询优化）：
- 每次响应替换全部数据（而非追加）
- 适用于 `GROUP BY` + 直方图聚合场景
- 显著减少内存占用和处理时间

### 4.3 数据转换管道 (Data Conversion)

#### 4.3.1 转换入口 (`convertPanelData.ts:34-150`)

根据面板类型分派到不同转换器：

```typescript
switch (panelSchema.type) {
  case "line" | "bar" | "area":
    return queryType === "promql" 
      ? convertPromQLData(...) 
      : convertMultiSQLData(...);
  case "table":
    return isPivot ? convertPivotTableData(...) : convertTableData(...);
  case "geomap":
    return convertGeoMapData(...);
  case "sankey":
    return convertSankeyData(...);
  // ... 其他类型
}
```

#### 4.3.2 Time Shift 多查询合并 (`convertSQLData.ts:27-103`)

对于时间偏移对比查询，合并多组数据序列：
- 保留基础查询的图例配置
- 为对比系列添加 `(1 hour ago)` 等后缀
- 重新应用颜色映射

### 4.4 缓存机制 (usePanelCache)

**缓存键构成** (`usePanelDataLoader.ts:115-127`)：
```typescript
const cacheKey = {
  panelSchema,        // 面板配置（忽略 version/layout/htmlContent）
  variablesData,      // 依赖的变量值（归一化后）
  forceLoad,
  dashboardId,
  folderId
};
```

**缓存恢复时机**：
- 面板首次加载时 (`runCount == 0`)
- 配置未变化且变量值未变
- 时间范围不匹配时标记 `isCachedDataDifferWithCurrentTimeRange`

---

## 五、关键协作时序图

### 5.1 完整查询执行时序

```
usePanelDataLoader
      │
      ├─ loadData()
      │  ├─ 防抖 50ms
      │  ├─ 缓存检查 → 命中则直接返回
      │  ├─ 等待面板可见 (IntersectionObserver)
      │  └─ 等待变量就绪 (四步判定)
      │
      ├─ executeSQL() / executePromQL()
      │  ├─ replaceQueryValue() 变量替换
      │  ├─ applyDynamicVariables() 动态过滤器
      │  └─ fetchQueryDataWithHttpStream()
      │
      ├─ [HTTP/2 Stream] ←─────────────────────────┐
      │                                              │
      ├─ handleSearchResponse()                     │
      │  ├─ search_response_metadata                │ 后端:
      │  │  └─ time_offset 方向检测 (含 Logs→Viz 修正)│
      │  ├─ search_response_hits                    │
      │  │  └─ 微任务 Hit 批处理                    │
      │  └─ end                                     │
      │                                              │
      └─ convertPanelData()                         │
         └─ 图表渲染                                │
                                                    │
search::mod.rs (Rust)                               │
      │                                             │
      ├─ Sql::new() 解析 SQL                        │
      │  └─ AST 改写链 (5个Visitor)                 │
      ├─ 分区发现与排序                             │
      ├─ 分区串行执行 (for + .await)                │
      │  ├─ 时间排序: 流式返回 + 累计跳过           │
      │  └─ 非时间排序: Top-K 堆入队                │
      ├─ [非时间排序] 全局 Top-K 合并               │
      └─ 流式发送分区结果 ───────────────────────────┘
```

### 5.2 变量变更触发流程

```
variablesData.value (Watcher)
      │
      ▼
variablesDataUpdated()
      ├─ Step 1: 检查动态变量加载状态
      ├─ Step 2: 检查依赖变量加载状态
      │   └─ (null || 空数组) && 从未加载 → 阻塞
      ├─ Step 3: 检查变量数量变化
      └─ Step 4: 比较变量值变化
          ├─ 多值变量: 排序后数组比较
          └─ 单值变量: 全等比较
              │
              └─ 有变化 → loadData()
```

### 5.3 非时间排序 Top-K 执行时序

```
后端 do_partitioned_search()
      │
      ├─ 检测 is_non_ts_order_by = true
      ├─ 初始化 BinaryHeap (容量 = from + size)
      │
      ├─ [for 循环串行处理每个分区]
      │   ├─ 分区查询: size = from + size, from = 0
      │   ├─ 分区内排序 + 局部 Top-K
      │   └─ heap.push_hits(partition_hits)  → O(log K)
      │
      ├─ 所有分区处理完毕
      │
      ├─ heap.into_sorted_vec(original_from)
      │   ├─ 堆排序 → O(K log K)
      │   └─ 跳过前 from 条 → 应用分页
      │
      └─ 发送最终合并结果
```

---

## 六、核心模块代码索引

| 模块 | 文件路径 | 核心函数 |
|------|---------|---------|
| 数据加载编排 | `web/src/composables/dashboard/usePanelDataLoader.ts` | `loadData()`, `restoreFromCache()`, `waitForTheVariablesToLoad()` |
| 变量替换与就绪判定 | `web/src/composables/dashboard/usePanelVariableSubstitution.ts` | `ifPanelVariablesCompletedLoading()`, `variablesDataUpdated()`, `replaceQueryValue()`, `applyDynamicVariables()` |
| SQL 执行器 | `web/src/composables/dashboard/usePanelSQLExecutor.ts` | `executeSQL()`, `getDataThroughStreaming()` |
| PromQL 执行器 | `web/src/composables/dashboard/usePanelPromQLExecutor.ts` | `executePromQL()` |
| 搜索响应处理 | `web/src/composables/dashboard/usePanelSearchHandlers.ts` | `handleSearchResponse()`, `flushHitsBuffer()`, `handleStreamingHistogramMetadata()` |
| 分块方向检测 | `web/src/utils/dashboard/chunkingDirection.ts` | `detectChunkingDirection()`, `shouldPrependChunk()` |
| 自动查询构建 | `web/src/utils/dashboard/dashboardAutoQueryBuilder.ts` | `buildSQLChartQuery()`, `buildCondition()` |
| 面板配置管理 | `web/src/composables/dashboard/useDashboardPanel.ts` | `makeAutoSQLQuery()`, `updateQueryValue()` |
| 后端查询入口 | `src/service/search/mod.rs` | `search()` |
| SQL 解析与 AST 改写 | `src/service/search/sql/mod.rs` | `Sql::new_with_options()` |
| 分区串行执行 | `src/service/search/streaming/execution.rs` | `do_partitioned_search()` |
| Top-K 堆排序 | `src/service/search/streaming/execution.rs` | `topk_heap.push_hits()`, `into_sorted_vec()` |
| AST 改写器 | `src/service/search/sql/rewriter/*.rs` | `TrackTotalHitsVisitor`, `RemoveDashboardAllVisitor`, `MatchAllRawVisitor`, `AddTimestampVisitor`, `AddO2IdVisitor` |
| 看板 CRUD | `src/service/dashboards/mod.rs` | `create_dashboard()`, `update_panel_in_dashboard()` |

---

## 七、设计亮点与优化策略

### 7.1 性能优化

1. **渐进式加载**：面板仅等待自身依赖的变量，而非全部变量
2. **可见性感知**：IntersectionObserver 确保仅可视面板执行查询
3. **HTTP/2 多路复用**：单连接并发多查询，减少握手开销
4. **分块响应**：分区结果边计算边返回，首屏时间显著降低
5. **Hit 批处理**：微任务合并多次状态更新，避免 UI 抖动
6. **Streaming Aggs**：聚合查询采用替换而非追加，减少内存拷贝
7. **串行分区 + 提前终止**：时间排序场景下，累计足够结果即停止后续分区
8. **Top-K 堆合并**：非时间排序场景内存固定为 O(K)，与数据总量无关
9. **调度与数据分层并行**：分区间串行保证有序，分区内 rayon 并行利用多核
10. **分块方向预判**：基于 time_offset 选择 prepend/append，避免全量排序

### 7.2 可靠性设计

1. **AbortController**：组件卸载/重新查询时及时取消飞行请求
2. **Trace ID 追踪**：端到端链路追踪（支持分区子 ID：`trace-id-0`, `trace-id-1`）
3. **部分数据标记**：`isPartialData` 标识不完整结果，用户可感知
4. **缓存降级**：查询失败时保留上次缓存数据，提升用户体验
5. **最大查询范围保护**：`max_query_range` 防止超大范围查询导致过载
6. **变量加载防死锁**：仅 `从未加载 + 值为空` 时才阻塞，避免空结果导致永久等待

### 7.3 架构设计

1. **组合式函数 (Composables)**：职责单一，依赖注入灵活组合
2. **策略模式**：SQL/PromQL 执行器实现统一接口，可无缝切换
3. **访问者模式 (AST Rewriter)**：5 个改写器职责单一，可独立测试和扩展
4. **事件驱动**：流式响应通过事件类型分发，扩展性强
5. **分层架构**：前端编排关注交互体验，后端关注执行效率
6. **两阶段 Top-K**：分区局部 + 全局堆合并，兼顾效率与正确性
7. **调度与数据分离**：分区间串行调度保证有序性，分区内数据并行提高吞吐量
8. **默认值容错设计**：time_offset 缺失时使用 RTL 默认值 + orderAsc 决定合并方向，避免阻塞

---

## 八、前后文档差异对照

### v1 → v2 → v3 三级版本演进

| 章节 | v1 初始版 | v2 补充版 | v3 校正版 (本次) | 关键变化说明 |
|------|----------|-----------|-----------------|-------------|
| **整体架构** | 三阶段概述 | 三阶段概述 + 关键细节标注 | 同 v2 | v2 已完成 |
| **变量替换** | 仅变量分类 | 补充 **3.2 变量就绪四步判定** | 同 v2 | v2 已完成 |
| **后端执行** | 「分区并行执行」❌ | 修正为 **分区串行执行** | 补充 **3.5.2.1 分区间串行 vs 分区内并行** 边界 | v2 修正串行，v3 补充并发边界 |
| **SQL 处理** | 仅元数据解析 | 补充 **3.5.1 SQL AST 改写链** | 同 v2 | v2 已完成 |
| **Top-K 排序** | 仅文字提及 | 补充完整章节 | 同 v2 | v2 已完成 |
| **searchType 取值** | 模糊描述为 `dashboards`/`logs` | 无修正 | 补充 **3.6 节不同场景取值表** + **4.1.2 节取值链** | Logs→Visualize 实际为 `"ui"` 而非 `"logs"` |
| **结果回填** | 基础分块方向 | 补充 Logs→Visualize 修正 | **time_offset 缺失行为校正** | 原"固定追加"错误，实际由 `orderAsc` 决定 |
| **分块合并策略** | 文字描述 | 补充 XOR 真值表 | 补充 **缺失时行为真值表** | 新增 `isLTR ?? false` 默认值分析 |
| **代码索引** | 10 个模块 | 14 个模块 | 同 v2 | v2 已完成 |
| **时序图** | 2 张 | 3 张 | 同 v2 | v2 已完成 |
| **设计亮点** | 6+5+4 | 8+6+6 | 同 v2 | v2 已完成 |
| **差异对照** | 无 | v1→v2 对照表 | 扩展为 v1→v2→v3 三级对照 | 本次新增 |

### 关键错误修正汇总（累计）

| 版本 | 原错误描述 | 修正后描述 | 影响程度 | 代码佐证 |
|------|-----------|-----------|---------|---------|
| v2 | "分区并行执行" | "分区间串行，分区内可并行" | ⚠️ 高 | `execution.rs:154` `for + .await` |
| v2 | Logs→Visualize `searchType="logs"` | Logs→Visualize `searchType="ui"` | 🟡 中 | `PanelEditor.vue:997` `case "logs": return "ui"` |
| v3 | time_offset 缺失时"固定追加" | 默认 `isLTR=false`，行为由 `orderAsc` 决定 | 🟡 中 | `usePanelSearchHandlers.ts:164` `?? false` |
| v2 | 未提及变量就绪判定 | 四步判定 + 阻塞条件真值表 | 🟡 中 | `usePanelVariableSubstitution.ts:219-330` |
| v2 | 未提及 SQL AST 改写过程 | 5 个改写器按顺序执行的完整流水线 | 🟡 中 | `search::sql::mod.rs:161-282` |
| v2 | 未提及非时间排序合并策略 | 两阶段 Top-K + 最小堆全局合并 | 🟡 中 | `execution.rs:159-508` |
| v3 | 未区分调度层与数据层并发 | 分区间串行（调度）/ 分区内并行（数据） | 🟢 低 | `listing_adapter.rs:246` `into_par_iter()` |
| v2 | 未提及 Logs→Visualize 特殊处理 | time_offset 判断优先级修正 | 🟡 中 | `usePanelSearchHandlers.ts:152-157` |

### v3 本次核心校正点

1. **searchType 实际取值链校正**：
   - `VisualizeLogsQuery.vue` → `pageType="logs"`
   - `PanelEditor.vue:997` → `case "logs": return "ui"`
   - 最终 `searchType = "ui"`

2. **time_offset 缺失行为校正**：
   - 原：默认按追加方式处理
   - 现：`isLTR = get() ?? false`（默认 RTL），`shouldPrepend = false !== orderAsc`

3. **并发边界澄清**：
   - 调度层：分区间串行（`for + .await`）
   - 数据层：分区内可并行（`rayon::into_par_iter`）
