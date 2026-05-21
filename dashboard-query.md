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
  • 变量替换 (固定变量 + 自定义变量 + 动态过滤器)
  • 查询执行器选择 (SQL/PromQL)
  • 流式请求构建
  • 后端分布式执行计划
       ↓
[阶段三] 结果回填
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
4. 变量加载等待 (ifPanelVariablesCompletedLoading)
   ↓
5. 查询类型分派
   ├─ SQL → executeSQL()
   └─ PromQL → executePromQL()
```

### 3.2 变量替换系统 (usePanelVariableSubstitution)

`usePanelVariableSubstitution.ts:35-751` 实现了三层变量替换机制。

#### 3.2.1 变量分类

| 变量类型 | 示例 | 处理方式 |
|---------|------|---------|
| 固定变量 | `$__interval`, `$__range` | 基于时间范围和图表宽度自动计算 |
| 自定义变量 | `$service`, `${status:csv}` | 来自看板变量配置，支持多格式输出 |
| 动态过滤器 | Ad-hoc filters | 全局应用，自动追加到 WHERE 子句 |

#### 3.2.2 固定变量计算 (`replaceQueryValue`, L443-672)

```typescript
// 固定变量值计算
const __interval = (endTime - startTime) / chartWidth / 1000;
const __interval_ms = formatInterval(__interval) * 1000;
const __rate_interval = Math.max(interval + scrapeInterval, 4 * scrapeInterval);
const __range = endTime - startTime; // 支持 s/ms 单位
```

#### 3.2.3 自定义变量替换

支持多种占位符格式和输出格式：
- 标准格式：`$var`, `${var}`, `{{var}}`
- CSV 格式：`${var:csv}` → `value1,value2`
- 管道格式：`${var:pipe}` → `value1|value2`
- 单引号：`${var:singlequote}` → `'value1','value2'`
- 双引号：`${var:doublequote}` → `"value1","value2"`

SQL 与 PromQL 的默认行为不同：
- SQL: 多选值默认转为 `'v1','v2'` 格式
- PromQL: 多选值默认转为 `v1|v2` 格式（正则匹配）

#### 3.2.4 动态变量应用 (`applyDynamicVariables`, L674-725)

- **PromQL**: 通过 `addLabelToPromQlQuery()` 追加标签匹配器
- **SQL**: 通过 `addLabelsToSQlQuery()` 自动追加 WHERE 条件

#### 3.2.5 变量依赖分析

```typescript
// 检测面板查询依赖的变量
const regex = /(?:\$\{?\s*varName\s*(?::\s*\w+\s*)?\}?)|(?:\{\{\s*varName\s*(?::\s*\w+\s*)?\}\})/;
const isDependent = panelSchema.queries.some(q => regex.test(q.query));
```

**渐进式加载优化**：每个面板只等待自己依赖的变量加载完成，而非等待所有变量。

### 3.3 查询执行器 (Executors)

#### 3.3.1 SQL 执行器 (usePanelSQLExecutor)

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

#### 3.3.2 PromQL 执行器 (usePanelPromQLExecutor)

`usePanelPromQLExecutor.ts:20-301` 处理 PromQL 类型查询。

**核心特性**：
- **并行执行**：所有查询通过 `Promise.all` 同时发起
- **分块处理**：`createPromQLChunkProcessor()` 高效合并 metric 数据
- **限流保护**：`max_dashboard_series` 限制最大序列数 (默认 100)
- **Step 值**：优先使用查询级 step_value，其次面板级 step_value，最后默认为 0

### 3.4 流式请求构建

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
  searchType: "dashboards",
  meta: {
    dashboard_id, panel_id, tab_id,
    fallback_order_by_col,
    is_ui_histogram,
    timeShiftQueries  // 时间偏移查询元数据
  },
  clear_cache: boolean
};
```

### 3.5 后端查询编排 (Rust)

#### 3.5.1 查询入口 (`search::mod.rs:129-330`)

```rust
pub async fn search(
    trace_id: &str,
    org_id: &str,
    stream_type: StreamType,
    user_id: Option<String>,
    in_req: &search::Request,
) -> Result<search::Response, Error> {
    // 1. SQL 解析与元数据提取
    let meta = Sql::new_from_req(&request, &query).await?;
    
    // 2. 集群搜索协调
    let res = cluster::http::search(request, query, regions, clusters, true).await?;
    
    // 3. 结果后处理 (流式排序)
    if in_req.query.streaming_output && meta.order_by.is_empty() {
        res = streaming::order_search_results(res, None);
    }
}
```

#### 3.5.2 SQL 元数据解析 (`search::sql::mod.rs:99-150`)

`Sql::new()` 执行以下解析：
1. 提取 FROM 子句中的流名称
2. 获取各流的 Schema 信息
3. 解析 WHERE 条件中的等值匹配项 (用于索引优化)
4. 识别 GROUP BY/ORDER BY 字段
5. 检测直方图间隔 (`histogram_interval`)
6. 标记是否为复杂查询 (`is_complex_query`)

#### 3.5.3 分区执行计划 (`search::streaming::execution.rs:49-150`)

`do_partitioned_search()` 实现分布式执行：

```
1. 最大查询范围限制 (max_query_range)
   ↓
2. 分区发现 (get_partitions)
   ├─ 按时间切分的文件列表
   ├─ 检测 streaming_aggs 支持
   └─ 非时间排序字段检测
   ↓
3. 分区排序策略
   ├─ Dashboard/Histogram: 按时间降序 (最新数据优先)
   └─ UI 搜索: 保持原始分区顺序
   ↓
4. Top-K 堆排序 (非时间排序时)
   └─ 增量维护 (from + size) 个元素
   ↓
5. 分区并行执行
   └─ 按顺序发送分块响应
```

**分区排序优化** (`execution.rs:131-142`)：
```rust
if search_type == SearchEventType::Dashboards 
    || (req.query.size == -1 && search_type != SearchEventType::UI) {
    partitions.sort_by(|a, b| b[0].cmp(&a[0]));  // 时间降序
    partition_order_by = &OrderBy::Desc;
}
```

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

#### 4.1.2 分块方向检测 (`chunkingDirection.ts`)

根据第一个分区的 `time_offset` 判断数据流向：
- **LTR (Left-to-Right)**: 首分区起始时间 ≈ 用户起始时间 → 数据从旧到新
- **RTL (Right-to-Left)**: 首分区结束时间 ≈ 用户结束时间 → 数据从新到旧

```typescript
// 分块合并策略
const shouldPrepend = (isLTR, orderAsc) => {
  // LTR + ASC: 追加到尾部
  // LTR + DESC: 插入到头部
  // RTL + ASC: 插入到头部
  // RTL + DESC: 追加到尾部
};
```

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
      │  └─ 等待变量就绪
      │
      ├─ executeSQL() / executePromQL()
      │  ├─ replaceQueryValue() 变量替换
      │  ├─ applyDynamicVariables() 动态过滤器
      │  └─ fetchQueryDataWithHttpStream()
      │
      ├─ [HTTP/2 Stream] ←─────────────┐
      │                                 │
      ├─ handleSearchResponse()        │
      │  ├─ search_response_metadata   │ 后端分区执行
      │  ├─ search_response_hits       │
      │  └─ end                        │
      │                                 │
      └─ convertPanelData()            │
         └─ 图表渲染                   │
                                     │
search::mod.rs (Rust)                │
      │                                │
      ├─ Sql::new() 解析 SQL        │
      ├─ 分区发现与排序               │
      ├─ 并行执行分区查询             │
      └─ 流式发送分区结果 ────────────┘
```

### 5.2 变量变更触发流程

```
variablesData.value (Watcher)
      │
      ▼
variablesDataUpdated()
      ├─ Step 1: 检查动态变量加载状态
      ├─ Step 2: 检查依赖变量加载状态
      ├─ Step 3: 检查变量数量变化
      └─ Step 4: 比较变量值变化
          ├─ 多值变量: 排序后数组比较
          └─ 单值变量: 全等比较
              │
              └─ 有变化 → loadData()
```

---

## 六、核心模块代码索引

| 模块 | 文件路径 | 核心函数 |
|------|---------|---------|
| 数据加载编排 | `web/src/composables/dashboard/usePanelDataLoader.ts` | `loadData()`, `restoreFromCache()` |
| 变量替换 | `web/src/composables/dashboard/usePanelVariableSubstitution.ts` | `replaceQueryValue()`, `applyDynamicVariables()` |
| SQL 执行器 | `web/src/composables/dashboard/usePanelSQLExecutor.ts` | `executeSQL()`, `getDataThroughStreaming()` |
| PromQL 执行器 | `web/src/composables/dashboard/usePanelPromQLExecutor.ts` | `executePromQL()` |
| 搜索响应处理 | `web/src/composables/dashboard/usePanelSearchHandlers.ts` | `handleSearchResponse()`, `flushHitsBuffer()` |
| 自动查询构建 | `web/src/utils/dashboard/dashboardAutoQueryBuilder.ts` | `buildSQLChartQuery()`, `buildCondition()` |
| 面板配置管理 | `web/src/composables/dashboard/useDashboardPanel.ts` | `makeAutoSQLQuery()`, `updateQueryValue()` |
| 后端查询入口 | `src/service/search/mod.rs` | `search()` |
| SQL 解析 | `src/service/search/sql/mod.rs` | `Sql::new()` |
| 分区执行 | `src/service/search/streaming/execution.rs` | `do_partitioned_search()` |
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

### 7.2 可靠性设计

1. **AbortController**：组件卸载/重新查询时及时取消飞行请求
2. **Trace ID 追踪**：端到端链路追踪，便于问题排查
3. **部分数据标记**：`isPartialData` 标识不完整结果，用户可感知
4. **缓存降级**：查询失败时保留上次缓存数据，提升用户体验
5. **最大查询范围保护**：`max_query_range` 防止超大范围查询导致过载

### 7.3 架构设计

1. **组合式函数 (Composables)**：职责单一，依赖注入灵活组合
2. **策略模式**：SQL/PromQL 执行器实现统一接口，可无缝切换
3. **事件驱动**：流式响应通过事件类型分发，扩展性强
4. **分层架构**：前端编排关注交互体验，后端关注执行效率
