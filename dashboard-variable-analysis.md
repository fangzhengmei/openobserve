# OpenObserve 仪表盘变量、模板与面板查询传导链路分析

## 1. 变量系统架构概览

OpenObserve 的仪表盘变量系统采用**三层作用域架构**，配合**两级状态管理**和**依赖图解析**实现变量在面板查询中的传导与应用。

### 核心文件定位
| 模块 | 文件路径 | 主要职责 |
|------|---------|---------|
| 变量工具 | `web/src/utils/dashboard/variables/variablesUtils.ts` | 变量值解析、语法标准化、作用域优先级处理 |
| 作用域工具 | `web/src/utils/dashboard/variables/variablesScopeUtils.ts` | 变量作用域类型判断 |
| 依赖图工具 | `web/src/utils/dashboard/variables/variablesDependencyUtils.ts` | 变量依赖关系构建、循环检测 |
| 变量管理器 | `web/src/composables/dashboard/useVariablesManager.ts` | 变量状态管理、生命周期、提交机制 |
| 变量替换 | `web/src/composables/dashboard/usePanelVariableSubstitution.ts` | 面板查询中的变量占位符替换 |
| 面板数据加载 | `web/src/composables/dashboard/usePanelDataLoader.ts` | 面板查询执行、变量变更监听 |
| 面板缓存 | `web/src/composables/dashboard/usePanelCache.ts` | 面板级别数据缓存 |

---

## 2. 变量类型与定义

### 2.1 变量类型 (`VariableConfig.type`)

| 类型 | 说明 | 数据来源 |
|------|------|---------|
| `query_values` | 查询值变量 | 通过 API 查询数据流的字段值 |
| `custom` | 自定义变量 | 用户预定义的选项列表 |
| `constant` | 常量变量 | 固定值，不可修改 |
| `textbox` | 文本框变量 | 用户自由输入 |
| `dynamic_filters` | 动态过滤器 | 运行时添加的临时过滤条件 |

### 2.2 变量作用域 (`VariableConfig.scope`)

| 作用域 | 配置字段 | 生效范围 |
|--------|---------|---------|
| `global` | 默认 | 整个仪表盘所有面板 |
| `tabs` | `tabs: string[]` | 指定 Tab 下的所有面板 |
| `panels` | `panels: string[]` | 仅指定面板 |

**作用域类型判定逻辑** (`variablesScopeUtils.ts:25-32`):
```typescript
export const getScopeType = (variable: any): "panels" | "tabs" | "global" => {
  if (variable.panels && variable.panels.length > 0) return "panels";
  else if (variable.tabs && variable.tabs.length > 0) return "tabs";
  else return "global";
};
```

---

## 3. 变量取值解析过程

### 3.1 变量语法标准化

支持三种变量占位符语法，解析前会先标准化：

| 语法形式 | 示例 | 标准化后 |
|---------|------|---------|
| Mustache 双大括号 | `{{ varName }}` | `{{varName}}` |
| Mustache 带格式 | `{{ varName : csv }}` | `{{varName:csv}}` |
| Dollar 括号 | `${ varName }` | `${varName}` |
| Dollar 括号带格式 | `${ varName : pipe }` | `${varName:pipe}` |
| Dollar 简写 | `$varName` | `$varName` |

**标准化函数** (`variablesUtils.ts:250-262`):
```typescript
export const normalizeVariableSyntax = (str: string): string => {
  // 移除 {{ }} 和 ${ } 内部的空白字符
  str = str.replace(/\{\{\s*([a-zA-Z0-9_-]+)\s*(?::\s*([a-zA-Z]+)\s*)?\}\}/g, 
    (_, name, format) => format ? `{{${name}:${format}}}` : `{{${name}}}`);
  str = str.replace(/\$\{\s*([a-zA-Z0-9_-]+)\s*(?::\s*([a-zA-Z]+)\s*)?\}/g,
    (_, name, format) => format ? `\${${name}:${format}}` : `\${${name}}`);
  return str;
};
```

### 3.2 数组值格式化选项

当变量值为数组时，支持以下格式化选项：

| 格式符 | 效果（示例值：`["a","b","c"]`） |
|--------|--------------------------------|
| `:csv` | `a,b,c` |
| `:pipe` | `a\|b\|c` |
| `:doublequote` | `"a","b","c"` |
| `:singlequote` | `'a','b','c'` |
| 默认（SQL） | `'a','b','c'` |
| 默认（PromQL） | `a\|b\|c` |

### 3.3 空值处理策略

当变量值为 `null`/`undefined` 或空数组时：
- 使用 `SELECT_ALL_VALUE`（"__SELECT_ALL__"）作为占位符
- 查询时通常匹配所有值

---

## 4. 作用域优先级与覆盖规则

### 4.1 三级优先级机制

**解析优先级**：`面板级 > Tab级 > 全局`

**核心解析函数** (`variablesUtils.ts:142-244`):
```typescript
const resolveVariablesWithPrecedence = (variablesData, context) => {
  // 1. 按变量名分组（同名不同作用域）
  const variablesByName = groupByName(variablesData.values);
  
  // 2. 对每个变量按优先级解析
  Object.keys(variablesByName).forEach((name) => {
    const variables = variablesByName[name];
    
    // 优先级1: 面板级 (panelId 匹配)
    if (context.panelId) {
      const panelVar = findPanelLevelVar(variables, context.panelId);
      if (panelVar?.value != null) return panelVar.value;
    }
    
    // 优先级2: Tab级 (tabId 匹配)
    if (context.tabId) {
      const tabVar = findTabLevelVar(variables, context.tabId);
      if (tabVar?.value != null) return tabVar.value;
    }
    
    // 优先级3: 全局 (scope=global 或无 scope)
    const globalVar = findGlobalVar(variables);
    return globalVar?.value;
  });
};
```

### 4.2 变量展开机制

变量配置在运行时会按作用域展开为多个实例：

**展开函数** (`useVariablesManager.ts:83-176`):
```typescript
const expandVariablesForScopes = (variables: VariableConfig[]) => {
  variables.forEach((variable) => {
    if (scope === "global") {
      // 全局变量：1个实例
      expanded.push({ ...variable, scope: "global" });
    } else if (scope === "tabs" && variable.tabs) {
      // Tab变量：每个指定Tab生成1个实例
      variable.tabs.forEach(tabId => {
        expanded.push({ ...variable, scope: "tabs", tabId });
      });
    } else if (scope === "panels" && variable.panels) {
      // 面板变量：每个指定面板生成1个实例
      variable.panels.forEach(panelId => {
        expanded.push({ ...variable, scope: "panels", panelId });
      });
    }
  });
};
```

---

## 5. 变量依赖图与加载顺序

### 5.1 依赖关系构建

`query_values` 类型变量的查询本身可能包含其他变量引用，形成依赖链。

**依赖提取来源** (`variablesDependencyUtils.ts:57-94`):
1. `query_data.stream` - 流名称可能包含变量
2. `query_data.field` - 字段名可能包含变量
3. `query_data.filter[].value` - 过滤条件值可能包含变量

**变量名提取正则**:
```typescript
// 匹配三种变量语法形式
const regex = /(?:\$\{\s*([a-zA-Z0-9_-]+)\s*(?::\s*[a-zA-Z]+\s*)?\})|(?:\$([a-zA-Z0-9_-]+))|(?:\{\{\s*([a-zA-Z0-9_-]+)\s*(?::\s*[a-zA-Z]+\s*)?\}\})/g;
```

### 5.2 作用域感知的依赖解析

子变量查找父变量时遵循作用域层级：

| 子变量作用域 | 父变量查找顺序 |
|------------|---------------|
| `global` | 1. 同作用域 global |
| `tabs` | 1. 同 Tab 的 tabs 变量<br>2. global 变量 |
| `panels` | 1. 同面板的 panels 变量<br>2. 所属 Tab 的 tabs 变量<br>3. global 变量 |

**依赖有效性规则** (`variablesDependencyUtils.ts:238-258`):
- ✅ global 可以作为任何变量的父节点
- ✅ tabs 可以作为同 Tab panels 或同 Tab tabs 的父节点
- ✅ panels 只能作为同面板 panels 的父节点
- ❌ tabs 不能作为其他 Tab 变量的父节点
- ❌ panels 不能作为其他面板或 Tab 变量的父节点

### 5.3 循环依赖检测

使用 DFS 深度优先搜索检测循环依赖：

**检测算法** (`variablesDependencyUtils.ts:117-186`):
```typescript
const isGraphHasCycleUtil = (node, visited, recStack, graph, path) => {
  if (!visited[node]) {
    visited[node] = true;
    recStack[node] = true;
    path.push(node);
    
    // 递归检查所有父节点（依赖的变量）
    for (const parent of graph[node].parentVariables) {
      if (!visited[parent] && isGraphHasCycleUtil(parent, visited, recStack, graph, path))
        return true;
      else if (recStack[parent])  // 发现回边
        return true;
    }
  }
  recStack[node] = false;
  path.pop();
  return false;
};
```

---

## 6. 与面板查询参数的绑定机制

### 6.1 两级状态架构

变量管理器维护**两套独立状态**，避免用户修改变量时频繁触发面板查询：

| 状态 | 触发时机 | 使用者 |
|------|---------|--------|
| **Live State**（实时状态） | 用户每次修改变量值立即更新 | 变量选择器 UI、变更检测 |
| **Committed State**（提交状态） | 用户点击「刷新」按钮时提交 | 面板查询执行 |

**状态存储结构** (`useVariablesManager.ts:183-205`):
```typescript
// 实时状态 - 用户操作立即更新
const variablesData = reactive({
  global: VariableRuntimeState[],
  tabs: Record<string, VariableRuntimeState[]>,
  panels: Record<string, VariableRuntimeState[]>,
  isInitialized: boolean,
});

// 提交状态 - 仅在刷新时更新，面板使用此状态
const committedVariablesData = reactive({
  global: VariableRuntimeState[],
  tabs: Record<string, VariableRuntimeState[]>,
  panels: Record<string, VariableRuntimeState[]>,
});
```

### 6.2 面板变量获取入口

**渲染面板时获取已提交变量** (`RenderDashboardCharts.vue:504-526`):
```typescript
const getMergedVariablesForPanel = (panelId: string) => {
  // 优先级1: 面板特定冻结覆盖（用户点击单个面板刷新）
  if (currentVariablesDataRef.value?.[panelId]) {
    return currentVariablesDataRef.value[panelId];
  }
  
  // 优先级2: 管理器提交状态
  const mergedVars = variablesManager.getCommittedVariablesForPanel(
    panelId,
    selectedTabId.value,
  );
  
  return {
    isVariablesLoading: variablesManager.isLoading.value,
    values: mergedVars,  // global + tab + panel 合并数组
  };
};
```

### 6.3 查询替换流水线

变量替换在查询执行前分两步完成：

```
原始查询字符串
     ↓
[步骤1] replaceQueryValue() - 替换常规变量占位符
     ↓  包含：固定变量(__interval等) + 用户自定义变量
     ↓
[步骤2] applyDynamicVariables() - 应用动态过滤器
     ↓  PromQL: addLabelToPromQlQuery()
     ↓  SQL: addLabelsToSQlQuery()
     ↓
最终查询
```

**核心替换函数** (`usePanelVariableSubstitution.ts:443-672`):
```typescript
const replaceQueryValue = (query, startISOTimestamp, endISOTimestamp, queryType) => {
  // 步骤0: 语法标准化
  query = normalizeVariableSyntax(query);
  
  // 步骤1: 替换固定时间变量
  const fixedVariables = [
    { name: "__interval_ms", value: `${__interval_ms}ms` },
    { name: "__interval", value: `${formattedInterval.value}${formattedInterval.unit}` },
    { name: "__rate_interval", value: formatRateInterval(__rate_interval) },
    { name: "__range", value: formattedRange },
    { name: "__range_s", value: `${Math.floor(__range_seconds)}` },
    { name: "__range_ms", value: `${Math.floor(__range_seconds * 1000)}` },
  ];
  
  // 步骤2: 替换用户自定义变量（支持多种格式化选项）
  currentDependentVariablesData.forEach((variable) => {
    // 数组值按 queryType 选择默认格式化
    // SQL: 默认单引号逗号分隔
    // PromQL: 默认管道分隔
  });
  
  return { query, metadata };
};
```

---

## 7. 面板间变量共享与隔离

### 7.1 共享机制

| 变量作用域 | 共享范围 | 示例场景 |
|-----------|---------|---------|
| Global | 仪表盘所有面板 | 时间范围、环境选择 |
| Tab | 同一 Tab 下所有面板 | 按业务域分组的过滤条件 |
| Panel | 仅单个面板 | 特定图表的自定义维度 |

### 7.2 隔离机制 - 面板级刷新

**面板单独刷新流程** (`RenderDashboardCharts.vue:1297-1303`):
```typescript
// 1. 仅提交该面板的变量
variablesManager.commitScope("panels", panelId);

// 2. 获取该面板的合并变量
const panelVars = variablesManager.getVariablesForPanel(
  panelId,
  selectedTabId.value,
);

// 3. 存储为面板特定覆盖，不影响其他面板
currentVariablesDataRef.value[panelId] = {
  isVariablesLoading: false,
  values: panelVars,
};

// 4. 仅该面板重新查询，其他面板不受影响
```

### 7.3 渐进式加载

面板不等待所有变量加载完成，而是**只要自身依赖的变量就绪即可开始查询**：

**面板级就绪检查** (`usePanelVariableSubstitution.ts:219-241`):
```typescript
const ifPanelVariablesCompletedLoading = () => {
  // 只检查该面板查询中引用的变量
  const newDependentVariablesData = getDependentVariablesData();
  
  // 关键判断：isVariablePartialLoaded === true
  // 表示变量已至少加载过一次（即使值为 null）
  return !newDependentVariablesData.some((it) => {
    const hasNullValue = it.value == null || (Array.isArray(it.value) && it.value.length === 0);
    const hasNeverBeenLoaded = !it.isVariablePartialLoaded;
    return hasNullValue && hasNeverBeenLoaded;  // 仅从未加载过才阻塞
  });
};
```

---

## 8. 值缓存与刷新策略

### 8.1 面板级别数据缓存

**存储方案**: IndexedDB 本地存储

**数据库结构** (`usePanelCache.ts:3-12`):
```
Database: 'PanelCache'
└── Object Store: 'panels'
    ├── Key: 'folder_id:dashboard_id:panel_id'
    └── Value: {
        key: { panelSchema, variablesData, forceLoad, ... },  // 缓存键
        value: { data, loading, errorDetail, ... },           // 面板状态
        cacheTimeRange: { start_time, end_time },             // 缓存时间范围
        timestamp: number                                     // 缓存时间戳
      }
```

### 8.2 缓存键计算

**缓存键组成** (`usePanelDataLoader.ts:115-127`):
```typescript
const getCacheKey = () => ({
  panelSchema: toRaw(panelSchema.value),           // 面板配置
  variablesData: [...dependentVars, ...dynamicVars], // 依赖变量值
  forceLoad: toRaw(forceLoad.value),               // 强制刷新标志
  dashboardId: toRaw(dashboardId?.value),          // 仪表盘ID
  folderId: toRaw(folderId?.value),                // 文件夹ID
});
```

**缓存匹配时忽略的字段**:
- `panelSchema.version` - 版本号
- `panelSchema.layout` - 布局信息
- `panelSchema.htmlContent` / `markdownContent` - 展示内容
- 变量运行时状态字段 (`options`, `isLoading`, `isVariableLoadingPending`, `isVariablePartialLoaded`)

### 8.3 缓存命中判断

**命中条件** (`usePanelDataLoader.ts:751-848`):
1. ✅ 缓存记录存在
2. ✅ 缓存键匹配（忽略非关键字段）
3. ✅ 首次加载（`runCount == 0`）
4. ✅ 非强制刷新（`forceLoad != true`）

### 8.4 面板刷新触发条件

**触发查询刷新的场景**:

| 触发源 | 监听逻辑 | 处理方式 |
|--------|---------|---------|
| 时间范围变更 | `watch([selectedTimeObj, forceLoad])` | 所有面板刷新 |
| 面板配置变更 | `watch([panelSchema])` | 单个面板刷新，先检查是否需要 API 调用 |
| 变量值提交 | 变量 watcher + `variablesDataUpdated()` | 检测到依赖变量值变化时刷新 |
| 面板可见性 | IntersectionObserver | 面板进入视口时加载 |

**变量变更检测逻辑** (`usePanelVariableSubstitution.ts:243-433`):
```typescript
const variablesDataUpdated = () => {
  // 步骤1: 动态变量是否仍在加载
  if (areDynamicVariablesStillLoading()) return false;
  
  // 步骤2: 依赖变量是否仍在加载
  if (areDependentVariablesStillLoadingWith(newDependentVariablesData)) return false;
  
  // 步骤3: 变量数量是否变化（新增/删除）
  if (countChanged()) {
    updateSnapshots();
    return true;
  }
  
  // 步骤4: 根据变量组合判断值是否变化
  // - 无变量: true
  // - 仅常规变量: 比较值
  // - 仅动态变量: 比较值
  // - 两者都有: 任一变化即触发
};
```

---

## 9. 完整传导链路图

```
用户操作
    ↓
[变量选择器 UI]
    ↓ 更新 Live State
[useVariablesManager]
    ↓
    ├─→ 构建依赖图 + 循环检测
    ├─→ 按依赖顺序加载变量（级联）
    └─→ 标记 isVariablePartialLoaded
    ↓
[用户点击刷新按钮]
    ↓ commitAll() / commitScope()
[Committed State 更新]
    ↓
[usePanelDataLoader] 变量 watcher 触发
    ↓
[usePanelVariableSubstitution]
    ├─→ getDependentVariablesData()  提取面板依赖变量
    ├─→ variablesDataUpdated()      检查值是否真正变化
    └─→ 是 → loadData()
    ↓
[等待条件]
    ├─→ waitForTimeout(50ms)           防抖
    ├─→ waitForThePanelToBecomeVisible 可见性
    └─→ waitForTheVariablesToLoad       面板依赖就绪
    ↓
[查询执行]
    ├─→ replaceQueryValue()       替换常规变量
    └─→ applyDynamicVariables()   应用动态过滤器
    ↓
[HTTP Streaming API]
    ↓
[面板渲染 + 缓存保存]
```

---

## 10. 关键设计总结

| 设计点 | 实现方式 | 优势 |
|--------|---------|------|
| **作用域优先级** | 三级覆盖（面板 > Tab > 全局） | 灵活控制变量影响范围 |
| **两级状态** | Live + Committed 分离 | 避免用户操作频繁触发查询 |
| **依赖图解析** | 作用域感知的 DAG 构建 | 正确处理级联查询顺序 |
| **渐进式加载** | 面板级就绪检查 | 提升首屏加载速度 |
| **面板级缓存** | IndexedDB 按面板存储 | 快速恢复仪表盘状态 |
| **面板级刷新** | 独立变量覆盖 + 单独提交 | 细粒度控制刷新范围 |
