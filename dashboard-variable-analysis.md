# OpenObserve 仪表盘变量、模板与面板查询传导链路深度分析

## 1. 变量系统架构概览

OpenObserve 的仪表盘变量系统采用**三层作用域架构**，配合**两级状态管理**、**依赖图解析**和**可见性驱动加载**实现变量在面板查询中的传导与应用。

### 核心文件定位

| 模块 | 文件路径 | 主要职责 |
|------|---------|---------|
| 变量工具 | `web/src/utils/dashboard/variables/variablesUtils.ts` | 变量值解析、语法标准化、时间区间变量计算 |
| 作用域工具 | `web/src/utils/dashboard/variables/variablesScopeUtils.ts` | 变量作用域类型判断 |
| 依赖图工具 | `web/src/utils/dashboard/variables/variablesDependencyUtils.ts` | 变量依赖关系构建、作用域感知解析、循环检测 |
| 变量管理器 | `web/src/composables/dashboard/useVariablesManager.ts` | 变量状态管理、初始化展开、提交机制、URL 同步、可见性控制 |
| 变量替换 | `web/src/composables/dashboard/usePanelVariableSubstitution.ts` | 面板查询中的变量占位符替换、变更检测、加载就绪判断 |
| 面板数据加载 | `web/src/composables/dashboard/usePanelDataLoader.ts` | 面板查询执行、可见性观察、变量 watcher、缓存恢复 |
| 面板缓存 | `web/src/composables/dashboard/usePanelCache.ts` | 面板级别 IndexedDB 缓存 |
| 变量选择器 | `web/src/components/dashboards/VariablesValueSelector.vue` | 变量 UI 交互、Streaming 数据获取、选项管理、父子级联 |
| 仪表盘渲染 | `web/src/views/Dashboards/RenderDashboardCharts.vue` | 变量管理器实例化、全局/Tab/面板变量分层渲染、刷新协调 |
| 仪表盘视图 | `web/src/views/Dashboards/ViewDashboard.vue` | URL 参数读写、刷新触发、下钻跳转 |
| 下钻跳转 | `web/src/composables/dashboard/usePanelDrilldown.ts` | 下钻时的变量替换与 URL 构建 |

---

## 2. 变量类型与定义

### 2.1 变量类型 (`VariableConfig.type`)

| 类型 | 说明 | 数据来源 | 是否需要 API |
|------|------|---------|------------|
| `query_values` | 查询值变量 | Streaming API 查询字段值 | ✅ 需要 |
| `custom` | 自定义变量 | 用户预定义的选项列表 | ❌ 不需要 |
| `constant` | 常量变量 | 固定值，不可修改 | ❌ 不需要 |
| `textbox` | 文本框变量 | 用户自由输入 | ❌ 不需要 |
| `dynamic_filters` | 动态过滤器 | 运行时添加的临时过滤条件 | ❌ 不需要 |

### 2.2 变量作用域 (`VariableConfig.scope`)

| 作用域 | 配置字段 | 生效范围 | URL 参数格式 |
|--------|---------|---------|-------------|
| `global` | 默认 | 整个仪表盘所有面板 | `var-{name}` |
| `tabs` | `tabs: string[]` | 指定 Tab 下的所有面板 | `var-{name}.t.{tabId}` |
| `panels` | `panels: string[]` | 仅指定面板 | `var-{name}.p.{panelId}` |

### 2.3 运行时状态标记

每个变量运行时维护三个关键状态标记：

| 标记 | 含义 | 设为 true 的时机 |
|------|------|----------------|
| `isLoading` | 正在进行 API 请求 | 发起 Streaming 请求前 |
| `isVariableLoadingPending` | 需要加载（等待触发） | 父变量就绪后、可见性变更后 |
| `isVariablePartialLoaded` | 已至少加载过一次（有值或确认无数据） | API 首次返回数据时；自定义/常量/文本框初始化时即标为 true |

---

## 3. 模板渲染中变量替换的完整链路

### 3.1 变量语法标准化

支持三种变量占位符语法，替换前统一标准化，消除空白差异：

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
  str = str.replace(/\{\{\s*([a-zA-Z0-9_-]+)\s*(?::\s*([a-zA-Z]+)\s*)?\}\}/g,
    (_, name, format) => format ? `{{${name}:${format}}}` : `{{${name}}}`);
  str = str.replace(/\$\{\s*([a-zA-Z0-9_-]+)\s*(?::\s*([a-zA-Z]+)\s*)?\}/g,
    (_, name, format) => format ? `\${${name}:${format}}` : `\${${name}}`);
  return str;
};
```

### 3.2 查询替换两阶段流水线

变量替换在查询执行前分两阶段完成：

```
原始查询字符串
     ↓
[阶段1] replaceQueryValue()
     ├─ 步骤0: normalizeVariableSyntax() 语法标准化
     ├─ 步骤1: 替换6个固定时间变量
     │    __interval_ms, __interval, __rate_interval,
     │    __range, __range_s, __range_ms
     ├─ 步骤2: 替换用户自定义常规变量
     │    遍历 currentDependentVariablesData
     │    ├─ 数组值: 按 queryType 选择默认格式化
     │    │    SQL → 单引号逗号分隔 ('a','b','c')
     │    │    PromQL → 管道分隔 (a|b|c)
     │    ├─ 空值: 替换为 SELECT_ALL_VALUE ("__SELECT_ALL__")
     │    └─ 支持格式化后缀: :csv, :pipe, :doublequote, :singlequote
     └─ 返回 { query, metadata }
     ↓
[阶段2] applyDynamicVariables()
     ├─ 提取 dynamic_filters 类型变量
     ├─ 过滤出有效的 (name + operator + value 都存在)
     ├─ PromQL: addLabelToPromQlQuery() 逐个添加 label
     └─ SQL: addLabelsToSQlQuery() 批量添加 WHERE 条件
     ↓
最终查询字符串
```

**关键实现细节** (`usePanelVariableSubstitution.ts:443-672`):

固定变量基于面板宽度和时间范围动态计算：
```typescript
const __interval = (endISOTimestamp - startISOTimestamp) /
  (chartPanelRef.value?.offsetWidth ?? 1000) / 1000;
const formattedInterval = formatInterval(__interval);
const __rate_interval = Math.max(
  getTimeInSecondsBasedOnUnit(formattedInterval.value, formattedInterval.unit) + scrapeInterval,
  4 * scrapeInterval,
);
```

数组值替换支持11种占位符形式（Mustache 5种 + Dollar 6种）：
```typescript
const possibleVariablesPlaceHolderTypes = [
  { placeHolder: `{{${variable.name}:csv}}`,       value: valueToUse.join(",") },
  { placeHolder: `{{${variable.name}:pipe}}`,       value: valueToUse.join("|") },
  { placeHolder: `{{${variable.name}:doublequote}}`,value: valueToUse.map(v => `"${v}"`).join(",") },
  { placeHolder: `{{${variable.name}:singlequote}}`,value: value },
  { placeHolder: `{{${variable.name}}}`,            value: queryType === "sql" ? value : valueToUse.join("|") },
  { placeHolder: `\${${variable.name}:csv}`,        value: valueToUse.join(",") },
  { placeHolder: `\${${variable.name}:pipe}`,       value: valueToUse.join("|") },
  { placeHolder: `\${${variable.name}:doublequote}`,value: valueToUse.map(v => `"${v}"`).join(",") },
  { placeHolder: `\${${variable.name}:singlequote}`,value: value },
  { placeHolder: `\${${variable.name}}`,            value: queryType === "sql" ? value : valueToUse.join("|") },
  { placeHolder: `\$${variable.name}`,              value: queryType === "sql" ? value : valueToUse.join("|") },
];
```

### 3.3 下钻跳转中的变量替换

下钻（drilldown）使用独立的替换逻辑，支持点号路径和方括号路径访问嵌套对象：

**URL 下钻** (`usePanelDrilldown.ts:122-148`):
```typescript
const replacePlaceholders = (str, obj) => {
  str = normalizeVariableSyntax(str);
  return str.replace(/(?:\{\{([^}]+)\}\})|(?:\$\{([^}]+)\})/g, (_, mustacheKey, dollarKey) => {
    const key = (mustacheKey || dollarKey).trim();
    let parts = key.split(/\.|\["(.*?)"\]/).filter(Boolean);
    let value = obj;
    for (let part of parts) {
      if (value && part in value) value = value[part];
      else return mustacheKey ? "{{" + key + "}}" : "${" + key + "}";
    }
    return value;
  });
};
```

下钻变量对象构建：
```typescript
const drilldownVariables = {
  start_time, end_time,           // 时间范围
  query, query_encoded,           // 当前查询及其 Base64 编码
  row: { field, index },          // 表格行数据（table 类型）
  series: { __name, __value, __axisValue },  // 图表系列数据
  node: { __name, __value },      // 桑科图节点数据
  edge: { __source, __target, __value },      // 桑科图边数据
  ...userVariables,               // 仪表盘变量值（排除 dynamic_filters）
};
```

---

## 4. URL 参数恢复为变量的全过程

### 4.1 URL 参数命名规则

| 参数格式 | 作用域 | 示例 |
|---------|--------|------|
| `var-{name}` | global | `var-environment=production` |
| `var-{name}.t.{tabId}` | tabs | `var-region.t.tab1=us-east` |
| `var-{name}.p.{panelId}` | panels | `var-metric.p.panel3=cpu` |

`dynamic_filters` 类型值在 URL 中以 `encodeURIComponent(JSON.stringify(value))` 形式编码。

### 4.2 完整恢复流程

```
URL ?var-env=prod&var-region.t.tab1=us-east
     ↓
[ViewDashboard.vue:670-678] 初始解析
     const initialVariableValues = reactive({ value: {} });
     Object.keys(route.query).forEach((key) => {
       if (key.startsWith("var-")) {
         const newKey = key.slice(4);
         initialVariableValues.value[newKey] = route.query[key];
       }
     });
     ↓
[RenderDashboardCharts.vue:1039] 管理器加载
     variablesManager.loadFromUrl(route);
     ↓
[useVariablesManager.ts:878-943] loadFromUrl 核心逻辑
     ├─ 遍历 route.query 所有 var-* 参数
     ├─ parseVariableUrlKey() 解析作用域
     │    ├─ 匹配 /^(.+)\.t\.(.+)$/ → tabs 作用域
     │    ├─ 匹配 /^(.+)\.p\.(.+)$/ → panels 作用域
     │    └─ 无后缀 → global 作用域
     ├─ parseValue() 解析值
     │    ├─ dynamic_filters: JSON.parse(decodeURIComponent(value))
     │    ├─ multiSelect: Array.isArray(value) ? value : [value]
     │    └─ 单选: Array.isArray(value) ? value[0] : value
     ├─ global 变量: 同步到所有 tab/panel 同名实例
     │    （向下覆盖，保证下钻兼容性）
     └─ 标记为完全加载
          variable.isVariablePartialLoaded = true;
          variable.isVariableLoadingPending = false;
          variable.isLoading = false;
     ↓
[RenderDashboardCharts.vue:1046] 初始提交
     variablesManager.commitAll();
     ↓
[面板获取已提交变量，开始查询]
```

### 4.3 URL 参数生成（反向同步）

**触发时机**：当 `committedVariablesData` 变更时，`ViewDashboard.vue` 中的 watcher 自动调用 `updateUrlWithCurrentState()`。

**生成逻辑** (`useVariablesManager.ts:1005-1059`):
```typescript
const getUrlParams = (opts?: { useLive: boolean }): Record<string, any> => {
  const sourceData = useLive ? variablesData : committedVariablesData;

  // Global: var-{name}
  sourceData.global.forEach((variable) => {
    if (hasValidValue(variable.value)) {
      variableParams[`var-${variable.name}`] = variable.value;
    }
  });

  // Tab: var-{name}.t.{tabId}
  Object.entries(sourceData.tabs).forEach(([tabId, variables]) => {
    variables.forEach((variable) => {
      if (hasValidValue(variable.value)) {
        variableParams[`var-${variable.name}.t.${tabId}`] = variable.value;
      }
    });
  });

  // Panel: var-{name}.p.{panelId}
  Object.entries(sourceData.panels).forEach(([panelId, variables]) => {
    variables.forEach((variable) => {
      if (hasValidValue(variable.value)) {
        variableParams[`var-${variable.name}.p.${panelId}`] = variable.value;
      }
    });
  });

  return variableParams;
};
```

**URL 更新防护机制**：
- `isDrilldownInProgress`：下钻进行中时跳过 URL 更新，避免覆盖下钻参数
- `isInternalUrlUpdate`：内部 URL 更新时阻止 `var-*` watcher 重复触发

### 4.4 同仪表盘下钻的 URL 恢复

当用户在同仪表盘内通过下钻传递新变量值时：

```typescript
// RenderDashboardCharts.vue:1190-1224
const updateInitialVariableValues = async (...args) => {
  const isSameDashboard = route.query.dashboard === props.dashboardData?.dashboardId;

  if (isSameDashboard) {
    // 不重新加载仪表盘，直接推入新值
    variablesManager.loadFromUrl({ query: route.query });
    variablesManager.commitAll();

    // 更新面板变量引用，触发面板重渲染
    const allGlobalVars = variablesManager.committedVariablesData.global;
    currentVariablesDataRef.value = {
      __global: JSON.parse(JSON.stringify({
        isVariablesLoading: variablesManager.isLoading.value,
        values: allGlobalVars,
      })),
    };
  } else {
    // 跨仪表盘下钻：完整重载
    refreshDashboard(false);
  }
};
```

---

## 5. 面板局部与全局刷新的触发条件

### 5.1 两级状态架构

变量管理器维护**两套独立响应式状态**：

| 状态 | 更新时机 | 使用者 |
|------|---------|--------|
| **Live State** (`variablesData`) | 用户每次修改变量值立即更新 | 变量选择器 UI、变更检测指示器 |
| **Committed State** (`committedVariablesData`) | 用户点击「刷新」按钮或自动提交时更新 | 面板查询执行 |

**状态存储结构** (`useVariablesManager.ts:183-205`):
```typescript
const variablesData = reactive({
  global: VariableRuntimeState[],
  tabs: Record<string, VariableRuntimeState[]>,
  panels: Record<string, VariableRuntimeState[]>,
  isInitialized: boolean,
});

const committedVariablesData = reactive({
  global: VariableRuntimeState[],
  tabs: Record<string, VariableRuntimeState[]>,
  panels: Record<string, VariableRuntimeState[]>,
});
```

### 5.2 全局刷新

**触发条件**：

| 触发源 | 代码位置 | 处理方式 |
|--------|---------|---------|
| 用户点击刷新按钮 | `ViewDashboard.vue` → `refreshData()` | `commitAll()` + 所有面板重新查询 |
| 时间范围变更 | `usePanelDataLoader.ts:451` watcher | `watch([selectedTimeObj, forceLoad])` → `loadData()` |
| 首次变量加载完成（自动提交） | `RenderDashboardCharts.vue:1076` watcher | 检测 `isVariablePartialLoaded` 从 false→true 且不在 committed 中 → `commitAll()` |

**commitAll 实现** (`useVariablesManager.ts:564-588`):
```typescript
const commitAll = () => {
  committedVariablesData.global = variablesData.global.map((v) => ({
    ...v,
    value: Array.isArray(v.value) ? [...v.value] : v.value,
  }));

  committedVariablesData.tabs = {};
  Object.entries(variablesData.tabs).forEach(([tabId, vars]) => {
    committedVariablesData.tabs[tabId] = vars.map((v) => ({
      ...v,
      value: Array.isArray(v.value) ? [...v.value] : v.value,
    }));
  });

  committedVariablesData.panels = {};
  Object.entries(variablesData.panels).forEach(([panelId, vars]) => {
    committedVariablesData.panels[panelId] = vars.map((v) => ({
      ...v,
      value: Array.isArray(v.value) ? [...v.value] : v.value,
    }));
  });
};
```

**自动提交的渐进式加载策略** (`RenderDashboardCharts.vue:1062-1136`):

系统监听所有变量的 `isVariablePartialLoaded` 变化，当检测到**首次加载完成**时自动提交：

```typescript
watch(() => ({
  global: variablesManager.variablesData.global,
  tabs: variablesManager.variablesData.tabs,
  panels: variablesManager.variablesData.panels,
}), (newData) => {
  const allVariables = [
    ...newData.global,
    ...Object.values(newData.tabs).flat(),
    ...Object.values(newData.panels).flat(),
  ];

  let shouldAutoCommit = false;
  for (const variable of allVariables) {
    if (variable.type !== "query_values") continue;
    if (!variable.isVariablePartialLoaded) continue;

    const committedVar = findInCommitted(variable);
    if (!committedVar) {
      // 变量不在 committed 中 → 首次加载 → 自动提交
      shouldAutoCommit = true;
      break;
    } else if (committedVar.isVariablePartialLoaded === false) {
      // 变量在 committed 中但从未加载 → 首次加载 → 自动提交
      shouldAutoCommit = true;
      break;
    }
    // committedVar.isVariablePartialLoaded === true → 重新加载 → 不自动提交
  }

  if (shouldAutoCommit) variablesManager.commitAll();
}, { deep: true });
```

| 场景 | 是否自动提交 |
|------|------------|
| 初始加载 | ✅ 自动提交 |
| Tab 切换后首次变量加载 | ✅ 自动提交 |
| 面板可见后首次变量加载 | ✅ 自动提交 |
| 父变量变更导致子变量重新加载 | ❌ 不自动提交（用户需手动刷新） |

### 5.3 面板局部刷新

**触发条件**：用户点击单个面板的刷新按钮。

**面板级刷新流程** (`RenderDashboardCharts.vue:1279-1313`):
```typescript
const refreshPanelRequest = async (panelId, shouldRefreshWithoutCache) => {
  // 1. 同步面板时间选择器状态
  syncPanelDateTimePickerState(panelId);

  // 2. 更新 URL 中的面板时间参数
  const timeValue = panelTimeValues.value[panelId];
  if (timeValue) await updateURLWithPanelTime(panelId, timeValue);

  // 3. 仅提交面板作用域变量
  variablesManager.commitScope("panels", panelId);

  // 4. 获取该面板的合并变量
  const panelVars = variablesManager.getVariablesForPanel(panelId, selectedTabId.value);

  // 5. 存储为面板特定覆盖（不影响其他面板）
  currentVariablesDataRef.value = {
    ...currentVariablesDataRef.value,
    [panelId]: JSON.parse(JSON.stringify({
      isVariablesLoading: variablesManager.isLoading.value,
      values: panelVars,
    })),
  };
};
```

**commitScope 实现** (`useVariablesManager.ts:594-605`):
```typescript
const commitScope = (scope: "panels", id: string) => {
  if (scope === "panels") {
    if (variablesData.panels[id]) {
      committedVariablesData.panels[id] = variablesData.panels[id].map((v) => ({
        ...v,
        value: Array.isArray(v.value) ? [...v.value] : v.value,
      }));
    }
  }
};
```

### 5.4 变量变更检测（面板级 watcher）

面板通过 `usePanelDataLoader` 中的 watcher 检测变量值变化：

**watcher 逻辑** (`usePanelDataLoader.ts:646-670`):
```typescript
watch(
  () => variablesData?.value?.values,
  () => {
    const newDependentVariablesData = getDependentVariablesData();
    const newDynamicVariablesData = getDynamicVariablesData();

    // 无变量且历史也无变量 → 跳过
    if (!newDependentVariablesData?.length && !newDynamicVariablesData?.length &&
        !getCurrentDependentVariablesData()?.length && !getCurrentDynamicVariablesData()?.length) {
      return;
    }

    if (variablesDataUpdated()) {
      loadData();
    }
  },
  { deep: true },
);
```

**variablesDataUpdated 四步判断** (`usePanelVariableSubstitution.ts:243-433`):

```
Step1: 动态过滤器是否仍在加载？
  └─ 是 → return false（不触发刷新）

Step2: 面板依赖的常规变量是否仍在加载？
  └─ 是 → return false
  └─ 判定标准：(value 为 null/空数组) AND (isVariablePartialLoaded === false)

Step3: 变量数量是否变化？
  └─ 是 → 更新快照，return true（触发刷新）

Step4: 变量值是否变化？（四种组合）
  ├─ 无常规 + 无动态 → true
  ├─ 有常规 + 无动态 → isAllRegularVariablesValuesSameWith()
  ├─ 无常规 + 有动态 → isAllDynamicVariablesValuesSameWith()
  └─ 有常规 + 有动态 → 任一变化即触发
```

### 5.5 全局刷新 vs 面板局部刷新对比

| 维度 | 全局刷新 | 面板局部刷新 |
|------|---------|------------|
| 提交范围 | `commitAll()` 全部三个作用域 | `commitScope("panels", panelId)` 仅面板作用域 |
| 影响面板 | 所有面板 | 仅该面板 |
| 变量来源 | `committedVariablesData` 全局+Tab+面板 | `currentVariablesDataRef[panelId]` 覆盖 |
| URL 同步 | 更新所有 var-* 参数 | 更新面板时间参数 |
| 典型触发 | 刷新按钮、自动提交 | 面板刷新按钮 |

---

## 6. 基于可见性的变量加载顺序

### 6.1 可见性控制机制

变量管理器通过 `tabsVisibility` 和 `panelsVisibility` 两个响应式字典控制哪些变量可以加载：

```typescript
const tabsVisibility = ref<Record<string, boolean>>({});
const panelsVisibility = ref<Record<string, boolean>>({});
```

**可见性设置时机**：
| 事件 | 代码位置 | 行为 |
|------|---------|------|
| Tab 切换 | `RenderDashboardCharts.vue:1139-1153` | 新 Tab → `setTabVisibility(newTabId, true)`，旧 Tab → `setTabVisibility(oldTabId, false)` |
| 初始化 | `RenderDashboardCharts.vue:1051-1054` | `setTabVisibility(selectedTabId, true)` |
| AddPanel | `AddPanel.vue:694` | `setPanelVisibility("current_panel", true)` |

### 6.2 可见性驱动的变量加载

**setTabVisibility** (`useVariablesManager.ts:755-775`):
```typescript
const setTabVisibility = (tabId: string, visible: boolean) => {
  tabsVisibility.value[tabId] = visible;

  if (visible) {
    const tabVars = variablesData.tabs[tabId] || [];
    tabVars.forEach((v) => {
      if (v.type === "query_values" && !v.isVariablePartialLoaded) {
        const hasCustomOrAllDefault =
          v.selectAllValueForMultiSelect === "custom" ||
          v.selectAllValueForMultiSelect === "all";

        // 仅在变量可加载（所有父变量就绪）且非自定义默认时标记为待加载
        if (canVariableLoad(v) && !hasCustomOrAllDefault) {
          v.isVariableLoadingPending = true;
        }
      }
    });
  }
};
```

**setPanelVisibility** 逻辑与 Tab 版本对称。

### 6.3 变量可加载条件检查

**canVariableLoad** (`useVariablesManager.ts:344-389`):
```typescript
const canVariableLoad = (variable: VariableRuntimeState): boolean => {
  // 检查1: 是否可见
  if (!isVariableVisible(variable)) return false;

  // 检查2: 是否正在加载
  if (variable.isLoading) return false;

  // 检查3: 所有父变量是否就绪
  const parents = dependencyGraph.value[key]?.parents || [];
  const allParentsReady = parents.every((parentKey) => {
    const parentVar = findVariableByKey(parentKey, allVars);
    if (!parentVar) return false;
    if (parentVar.isVariablePartialLoaded !== true) return false;
    // 父变量必须有有效值
    const hasValue = parentVar.value !== null &&
      parentVar.value !== undefined &&
      parentVar.value !== "" &&
      (!Array.isArray(parentVar.value) || parentVar.value.length > 0);
    return hasValue;
  });

  return allParentsReady;
};
```

### 6.4 变量加载的完整时序

```
[初始化阶段]
  ↓
useVariablesManager.initialize()
  ├─ expandVariablesForScopes() 展开变量
  ├─ buildScopedDependencyGraph() 构建依赖图
  ├─ detectCyclesInScopedGraph() 循环检测
  ├─ 标记全局无父变量的 query_values 为 isVariableLoadingPending=true
  ├─ 标记全局非API类型变量(custom/constant/textbox) 为 isVariableLoadingPending=true
  └─ 标记全局自定义/全选默认变量的子变量为 isVariableLoadingPending=true
  ↓
VariablesValueSelector 感知 isVariableLoadingPending
  ↓ 发起 Streaming API 请求
  ↓
handleSearchResponse() 处理响应
  ├─ 解析字段值，更新 options
  ├─ 设置变量值（首次响应或值变化时）
  ├─ 标记 isVariablePartialLoaded = true
  └─ 通知管理器 onVariablePartiallyLoaded(variableKey)
  ↓
[级联加载阶段]
  ↓
onVariablePartiallyLoaded(variableKey)
  ├─ 标记 variable.isVariablePartialLoaded = true
  ├─ 获取子变量列表
  ├─ 父变量值为 null → 递归置空子变量，不触发 API
  └─ 父变量有值 → 检查 canVariableLoad(childVar)
       └─ 可以加载 → 标记 isVariableLoadingPending=true
                      → VariablesValueSelector 发起子变量 API 请求
  ↓
[可见性驱动阶段]
  ↓
用户切换 Tab → setTabVisibility(newTabId, true)
  ↓
Tab 变量检查 canVariableLoad()
  ├─ 全局父变量已就绪 → isVariableLoadingPending=true → 发起加载
  └─ 全局父变量未就绪 → 等待父变量加载完成
  ↓
面板滚动进入视口 → IntersectionObserver → isVisible=true
  ↓
面板变量检查 canVariableLoad()
  ├─ Tab/全局父变量已就绪 → isVariableLoadingPending=true → 发起加载
  └─ Tab/全局父变量未就绪 → 等待
```

### 6.5 IntersectionObserver 可见性检测

面板级别的可见性由 `IntersectionObserver` 实现：

**注册** (`usePanelDataLoader.ts:676-691`):
```typescript
onMounted(async () => {
  observer = new IntersectionObserver(handleIntersection, {
    root: null,
    rootMargin: "0px",
    threshold: 0,
  });

  setTimeout(() => {
    if (chartPanelRef?.value) {
      observer.observe(chartPanelRef?.value);
    }
  }, 0);
});

const handleIntersection = async (entries) => {
  isVisible.value = entries[0].isIntersecting;
};
```

**等待可见才查询** (`usePanelDataLoader.ts:228-255`):
```typescript
const waitForThePanelToBecomeVisible = (signal) => {
  return new Promise((resolve, reject) => {
    if (forceLoad.value == true) resolve();       // 强制加载时跳过
    if (isVisible.value) resolve();               // 已可见
    const stopWatching = watch(isVisible, (newValue) => {
      if (newValue) { resolve(); stopWatching(); } // 变为可见
    });
    signal.addEventListener("abort", () => { stopWatching(); reject(); });
  });
};
```

### 6.6 渐进式变量就绪等待

面板不等待所有变量加载完成，只等待**自身查询中引用的变量**就绪：

**等待逻辑** (`usePanelDataLoader.ts:258-293`):
```typescript
const waitForTheVariablesToLoad = (signal) => {
  return new Promise((resolve, reject) => {
    if (ifPanelVariablesCompletedLoading()) {
      resolve();  // 面板依赖的变量已就绪
      return;
    }

    const stopWatching = watch(() => variablesData.value, () => {
      if (ifPanelVariablesCompletedLoading()) {
        resolve();  // 变量就绪后解除等待
        stopWatching();
      }
    }, { deep: true });

    signal.addEventListener("abort", () => { stopWatching(); reject(); });
  });
};
```

**面板变量就绪判断** (`usePanelVariableSubstitution.ts:219-241`):
```typescript
const ifPanelVariablesCompletedLoading = () => {
  // Step1: 动态过滤器是否仍在加载
  if (areDynamicVariablesStillLoading()) return false;

  // Step2: 依赖变量是否仍在加载
  const newDependentVariablesData = getDependentVariablesData();
  if (areDependentVariablesStillLoadingWith(newDependentVariablesData)) return false;

  return true;
};
```

**关键判定标准**：`(value 为 null/空) AND (isVariablePartialLoaded === false)` → 阻塞
- `isVariablePartialLoaded=true` 但 `value=null` → 不阻塞（查询确实返回空结果）

---

## 7. 依赖图与级联加载

### 7.1 依赖关系构建

`query_values` 类型变量的查询配置可能包含其他变量引用，形成有向无环图（DAG）。

**依赖提取来源** (`variablesDependencyUtils.ts`):
1. `query_data.stream` - 流名称可能包含变量（如 `logs_${service}`）
2. `query_data.field` - 字段名可能包含变量（如 `${field}_name`）
3. `query_data.filter[].value` - 过滤条件值可能包含变量

### 7.2 作用域感知的依赖解析

子变量查找父变量时遵循作用域层级：

| 子变量作用域 | 父变量查找顺序 |
|------------|---------------|
| `global` | 1. 同作用域 global |
| `tabs` | 1. 同 Tab 的 tabs 变量 → 2. global 变量 |
| `panels` | 1. 同面板的 panels 变量 → 2. 所属 Tab 的 tabs 变量 → 3. global 变量 |

**依赖有效性规则**:
- ✅ global 可以作为任何变量的父节点
- ✅ tabs 可以作为同 Tab panels 或同 Tab tabs 的父节点
- ✅ panels 只能作为同面板 panels 的父节点
- ❌ tabs 不能作为其他 Tab 变量的父节点
- ❌ panels 不能作为其他面板或 Tab 变量的父节点

### 7.3 循环依赖检测

使用 DFS 深度优先搜索检测循环依赖，发现回边即报错：
```
Circular dependency detected: varA → varB → varC → varA
```

### 7.4 父变量变更的级联重置

当用户修改一个变量值时，管理器递归重置所有后代变量：

**updateVariableValue** (`useVariablesManager.ts:686-752`):
```typescript
const updateVariableValue = (name, scope, tabId, panelId, newValue) => {
  variable.value = newValue;

  // 递归重置所有后代
  const resetDescendants = (parentKey) => {
    const children = dependencyGraph.value[parentKey]?.children || [];
    children.forEach((childKey) => {
      const childVar = findVariableByKey(childKey, allVars);
      if (childVar) {
        childVar.isVariablePartialLoaded = false;
        childVar.isLoading = false;
        childVar.isVariableLoadingPending = false;  // 不立即标记，等待顺序加载
        childVar.value = childVar.multiSelect ? [] : null;
        childVar.options = [];
        resetDescendants(childKey);  // 递归
      }
    });
  };

  resetDescendants(variableKey);

  // 重置完成后，仅触发可立即加载的直系子变量
  immediateChildrenKeys.forEach(childKey => {
    const childVar = findVariableByKey(childKey, allVars);
    if (isVisible && canLoad) {
      childVar.isVariableLoadingPending = true;
    }
  });
};
```

**关键设计**：不立即标记后代为 `isVariableLoadingPending=true`，而是等父变量加载完成后由 `onVariablePartiallyLoaded` 逐级触发，确保**顺序加载**而非**同时加载**。

### 7.5 父变量空值的级联传播

当父变量查询返回空结果时，子变量无需再发 API 请求：

```typescript
onVariablePartiallyLoaded(variableKey) {
  // 如果父变量值为 null/空
  if (parentHasNullValue) {
    // 直接将子变量置空，不触发 API
    childVar.value = childVar.multiSelect ? [] : null;
    childVar.options = [];
    childVar.isVariablePartialLoaded = true;
    // 递归传播到孙变量
    onVariablePartiallyLoaded(childKey);
  }
}
```

---

## 8. 共享与覆盖优先级的协同机制

### 8.1 三级优先级规则

**解析优先级**：`面板级 > Tab级 > 全局`

变量在面板查询中的取值由 `getCommittedVariablesForPanel` 决定：

```typescript
const getCommittedVariablesForPanel = (panelId, tabId) => {
  const merged = [
    ...committedVariablesData.global,
    ...(tabId && committedVariablesData.tabs[tabId] ? committedVariablesData.tabs[tabId] : []),
    ...(committedVariablesData.panels[panelId] || []),
  ];
  return merged;
};
```

合并后的数组中，**同名变量后面的覆盖前面的**（panel > tab > global），因为 `usePanelVariableSubstitution` 中的 `currentDependentVariablesData` 按数组顺序遍历替换，后替换的值生效。

### 8.2 面板特定覆盖机制

面板刷新时创建的面板特定变量快照优先级最高：

```typescript
// RenderDashboardCharts.vue:1299-1312
const refreshPanelRequest = (panelId) => {
  // 1. 仅提交面板作用域
  variablesManager.commitScope("panels", panelId);

  // 2. 获取合并变量
  const panelVars = variablesManager.getVariablesForPanel(panelId, selectedTabId.value);

  // 3. 存储为面板特定覆盖
  currentVariablesDataRef.value[panelId] = {
    isVariablesLoading: false,
    values: panelVars,
  };
};
```

面板获取变量时的优先级：

```
1. currentVariablesDataRef.value[panelId]   ← 面板特定覆盖（最高优先级）
2. currentVariablesDataRef.value.__global    ← 默认全局提交状态
   └─ 内含 committedVariablesData.global + committedVariablesData.tabs[tabId]
       + committedVariablesData.panels[panelId]
```

### 8.3 VariablesValueSelector 的作用域查找

变量选择器在解析变量引用时构建 `resolvedVarLookup`，按覆盖优先级填充：

```typescript
const resolvedVarLookup = computed(() => {
  const lookup = {};
  if (useManager && manager) {
    // 优先级1（最低）: global 变量
    (manager.variablesData.global || []).forEach((v) => {
      lookup[v.name] = v.value;
    });
    // 优先级2: tab 变量（覆盖同名 global）
    if (props.tabId && manager.variablesData.tabs?.[props.tabId]) {
      manager.variablesData.tabs[props.tabId].forEach((v) => {
        lookup[v.name] = v.value;
      });
    }
    // 优先级3（最高）: 当前作用域变量（覆盖同名 global/tab）
    variablesData.values.forEach((v) => {
      lookup[v.name] = v.value;
    });
  }
  return lookup;
});
```

### 8.4 全局变量对下游作用域的级联覆盖

当 `loadFromUrl` 处理全局变量时，会**同步覆盖**所有 Tab 和 Panel 中的同名变量：

```typescript
// useVariablesManager.ts:900-924
if (parsed.scope === "global") {
  const variable = getVariable(parsed.name, "global");
  if (variable) {
    variable.value = parsedValue;
    variable.isVariablePartialLoaded = true;
  }

  // 同步到所有 Tab 同名变量
  Object.values(variablesData.tabs).forEach((tabVars) => {
    const tabVar = tabVars.find((v) => v.name === parsed.name);
    if (tabVar) {
      tabVar.value = parsedValue;
      tabVar.isVariablePartialLoaded = true;
    }
  });

  // 同步到所有 Panel 同名变量
  Object.values(variablesData.panels).forEach((panelVars) => {
    const panelVar = panelVars.find((v) => v.name === parsed.name);
    if (panelVar) {
      panelVar.value = parsedValue;
      panelVar.isVariablePartialLoaded = true;
    }
  });
}
```

### 8.5 各机制如何协同影响优先级

| 场景 | 协同效果 |
|------|---------|
| 全局变量 + Tab 同名变量 | Tab 变量覆盖全局变量的值 |
| 全局变量 + Panel 同名变量 | Panel 变量覆盖全局变量的值 |
| URL 中指定全局 var-* | 通过 `loadFromUrl` 向下覆盖所有 Tab/Panel 同名变量 |
| URL 中指定 Tab var-*.t.* | 仅覆盖该 Tab 的变量，不影响其他 Tab |
| 面板单独刷新 | 创建 `currentVariablesDataRef[panelId]` 覆盖，其他面板不受影响 |
| 父变量变更 | 递归重置所有后代（跨作用域），但仅触发可见且父变量就绪的子变量 |
| Tab 不可见 | 该 Tab 下变量不参与 `isLoading` 计算，不发 API，不影响面板 |

---

## 9. 值缓存与刷新策略

### 9.1 面板级别数据缓存

**存储方案**: IndexedDB 本地存储

```
Database: 'PanelCache'
└── Object Store: 'panels'
    ├── Key: 'folder_id:dashboard_id:panel_id'
    └── Value: {
        key: { panelSchema, variablesData, forceLoad, dashboardId, folderId },
        value: { data, loading, errorDetail, metadata, resultMetaData, annotations,
                 isPartialData, isOperationCancelled, lastTriggeredAt },
        cacheTimeRange: { start_time, end_time },
        timestamp: number
      }
```

### 9.2 缓存键计算

```typescript
const getCacheKey = () => ({
  panelSchema: toRaw(panelSchema.value),
  variablesData: JSON.parse(JSON.stringify([
    ...(getDependentVariablesData() || []),
    ...(getDynamicVariablesData() || []),
  ])),
  forceLoad: toRaw(forceLoad.value),
  dashboardId: toRaw(dashboardId?.value),
  folderId: toRaw(folderId?.value),
});
```

**缓存匹配时忽略的字段**:
- `panelSchema.version`, `panelSchema.layout`, `panelSchema.htmlContent`, `panelSchema.markdownContent`, `panelSchema.customChartResult`
- 变量运行时状态字段（`options`, `isLoading`, `isVariableLoadingPending`, `isVariablePartialLoaded`）

**缓存变量标准化** (仅保留影响查询结果的字段):
```typescript
const normalizeVariablesForCache = (variables) => {
  return variables.map((v) => ({
    name: v.name, type: v.type, value: v.value,
    scope: v.scope, multiSelect: v.multiSelect, query_data: v.query_data,
  }));
};
```

### 9.3 缓存命中条件

1. ✅ 缓存记录存在且非空
2. ✅ 标准化后的缓存键匹配
3. ✅ 首次加载（`runCount == 0`）
4. ✅ 非强制刷新（`forceLoad != true`）

### 9.4 面板数据加载完整等待链

```
loadData() 被调用
  ↓
1. waitForTimeout(50ms) — 防抖
  ↓
2. 首次加载？→ 尝试 restoreFromCache() — 命中则直接返回
  ↓
3. waitForThePanelToBecomeVisible() — IntersectionObserver
  ↓
4. waitForTheVariablesToLoad() — 面板依赖变量就绪
  ↓
5. 替换变量 → 执行查询 → 渲染 + 缓存保存
```

---

## 10. 完整传导链路图

```
┌─────────────────────────────────────────────────────────────────┐
│                        URL 参数层                                │
│  ?var-env=prod&var-region.t.tab1=us-east                       │
│         ↓                                                        │
│  ViewDashboard.vue 解析 initialVariableValues                   │
│  RenderDashboardCharts.vue 调用 loadFromUrl(route)              │
│         ↓                                                        │
│  useVariablesManager.loadFromUrl()                               │
│    ├─ parseVariableUrlKey() → 解析作用域                         │
│    ├─ parseValue() → 处理多选/动态过滤器                          │
│    ├─ 赋值 + 标记 isVariablePartialLoaded=true                  │
│    └─ global 变量级联覆盖 tab/panel 同名变量                     │
│         ↓                                                        │
│  variablesManager.commitAll() → committedVariablesData 更新     │
└─────────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────────┐
│                     变量管理器层                                  │
│  useVariablesManager                                             │
│    ├─ Live State (variablesData)                                │
│    │    ├─ global: VariableRuntimeState[]                       │
│    │    ├─ tabs: Record<string, VariableRuntimeState[]>         │
│    │    └─ panels: Record<string, VariableRuntimeState[]>       │
│    ├─ Committed State (committedVariablesData)                  │
│    ├─ Dependency Graph (DAG + 循环检测)                         │
│    ├─ Visibility: tabsVisibility, panelsVisibility              │
│    ├─ canVariableLoad() → 可见 + 非加载中 + 父变量就绪          │
│    └─ onVariablePartiallyLoaded() → 级联触发子变量              │
└─────────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────────┐
│                     变量选择器层                                  │
│  VariablesValueSelector                                          │
│    ├─ 按 scope 渲染对应变量 (global/tab/panel)                   │
│    ├─ Streaming API 获取 query_values 选项                       │
│    ├─ resolveVariableValue() → 解析父变量引用                    │
│    ├─ handleQueryValuesLogic() → 处理选项/默认值                 │
│    ├─ onVariablePartiallyLoaded() → 通知管理器                   │
│    └─ emitVariablesData() → 通知面板数据更新                    │
└─────────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────────┐
│                     面板数据加载层                                │
│  usePanelDataLoader                                              │
│    ├─ Watchers:                                                  │
│    │    ├─ watch([selectedTimeObj, forceLoad]) → loadData()     │
│    │    ├─ watch([panelSchema]) → 配置变更检测 → loadData()     │
│    │    └─ watch(variablesData.values) → variablesDataUpdated() │
│    ├─ 加载等待链:                                                │
│    │    1. waitForTimeout(50ms) 防抖                             │
│    │    2. restoreFromCache() 缓存恢复                          │
│    │    3. waitForThePanelToBecomeVisible() 可见性              │
│    │    4. waitForTheVariablesToLoad() 依赖就绪                 │
│    └─ 查询执行: replaceQueryValue() + applyDynamicVariables()   │
└─────────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────────┐
│                     变量替换层                                    │
│  usePanelVariableSubstitution                                    │
│    ├─ getDependentVariablesData() — 正则匹配查询中的变量引用      │
│    ├─ getDynamicVariablesData() — 提取动态过滤器                 │
│    ├─ variablesDataUpdated() — 四步变更检测                      │
│    ├─ ifPanelVariablesCompletedLoading() — 渐进式就绪检查        │
│    ├─ replaceQueryValue()                                        │
│    │    ├─ normalizeVariableSyntax()                             │
│    │    ├─ 固定变量: __interval, __range 等                      │
│    │    └─ 用户变量: 11种占位符 → 格式化值                       │
│    └─ applyDynamicVariables()                                    │
│         ├─ PromQL: addLabelToPromQlQuery()                      │
│         └─ SQL: addLabelsToSQlQuery()                           │
└─────────────────────────────────────────────────────────────────┘
         ↓
      最终查询 → HTTP Streaming API → 面板渲染 + IndexedDB 缓存
```

---

## 11. 关键设计总结

| 设计点 | 实现方式 | 优势 |
|--------|---------|------|
| **三级作用域** | global → tabs → panels 逐级覆盖 | 灵活控制变量影响范围，同名变量可精细隔离 |
| **两级状态** | Live + Committed 分离 | 用户修改变量不立即触发查询，避免请求风暴 |
| **渐进式自动提交** | 首次 isVariablePartialLoaded 变化时自动 commitAll | 无需用户手动刷新即可看到初始数据 |
| **依赖图解析** | 作用域感知的 DAG 构建 + 循环检测 | 正确处理级联查询顺序，防止死循环 |
| **可见性驱动** | tabsVisibility + panelsVisibility + IntersectionObserver | 不可见的 Tab/面板不发 API 请求，节省资源 |
| **渐进式加载** | 面板只等待自身依赖的变量就绪 | 提升首屏加载速度，不阻塞无关面板 |
| **URL 双向同步** | loadFromUrl + getUrlParams + 防护标志 | 刷新/分享/下钻均可正确恢复变量状态 |
| **面板级缓存** | IndexedDB 按面板存储 + 标准化缓存键 | 快速恢复仪表盘状态，减少重复请求 |
| **面板级刷新** | commitScope + currentVariablesDataRef 覆盖 | 细粒度控制刷新范围，不影响其他面板 |
| **空值级联传播** | 父变量空值递归置空子变量，不发 API | 减少无效请求，避免子变量加载无意义数据 |

---

## 12. 深度分析：SQL 动态过滤器注入是否按 stream 过滤

### 12.1 数据结构：动态过滤器的 streams 属性

每个动态过滤器条目包含 `streams` 数组字段，理论上用于标识该过滤器适用于哪些 stream：

**数据创建** (`VariableAdHocValueSelector.vue:94-103`):
```typescript
const addFields = () => {
  const adhocVariablesTemp = adhocVariables.value;
  adhocVariablesTemp.push({
    name: "",
    operator: operatorOptions[0],  // 默认 "="
    value: "",
    streams: [],                    // ← 预留了 streams 字段
  });
  emitValue();
};
```

单个动态过滤器的完整数据结构：
```typescript
{
  name: "host",          // 字段名
  operator: "=",         // 操作符（=, != 等）
  value: "server-01",    // 过滤值
  streams: [],           // 理论上应关联的 stream 列表，当前始终为空数组
}
```

**UI 提供的操作符** (`VariableAdHocValueSelector.vue:84`):
```typescript
const operatorOptions = ["=", "!="];
```
仅支持 `=` 和 `!=` 两种操作符。

### 12.2 SQL 动态过滤器注入的关键代码

**applyDynamicVariables** (`usePanelVariableSubstitution.ts:674-724`):
```typescript
const applyDynamicVariables = async (query: any, queryType: any) => {
  const adHocVariables = variablesData.value?.values
    ?.filter((it: any) => it.type === "dynamic_filters")
    ?.map((it: any) => it?.value)
    .flat()
    ?.filter((it: any) => it?.operator && it?.name && it?.value);

  if (!adHocVariables?.length) {
    return { query, metadata };
  }

  if (queryType === "sql") {
    const queryStream = await getStreamFromQuery(query);  // ← 已提取 stream

    const applicableAdHocVariables = adHocVariables;
    // ⚠️ 以下 stream 过滤逻辑已被注释掉！
    // .filter((it: any) => {
    //   return it?.streams?.find((it: any) => it.name == queryStream);
    // });

    applicableAdHocVariables.forEach((variable: any) => {
      metadata.push({
        type: "dynamicVariable",
        name: variable.name,
        value: variable.value,
        operator: variable.operator,
      });
    });
    query = await addLabelsToSQlQuery(query, applicableAdHocVariables);
  }
};
```

### 12.3 关键发现：stream 过滤被禁用

代码中**已经提取了查询的 stream 名称**（`getStreamFromQuery(query)`），但紧接着的 `.filter()` 调用被注释掉了：

```typescript
const queryStream = await getStreamFromQuery(query);   // ✅ 执行了 stream 提取

const applicableAdHocVariables = adHocVariables;
// .filter((it: any) => {                               // ❌ 过滤被注释掉
//   return it?.streams?.find((it: any) => it.name == queryStream);
// });
```

这意味着：**当前所有动态过滤器会无条件应用到所有 SQL 查询，不区分 stream。**

### 12.4 对比：PromQL 的处理方式

PromQL 的动态过滤器同样**不做 stream 过滤**：

```typescript
if (queryType === "promql") {
  adHocVariables.forEach((variable: any) => {
    query = addLabelToPromQlQuery(
      query,
      variable.name,
      variable.value,
      variable.operator,
    );
  });
}
```

PromQL 没有 stream 概念，所有 label 直接注入到查询表达式中。

### 12.5 SQL 注入的具体方式

**addLabelsToSQlQuery** (`sqlUtils.ts:35-87`) 使用 AST 级别的 WHERE 子句注入：

```typescript
export const addLabelsToSQlQuery = async (originalQuery, labels) => {
  // 步骤1: 构建一个包含所有动态过滤条件的 dummy 查询
  let dummyQuery = "select * from 'default'";
  for (let i = 0; i < labels.length; i++) {
    dummyQuery = await addLabelToSQlQuery(
      dummyQuery,
      labels[i].name,
      labels[i].value,
      labels[i].operator,
    );
  }

  // 步骤2: 解析原始查询和 dummy 查询的 AST
  const astOfOriginalQuery = parser.astify(originalQuery);
  const astOfDummy = parser.astify(dummyQuery);

  // 步骤3: 合并 WHERE 子句
  if (astOfOriginalQuery.where) {
    // 原查询有 WHERE → AND 连接
    const newWhereClause = {
      type: "binary_expr",
      operator: "AND",
      left: { ...astOfOriginalQuery.where, parentheses: true },
      right: { ...astOfDummy.where, parentheses: true },
    };
  } else {
    // 原查询无 WHERE → 直接使用 dummy 的 WHERE
  }
};
```

**注入示例**：

```
原始查询: SELECT host, count(*) FROM "logs" WHERE level='error'
动态过滤器: [{ name: "host", operator: "=", value: "server-01" }]

注入结果:
SELECT host, count(*) FROM "logs" WHERE (level='error') AND (host = 'server-01')
```

### 12.6 不按 stream 过滤的影响分析

| 影响维度 | 具体表现 |
|---------|---------|
| **正确性** | 当不同 stream 有同名字段但语义不同时，可能注入不相关的过滤条件，导致查询报错或返回空结果 |
| **性能** | 对不包含该字段的 stream 查询注入过滤条件，可能导致后端报错（字段不存在）或无效的全文扫描 |
| **多 stream 面板** | 一个面板的多个查询可能引用不同 stream，所有查询都会被注入相同的动态过滤器 |
| **JOIN 查询** | 动态过滤条件注入时**不指定表别名**（`table: null`），在 JOIN 查询中可能产生歧义 |
| **字段不存在** | 如果 stream 中没有动态过滤器指定的字段名，SQL 执行可能报错 |

**JOIN 查询中的歧义示例**：
```
原始查询:
  SELECT a.host, b.status FROM "stream_a" a JOIN "stream_b" b ON a.id = b.id

动态过滤器: { name: "host", operator: "=", value: "server-01" }

注入结果:
  SELECT a.host, b.status FROM "stream_a" a JOIN "stream_b" b
    ON a.id = b.id AND host = 'server-01'
    -- ❌ host 未指定表别名，数据库无法确定是 a.host 还是 b.host
```

### 12.7 stream 过滤被禁用的原因推测

1. **streams 数组始终为空**：`VariableAdHocValueSelector` 创建新条目时 `streams: []`，没有任何 UI 或逻辑来填充此字段
2. **过度过滤的风险**：如果启用 stream 过滤但 streams 为空，所有动态过滤器都会被过滤掉，导致功能完全失效
3. **简化实现**：当前实现选择"全部应用"而非"按 stream 精确匹配"，降低了实现复杂度

### 12.8 getStreamFromQuery 的 stream 提取逻辑

该函数虽然被调用了，但提取结果未被使用，仅做了一次无效计算：

**getStreamFromQuery** (`sqlUtils.ts:261-270`):
```typescript
export const getStreamFromQuery = async (query: any) => {
  await importSqlParser();
  try {
    const ast: any = parser.astify(query);
    return ast?.from[0]?.table || "";  // 取第一个 FROM 表名
  } catch (e: any) {
    return "";
  }
};
```

**局限性**：只取 `from[0].table`，对于 UNION、子查询、WITH 子句等复杂 SQL 仅返回第一个表名。

---

## 13. 深度分析：同名变量替换结果的确定机制——完整证据链

### 13.1 证据链概览

同名变量的最终替换结果由**三层机制**共同决定：

```
第一层：变量命名规范
  → 限制同名变量能否被创建

第二层：变量合并顺序
  → 决定同名变量进入替换函数时的数组位置

第三层：替换遍历顺序
  → 决定最终生效的值（后替换覆盖先替换）
```

### 13.2 第一层：变量命名规范与同名限制

#### 13.2.1 配置层面：同一 variablesConfig.list 中不允许同名

**addVariable 函数** (`commons.ts:343-381`):
```typescript
export const addVariable = async (store, dashboardId, variableData, folderId) => {
  const currentDashboard = await getDashboard(store, dashboardId, folderId);

  const variableExists = currentDashboard.variables.list.filter(
    (it) => it.name == variableData.name,
  );

  if (variableExists.length) {
    throw new Error("Variable with same name already exists");  // ← 抛出异常
  }

  currentDashboard.variables.list.push(variableData);
  return await updateDashboard(...);
};
```

**单元测试验证** (`commons.spec.ts:811-828`):
```typescript
it("should throw error when variable with same name exists", async () => {
  const variableData = { name: "var1", type: "query", query: "SELECT 2" };
  const mockDashboard = {
    variables: {
      showDynamicFilters: false,
      list: [{ name: "var1", type: "query", query: "SELECT 1" }],
    },
  };

  await expect(
    addVariable(mockStore, dashboardId, variableData, folderId)
  ).rejects.toThrow("Variable with same name already exists");
});
```

**AddPanel 模式下的客户端校验** (`AddSettingVariable.vue:1516-1527`):
```typescript
if (props.isFromAddPanel && props.dashboardVariablesList) {
  const isDuplicate = props.dashboardVariablesList.some(
    (v) => v.name === variableData.name && v.name !== props.variableName,
  );
  if (isDuplicate) {
    showErrorNotification(`Variable with same name already exists.`);
    return false;
  }
}
```

#### 13.2.2 运行时：展开后可产生同名变量实例

虽然 `variablesConfig.list` 中不允许同名，但**展开后**同一变量名可出现在多个作用域：

**expandVariablesForScopes** (`useVariablesManager.ts:82-176`):
```
变量配置: { name: "region", scope: "global" }
  → 展开: [{ name: "region", scope: "global" }]           // 1个实例

变量配置: { name: "region", scope: "tabs", tabs: ["tab1", "tab2"] }
  → 展开: [
      { name: "region", scope: "tabs", tabId: "tab1" },   // 实例1
      { name: "region", scope: "tabs", tabId: "tab2" },   // 实例2
    ]

变量配置: { name: "region", scope: "panels", panels: ["panel1"] }
  → 展开: [{ name: "region", scope: "panels", panelId: "panel1" }]
```

**关键点**：如果配置了两个不同变量都叫 `region`（一个 global，一个 tabs），这在配置层面**不会**被阻止（因为它们是不同的变量配置项，只是恰好同名）。但这种情况在正常使用中很少见，因为 `addVariable` 检查的是整个 list 中的 name 唯一性。

**更常见的情况**：同一个变量因作用域展开而出现在合并数组中。例如 `region` 变量配置为 `scope: "global"`，但面板查询时通过 `getCommittedVariablesForPanel` 合并时，global 实例和 tab 实例（如果存在同名 tab 变量）会同时出现在合并数组中。

#### 13.2.3 变量标识键（getVariableKey）

运行时使用 `getVariableKey` 唯一标识每个变量实例：

```typescript
export const getVariableKey = (name, scope, tabId?, panelId?) => {
  if (scope === "global")   return `${name}@global`;
  if (scope === "tabs")     return `${name}@tab@${tabId}`;
  if (scope === "panels")   return `${name}@panel@${panelId}`;
};
```

示例：
- `region@global` — 全局的 region 变量
- `region@tab@tab1` — tab1 的 region 变量
- `region@panel@panel1` — panel1 的 region 变量

这三个是**完全独立的运行时实例**，但拥有相同的 `name` 属性。

### 13.3 第二层：变量合并顺序

#### 13.3.1 getCommittedVariablesForPanel 的合并

**代码** (`useVariablesManager.ts:835-848`):
```typescript
const getCommittedVariablesForPanel = (panelId, tabId) => {
  const merged = [
    ...committedVariablesData.global,                              // 位置: 最前
    ...(committedVariablesData.tabs[tabId] || []),                 // 位置: 中间
    ...(committedVariablesData.panels[panelId] || []),             // 位置: 最后
  ];
  return merged;
};
```

**合并顺序**：`global → tab → panel`

当存在同名变量时，合并数组中会出现多个 `name` 相同但 `value` 不同的元素：

```
合并结果示例（假设存在同名变量 region）：
[
  { name: "env",     value: "prod",   scope: "global" },
  { name: "region",  value: "us",     scope: "global" },    // global region
  { name: "region",  value: "eu",     scope: "tabs" },      // tab region（覆盖 global）
  { name: "host",    value: "srv1",   scope: "panels" },
  { name: "region",  value: "ap",     scope: "panels" },    // panel region（覆盖 tab）
]
```

#### 13.3.2 getVariablesForPanel 的 Live 状态合并

**代码** (`useVariablesManager.ts:817-829`):
```typescript
const getVariablesForPanel = (panelId, tabId) => {
  const merged = [
    ...variablesData.global,
    ...(variablesData.tabs[tabId] || []),
    ...(variablesData.panels[panelId] || []),
  ];
  return merged;
};
```

与 committed 版本完全相同的合并顺序。

#### 13.3.3 resolvedVarLookup 的合并（VariablesValueSelector 内部）

**代码** (`VariablesValueSelector.vue:247-266`):
```typescript
const resolvedVarLookup = computed(() => {
  const lookup = {};
  if (useManager && manager) {
    // 1. 先填 global
    (manager.variablesData.global || []).forEach((v) => {
      lookup[v.name] = v.value;        // 写入
    });
    // 2. 再填 tab（覆盖同名 global）
    if (props.tabId && manager.variablesData.tabs?.[props.tabId]) {
      manager.variablesData.tabs[props.tabId].forEach((v) => {
        lookup[v.name] = v.value;      // 覆盖
      });
    }
    // 3. 最后填当前作用域（覆盖同名 global/tab）
    variablesData.values.forEach((v) => {
      lookup[v.name] = v.value;        // 最终覆盖
    });
  }
  return lookup;
});
```

**这里采用了 HashMap 语义**：同名 key 后赋值覆盖先赋值，天然实现 `panel > tab > global` 的优先级。

### 13.4 第三层：替换遍历顺序

#### 13.4.1 replaceQueryValue 的遍历

**代码** (`usePanelVariableSubstitution.ts:547-666`):
```typescript
if (currentDependentVariablesData?.length) {
  currentDependentVariablesData?.forEach((variable) => {
    // 遍历合并后的变量数组
    // 每个变量执行 11 种占位符替换
    query = query.replaceAll(placeHolder, value);
  });
}
```

**关键机制**：`replaceAll` 是幂等操作。对于同名变量，**后遍历的变量会覆盖先遍历的结果**。

**推理示例**：

假设合并数组中有两个同名变量：
```
currentDependentVariablesData = [
  { name: "region", value: "us" },     // global → 先遍历
  { name: "region", value: "eu" },     // tab → 后遍历
]
```

原始查询：`SELECT * FROM logs WHERE region = '$region'`

```
步骤1: 遍历 global region (value="us")
  query = "SELECT * FROM logs WHERE region = 'us'"

步骤2: 遍历 tab region (value="eu")
  query = "SELECT * FROM logs WHERE region = 'eu'"   ← 覆盖

最终结果: region = 'eu'  (tab 值生效)
```

#### 13.4.2 getDependentVariablesData 的变量提取

**代码** (`usePanelVariableSubstitution.ts:87-98`):
```typescript
const getDependentVariablesData = () =>
  variablesData.value?.values
    ?.filter((it) => it.type != "dynamic_filters")  // 排除动态过滤器
    ?.filter((it) => {
      // 仅保留在面板查询中被引用的变量
      const regexForVariable = new RegExp(
        `(?:\\$\\{?\\s*${it.name}\\s*(?::\\s*(?:csv|pipe|doublequote|singlequote)\\s*)?\\}?)|(?:\\{\\{\\s*${it.name}\\s*(?::\\s*(?:csv|pipe|doublequote|singlequote)\\s*)?\\}\\})`,
      );
      return panelSchema.value.queries
        ?.map((q) => regexForVariable.test(q?.query))
        ?.includes(true);
    });
```

此函数**不去重**。如果 `variablesData.values` 中有多个同名变量，且查询中引用了该变量名，则所有同名变量实例都会被保留。

#### 13.4.3 面板特定覆盖的优先级

面板刷新时，`currentVariablesDataRef[panelId]` 存储了面板特定的变量快照：

**RenderDashboardCharts.vue 面板获取变量**：
```typescript
const getMergedVariablesForPanel = (panelId) => {
  // 最高优先级：面板特定覆盖
  if (currentVariablesDataRef.value?.[panelId]) {
    return currentVariablesDataRef.value[panelId];
  }
  // 默认：全局提交状态
  return currentVariablesDataRef.value.__global;
};
```

面板特定覆盖已经是一个合并后的数组（global+tab+panel），不存在额外的同名问题。

### 13.5 完整证据链：同名变量从配置到最终替换的全路径

```
[配置层]
  variablesConfig.list 中不允许同名变量
  → addVariable() 抛出 "Variable with same name already exists"
  → 但同一变量可因 scope 展开而产生多实例

      ↓ 展开后

[运行时存储层]
  variablesData.global  = [{ name: "region", value: "us", scope: "global" }]
  variablesData.tabs["tab1"] = [{ name: "region", value: "eu", scope: "tabs" }]
  variablesData.panels["p1"] = [{ name: "region", value: "ap", scope: "panels" }]

      ↓ 合并时

[合并层] getCommittedVariablesForPanel("p1", "tab1")
  合并顺序: global → tab → panel
  结果: [
    { name: "region", value: "us" },   // [0] global
    { name: "region", value: "eu" },   // [1] tab
    { name: "region", value: "ap" },   // [2] panel
  ]
  ← 数组中三个同名变量，index 递增

      ↓ 提取依赖变量时

[依赖提取层] getDependentVariablesData()
  不去重，三个同名变量全部保留
  currentDependentVariablesData = [region:us, region:eu, region:ap]

      ↓ 遍历替换时

[替换层] replaceQueryValue()
  forEach 顺序遍历:
    第1次: $region → 'us'
    第2次: $region → 'eu'    ← 覆盖
    第3次: $region → 'ap'    ← 最终覆盖

  最终结果: 'ap' (panel 值生效)

      ↓ 同一机制在 VariablesValueSelector 中

[变量解析层] resolvedVarLookup
  HashMap 语义，后赋值覆盖先赋值:
    lookup["region"] = "us"    // global
    lookup["region"] = "eu"    // tab 覆盖
    lookup["region"] = "ap"    // panel 最终覆盖

  最终结果: "ap" (panel 值生效)
```

### 13.6 特殊场景分析

#### 场景1：URL 全局变量对同名 Tab/Panel 变量的影响

**loadFromUrl** (`useVariablesManager.ts:878-943`):
```typescript
if (parsed.scope === "global") {
  // 设置 global 变量值
  globalVar.value = parsedValue;
  globalVar.isVariablePartialLoaded = true;

  // 同步覆盖所有 tab 同名变量
  Object.values(variablesData.tabs).forEach((tabVars) => {
    const tabVar = tabVars.find((v) => v.name === parsed.name);
    if (tabVar) {
      tabVar.value = parsedValue;           // ← 直接覆盖 tab 值
      tabVar.isVariablePartialLoaded = true;
    }
  });

  // 同步覆盖所有 panel 同名变量
  Object.values(variablesData.panels).forEach((panelVars) => {
    const panelVar = panelVars.find((v) => v.name === parsed.name);
    if (panelVar) {
      panelVar.value = parsedValue;         // ← 直接覆盖 panel 值
      panelVar.isVariablePartialLoaded = true;
    }
  });
}
```

**效果**：URL 中的 `var-region=apac` 会将 global、所有 tab、所有 panel 中的 `region` 变量统一设为 `apac`。合并后虽然仍有三个实例，但值相同，替换结果不受遍历顺序影响。

**设计意图**：下钻场景中，用户通过 URL 传递变量值，期望所有作用域统一使用该值。

#### 场景2：dynamic_filters 不参与常规替换

**代码** (`usePanelVariableSubstitution.ts:53, 89`):
```typescript
// 初始化时过滤
.filter((it) => it.type != "dynamic_filters")

// 获取时过滤
const getDependentVariablesData = () =>
  variablesData.value?.values
    ?.filter((it) => it.type != "dynamic_filters")
```

动态过滤器不参与 `replaceQueryValue` 遍历，而是通过独立的 `applyDynamicVariables` 函数注入 WHERE 子句。因此即使 dynamic_filters 变量与常规变量同名，也不会产生替换冲突。

#### 场景3：VariablesValueSelector 中的变量解析跳过 dynamic_filters

**代码** (`VariablesValueSelector.vue:2058-2060`):
```typescript
for (const variable of variablesToResolve) {
  // Skip dynamic_filters as they don't participate in standard variable replacement
  if (variable.type === "dynamic_filters") continue;
  // ...
}
```

在 `resolveVariableValue`（用于解析 `query_values` 变量的 stream/field/filter 中的变量引用）时也跳过 `dynamic_filters`。

### 13.7 同名变量优先级总结

| 阶段 | 机制 | 优先级规则 | 代码位置 |
|------|------|-----------|---------|
| 配置 | `addVariable` 同名检查 | 同一 list 禁止同名 | `commons.ts:361-366` |
| 合并 | `getCommittedVariablesForPanel` | global → tab → panel 数组拼接 | `useVariablesManager.ts:839-845` |
| 解析(HashMap) | `resolvedVarLookup` | 后赋值覆盖（panel > tab > global） | `VariablesValueSelector.vue:247-266` |
| 替换(遍历) | `replaceQueryValue` forEach | 后替换覆盖（panel > tab > global） | `usePanelVariableSubstitution.ts:547-666` |
| URL恢复 | `loadFromUrl` global 级联 | 统一所有作用域同名变量值 | `useVariablesManager.ts:900-924` |
| 面板刷新 | `currentVariablesDataRef[panelId]` | 面板特定覆盖最高优先级 | `RenderDashboardCharts.vue` |

**核心结论**：两种不同实现（HashMap 和数组遍历）最终都实现了 `panel > tab > global` 的优先级，但实现方式不同——HashMap 依靠 key 覆盖语义，数组遍历依靠后替换覆盖前替换。这两种方式在**绝大多数情况**下结果一致，但在**极端场景**下（如 URL global 级联覆盖后 tab 值被修改）可能产生微妙差异。
