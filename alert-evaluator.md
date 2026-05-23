# 告警评估器对接通知投递通道链路分析

## 1. 整体架构概览

告警评估器的完整链路由以下核心模块组成：

```
调度器触发 → 告警评估 → [分组批处理] 或 [历史去重] → 事件关联(可选) → 模板渲染 → 通知投递
```

> **重要修正**：分组批处理和历史去重是**互斥**的分支关系，而非顺序关系。分组启用时**不会**执行去重。

主要涉及的代码文件：

| 模块 | 文件路径 | 核心功能 |
|------|---------|---------|
| 告警评估核心 | `src/service/alerts/alert.rs` | 告警评估、通知发送入口、模板渲染 |
| 告警调度器 | `src/service/alerts/scheduler/handlers.rs` | 触发处理、评估窗口管理、执行流程编排 |
| 告警分组 | `src/service/alerts/grouping.rs` | 告警批处理、抑制策略应用 |
| 历史去重 | `src/service/alerts/deduplication.rs` | 指纹计算、去重状态管理 |
| 事件关联 | `src/service/alerts/incidents.rs` | 告警事件关联、事故聚合 |
| 模板管理 | `src/service/alerts/templates.rs` | 模板CRUD、系统模板初始化 |
| 去重配置 | `src/config/src/meta/alerts/deduplication.rs` | 去重配置数据结构 |
| 告警配置 | `src/config/src/meta/alerts/alert.rs` | 告警数据结构定义 |
| 去重状态实体 | `src/infra/src/table/entity/alert_dedup_state.rs` | 去重状态数据库实体 |
| 告警管理器 | `src/job/alert_manager.rs` | 服务启动、定时任务管理 |

---

## 2. 评估窗口实现

### 2.1 评估窗口计算逻辑

评估窗口在调度器处理器中计算，核心位于 `handlers.rs:266-330` 的 `get_skipped_timestamps` 函数。

#### 时间窗口计算方式：

```rust
// 起始时间 = 最终结束时间 - 告警周期(分钟)
let start_time = final_end_time - Duration::try_minutes(alert.trigger_condition.period)
    .unwrap().num_microseconds().unwrap();
```

#### 支持两种调度模式：

1. **Cron 调度模式**：基于 cron 表达式计算下次执行时间
   - 参考：`handlers.rs:278-298`
   - 支持时区偏移（`tz_offset` 分钟）

2. **固定频率模式**：基于 `frequency`（秒）计算执行间隔
   - 参考：`handlers.rs:299-315`
   - 支持时间对齐（`align_time`）

#### 延迟处理机制（修正）：

> **重要修正**：`_get_max_considerable_delay()` 函数（`handlers.rs:333-344`）定义了 `min(1小时, 频率 × 20%)` 的最大可接受延迟逻辑，但**该函数从未被实际调用**，是死代码。

**实际的跳过逻辑**（`handlers.rs:629-692`）：
- 只要 `delay > frequency`（延迟超过一个执行周期），就会跳过中间的执行点
- 跳过的时间戳通过 `get_skipped_timestamps()` 计算并记录到 `triggers` 使用率流中
- 跳过的告警会记录 `skipped_alerts_count` 指标
- 最终使用的 `final_end_time` 是跳过所有积压后的最新时间点

```rust
// 只要 next_run_at <= supposed_to_run_at + delay，就会被跳过
while next_run_at <= supposed_to_run_at + delay {
    skipped_timestamps.push(next_run_at);
    // 计算下一个执行时间...
}
```

### 2.2 评估窗口参数

| 参数 | 位置 | 说明 |
|------|------|------|
| `period` | `TriggerCondition.period` | 数据回溯时间窗口（分钟） |
| `frequency` | `TriggerCondition.frequency` | 评估执行频率（秒） |
| `cron` | `TriggerCondition.cron` | Cron 调度表达式 |
| `tz_offset` | `Alert.tz_offset` | 时区偏移（分钟） |
| `align_time` | `TriggerCondition.align_time` | 是否对齐时间边界 |
| `multi_time_range` | `QueryCondition.multi_time_range` | 多时间范围查询 |

### 2.3 告警评估执行

告警评估入口位于 `alert.rs:1012-1040` 的 `AlertExt::evaluate` 方法：

```rust
async fn evaluate(
    &self,
    row: Option<&Map<String, Value>>,
    (start_time, end_time): (Option<i64>, i64),
    trace_id: Option<String>,
) -> Result<TriggerEvalResults, anyhow::Error> {
    if self.is_real_time {
        // 实时告警评估
        self.query_condition.evaluate_realtime(row).await
    } else {
        // 调度告警评估
        self.query_condition.evaluate_scheduled(
            &self.org_id,
            Some(&self.stream_name),
            self.stream_type,
            &self.trigger_condition,
            (start_time, end_time),
            // ...
        ).await
    }
}
```

---

## 3. 抑制策略实现

### 3.1 静默期抑制（Silence）

静默期抑制在评估完成后应用，位于 `handlers.rs:827-834`：

```rust
if trigger_results.data.is_some() && alert.trigger_condition.silence > 0 {
    new_trigger.next_run_at = alert.trigger_condition.get_next_trigger_time(
        true, 
        alert.tz_offset, 
        true,  // is_silenced = true
        None
    )?;
    new_trigger.is_silenced = true;
    should_store_last_end_time = false;
}
```

- `silence` 字段定义静默期时长
- 静默期内告警不会被重新评估
- 静默期结束后自动恢复评估

### 3.2 分组批处理抑制（Grouping）

分组批处理是企业版功能，核心位于 `grouping.rs`：

#### 分组配置（`deduplication.rs:148-166`）：

```rust
pub struct GroupingConfig {
    pub enabled: bool,                    // 启用分组
    pub max_group_size: usize,           // 最大批大小（默认100）
    pub send_strategy: SendStrategy,     // 发送策略
    pub group_wait_seconds: i64,         // 等待时长（默认30秒）
}

pub enum SendStrategy {
    FirstWithCount,  // 仅发送第一条 + 计数
    Summary,         // 发送聚合摘要
    All,             // 发送全部
}
```

#### 分组与去重的分支关系（重要修正）：

> **关键修正**：分组和历史去重是**互斥**的分支关系，执行顺序如下：
> 
> 1. 先检查 `grouping.enabled`（`handlers.rs:851-964`）
> 2. **如果分组启用**：
>    - 计算指纹（复用去重的指纹算法）
>    - 添加到批次（`add_to_batch()`）
>    - **直接 return，不会执行后续的去重逻辑**
> 3. **只有分组不启用时**，才会检查 `deduplication.enabled` 并执行去重

#### 分组工作流程（`handlers.rs:851-964`）：

```rust
if grouping_enabled {
    // 计算指纹（与去重共用算法）
    let fingerprint = calculate_fingerprint(alert, first_row, ...);
    
    // 添加到批次
    let batch_ready = add_to_batch(fingerprint, ...);
    
    if batch_ready {
        // 批次满，立即发送
        send_grouped_notification_sync(batch).await;
    }
    
    // 标记分组状态
    trigger_data_stream.grouped = Some(true);
    
    // 直接返回，不执行后续的去重和通知逻辑
    return Ok(());
}
// 只有分组不启用，才会走到这里的去重逻辑
```

#### 待处理批次管理：

使用内存中的 `DashMap` 存储待处理批次，键为指纹，值为 `PendingBatch`：

```rust
// grouping.rs:28-30
static PENDING_BATCHES: Lazy<Arc<DashMap<String, PendingBatch>>> = 
    Lazy::new(|| Arc::new(DashMap::new()));
```

#### 批次触发条件：

1. **批次满**：添加新告警后达到 `max_group_size`，立即发送
2. **超时**：通过定时任务扫描 `get_expired_batches()`，超过 `group_wait_seconds` 后发送

### 3.3 事件关联抑制（Incident Correlation）

当 `alert.creates_incident = true` 时启用，位于 `handlers.rs:1051-1095`：

#### 事故关联只取首条结果行的影响（重要修正）：

> **关键修正**：`correlate_alert_to_incident()` 函数签名如下：
> ```rust
> pub async fn correlate_alert_to_incident(
>     alert: &Alert,
>     result_row: &Map<String, Value>,    // 只取 first_row
>     notify_rows: &[Map<String, Value>], // 所有结果行
>     triggered_at: i64,
> ) -> Result<Option<IncidentCorrelationOutcome>, anyhow::Error>
> ```

**实际使用方式**（`handlers.rs:1051-1062`）：
```rust
let incident_handled_notification = if alert.creates_incident
    && ...
    && let Some(first_row) = data.first()  // 只取第一条结果行
{
    correlate_alert_to_incident(
        &alert,
        first_row,      // 只用第一行提取维度
        &data,          // 所有行仅用于判断非空
        triggered_at,
    ).await
}
```

**对结果行维度的处理**（`incidents.rs:489-500`）：
```rust
// 只从 first_row 提取标签用于关联
let mut labels: HashMap<String, String> = result_row
    .iter()
    .filter_map(|(k, v)| {
        // 转换为字符串...
        Some((k.clone(), value_str))
    })
    .collect();
```

**业务影响**：
1. **维度丢失风险**：一次评估返回多个结果行时，只有第一行的维度用于事故关联，其他行的维度被完全忽略
2. **错误聚合风险**：如果不同结果行有不同的服务标签或语义维度，只有第一行的维度决定了事故归属
3. **通知内容限制**：事故通知负载**不包含任何告警结果行数据**，只有事故元数据（severity, title, service_name 等）
4. **`notify_rows` 参数的实际作用**：仅用于 `!notify_rows.is_empty()` 判断，从未用于内容提取或维度计算

#### 抑制规则（`incidents.rs:630-656`）：

```rust
if !notify_rows.is_empty() {
    match &outcome {
        IncidentCorrelationOutcome::NewIncidentCreated { .. } |
        IncidentCorrelationOutcome::NewAlertTypeJoined { .. } => {
            // 新事故或新告警类型加入 → 发送通知
            send_incident_notifications(...).await;
        }
        IncidentCorrelationOutcome::ExistingAlertRepeated { .. } => {
            // 已有告警类型重复触发 → 抑制通知
            log::debug!("Suppressing notification for repeated alert type");
        }
    }
}
```

#### 关联优先级（`incidents.rs:141-220`）：

1. **服务发现（Service Discovery）**：最高优先级，通过 `service_streams` 匹配服务
2. **语义提取（Semantic Extraction）**：中优先级，基于 `distinguish_by` 配置提取维度
3. **告警ID隔离（AlertId Fallback）**：最低优先级，按告警ID单独隔离

---

## 4. 模板渲染实现

### 4.1 模板体系结构

模板分为两级：

1. **告警级模板**（`Alert.template`）：优先级最高，作用于所有目的地
2. **目的地级模板**（`Destination.template`）：单个目的地使用

模板优先级逻辑位于 `alert.rs:1078-1098`：

```rust
let template = match (&alert_template, &dest_template) {
    (Some(alert_tpl), _) => alert_tpl,        // 告警级优先
    (None, Some(dest_tpl)) => dest_tpl,       // 目的地级次之
    (None, None) => { /* 报错 */ }            // 必须配置模板
};
```

### 4.2 系统预置模板

系统预置模板在 `templates.rs:108-189` 的 `ensure_system_templates()` 中初始化：

```rust
let prebuilt_types = vec![
    "slack", "msteams", "pagerduty", "discord", 
    "webhook", "opsgenie", "servicenow", "email"
];
```

使用分布式锁确保多实例环境下只初始化一次。

### 4.3 模板渲染流程

模板渲染分为两步，位于 `alert.rs:1160-1225`：

#### 步骤1：行模板渲染（Row Template）

处理 `Alert.row_template`，为每条结果行生成渲染内容：

```rust
// alert.rs:1389-1511
fn process_row_template(
    org_name: &str,
    tpl: &String,
    alert: &Alert,
    row_type: RowTemplateType,
    rows: &[Map<String, Value>],
) -> Vec<Value>
```

支持的变量替换：
- 行字段变量：`{field_name}`
- 系统变量：`{org_name}`, `{stream_name}`, `{alert_name}`, `{alert_type}`
- 告警条件：`{alert_period}`, `{alert_operator}`, `{alert_threshold}`, `{alert_count}`
- 时间变量：`{alert_start_time}`, `{alert_end_time}`
- 上下文属性：`{context_attribute_name}`
- PromQL 特定：`{alert_promql_operator}`, `{alert_promql_value}`

支持两种输出类型：
- `RowTemplateType::String`：字符串输出
- `RowTemplateType::Json`：JSON 结构化输出

#### 步骤2：目的地模板渲染（Destination Template）

处理 `Template.body`，整合所有行数据生成最终消息：

```rust
// alert.rs:1520-...
async fn process_dest_template(
    org_name: &str,
    tpl: &str,
    alert: &Alert,
    rows: &[Map<String, Value>],
    rows_tpl_val: &[Value],
    options: ProcessTemplateOptions,
) -> String
```

#### 邮件标题单独渲染：

对于 `TemplateType::Email` 类型，标题也需要单独渲染：

```rust
// alert.rs:1201-1218
let email_subject = if let TemplateType::Email { title } = &template.template_type {
    process_dest_template(..., title, ...).await
} else {
    template.name.clone()
};
```

### 4.4 变量替换实现

核心变量替换函数 `process_variable_replace` 位于 `alert.rs` 中（未在截取部分显示），处理 `{var}` 格式的占位符替换。

---

## 5. 历史去重实现

### 5.1 去重配置层级

去重配置分为两层：

#### 组织级全局配置（`GlobalDeduplicationConfig`）：

```rust
// deduplication.rs:55-102
pub struct GlobalDeduplicationConfig {
    pub enabled: bool,                      // 全局启用开关
    pub alert_dedup_enabled: bool,          // 跨告警去重
    pub alert_fingerprint_groups: Vec<String>,  // 跨告警指纹维度
    pub time_window_minutes: Option<i32>,   // 默认时间窗口
    pub upgrade_window_minutes: u64,        // 层级升级窗口
}
```

#### 告警级配置（`DeduplicationConfig`）：

```rust
// deduplication.rs:113-140
pub struct DeduplicationConfig {
    pub enabled: bool,                      // 启用开关
    pub fingerprint_fields: Vec<String>,    // 指纹字段（空则自动检测）
    pub time_window_minutes: Option<i32>,   // 时间窗口（None则为 2×频率）
    pub grouping: Option<GroupingConfig>,   // 分组配置
}
```

### 5.2 指纹计算

指纹计算委托给企业版实现，位于 `deduplication.rs:34-48`：

```rust
pub fn calculate_fingerprint(
    alert: &Alert,
    result_row: &Map<String, Value>,
    config: &DeduplicationConfig,
    org_config: Option<&GlobalDeduplicationConfig>,
    semantic_groups: &[FieldAlias],
) -> String
```

#### 自动检测字段规则：

- `Custom` 查询类型：从查询条件中提取字段
- `SQL` 查询类型：使用 GROUP BY 列
- `PromQL` 查询类型：使用所有标签维度

#### 跨告警去重：

当 `alert_dedup_enabled = true` 时，使用 `alert_fingerprint_groups` 配置的语义组生成跨告警指纹，不同告警规则的相同维度值会被去重。

### 5.3 去重状态存储

去重状态存储在 `alert_dedup_state` 表中，实体定义位于 `alert_dedup_state.rs:20-40`：

```rust
pub struct Model {
    pub fingerprint: String,           // 主键：指纹哈希
    pub alert_id: String,              // 关联告警ID
    pub org_id: String,                // 组织ID
    pub first_seen_at: i64,            // 首次出现时间（微秒）
    pub last_seen_at: i64,             // 最后出现时间（微秒）
    pub occurrence_count: i64,         // 出现次数
    pub notification_sent: bool,       // 是否已发送通知（死字段，从未使用）
    pub created_at: i64,               // 创建时间
}
```

### 5.4 去重状态字段的实际使用（重要修正）：

> **关键修正**：`notification_sent` 字段是**死字段**，从未被实际使用。

**字段使用分析**：
1. **`notification_sent`**：
   - 新建时设置为 `false`（`deduplication.rs:92`）
   - 读取时**从未被访问**（`get_dedup_state()` 只用于存在性检查和时间窗口判断）
   - 更新时**从未被修改**（`save_dedup_state()` 只更新 `last_seen_at` 和 `occurrence_count`）
   - 去重判断逻辑**完全不依赖此字段**

2. **实际使用的字段**：
   - `fingerprint`：主键，用于查询
   - `last_seen_at`：用于时间窗口判断（`is_within_window()`）
   - `occurrence_count`：记录出现次数，用于指标统计
   - `alert_id`：用于区分同告警/跨告警去重类型

**`save_dedup_state` 更新逻辑**（`deduplication.rs:76-81`）：
```rust
if let Some(existing) = get_dedup_state(db, params.fingerprint).await? {
    let mut active: alert_dedup_state::ActiveModel = existing.clone().into();
    active.last_seen_at = Set(params.last_seen_at);
    active.occurrence_count = Set(params.occurrence_count);
    // 注意：notification_sent 从未被更新！
    active.update(db).await
}
```

### 5.5 去重应用流程

去重应用入口位于 `deduplication.rs:160-191` 的 `apply_deduplication`：

```rust
pub async fn apply_deduplication(
    db: &DatabaseConnection,
    alert: &Alert,
    result_rows: Vec<Map<String, Value>>,
) -> Result<(Vec<Map<String, Value>>, bool), sea_orm::DbErr>
```

#### 去重逻辑（`deduplication.rs:194-313`）：

```
对于每条结果行：
1. 计算指纹 fingerprint
2. 查询去重状态表
3. 若存在且在时间窗口内（通过 last_seen_at 判断）：
   - 更新 occurrence_count + 1
   - 更新 last_seen_at = now
   - 标记为抑制（不加入结果集）
   - 记录指标 ALERT_DEDUP_SUPPRESSED_TOTAL
4. 若不存在或超出窗口：
   - 创建新的去重状态记录（notification_sent = false）
   - 加入结果集（发送通知）
   - 记录指标 ALERT_DEDUP_PASSED_TOTAL
```

#### 时间窗口判断：

```rust
// deduplication.rs:100-105
pub fn is_within_window(state: &Model, time_window_minutes: i64) -> bool {
    o2_enterprise::enterprise::alerts::dedup::is_within_time_window(
        state.last_seen_at,  // 只使用 last_seen_at，不使用 notification_sent
        time_window_minutes,
    )
}
```

### 5.6 去重状态清理

定时清理任务位于 `alert_manager.rs:100-185`：

- 每小时执行一次（3600秒）
- 清理超过24小时的去重状态记录
- 企业版特性，OSS 版本为 no-op

### 5.7 去重全抑制时跳过事故关联（重要补充）

> **关键发现**：去重全抑制时（所有结果行都被抑制），**直接 return，不会进入事故关联逻辑**，也不会发送任何通知。

**代码逻辑**（`handlers.rs:977-999`）：
```rust
Ok((deduplicated_data, deduplicated)) => {
    if deduplicated_data.is_empty() && deduplicated {
        // 所有结果行都被去重抑制
        log::debug!(
            "[SCHEDULER trace_id {scheduler_trace_id}] All alert results deduplicated for org: {}, module_key: {}",
            &new_trigger.org,
            &new_trigger.module_key
        );

        // 标记抑制状态
        trigger_data_stream.dedup_enabled = Some(true);
        trigger_data_stream.dedup_suppressed = Some(true);

        // 更新触发器时间
        trigger_data.period_end_time = if should_store_last_end_time {
            Some(trigger_results.end_time)
        } else {
            None
        };
        new_trigger.data = json::to_string(&trigger_data).unwrap();
        db::scheduler::update_trigger(new_trigger, true, &query_trace_id).await?;
        publish_triggers_usage(trigger_data_stream);
        
        // ⚠️ 直接 return，跳过后续的事故关联和通知！
        return Ok(());
    }
    deduplicated_data
}
```

**执行流程分析**：

```
去重前 data 有 N 条结果行
    │
    ▼
apply_deduplication() 处理每条结果行
    │
    ▼
结果：deduplicated_data.is_empty() && deduplicated == true
    │
    ├─ 是（全抑制）→ 更新触发器状态 → return Ok(())
    │           （跳过事故关联、跳过通知）
    │
    └─ 否（部分/全部通过）→ 继续执行后续逻辑
                │
                ▼
            事故关联检查（handlers.rs:1051+）
                │
                ▼
            直接发送通知（handlers.rs:1138+）
```

**对事故关联的影响**：
1. 去重全抑制时，即使 `creates_incident = true`，也**不会调用** `correlate_alert_to_incident()`
2. 不会创建新事故，不会更新已有事故的告警计数
3. 不会发送任何事故通知
4. 去重状态表仍会更新（`occurrence_count` 和 `last_seen_at`）

**业务影响**：
- 如果告警结果行在去重窗口内重复出现，事故系统不会感知到这些重复触发
- 事故的严重级别升级逻辑不会被触发
- 可能导致事故系统中的告警计数与实际触发次数不一致

### 5.8 指标监控

去重相关 Prometheus 指标：

- `ALERT_DEDUP_SUPPRESSED_TOTAL`：被抑制的告警数（标签：org, alert, type）
- `ALERT_DEDUP_PASSED_TOTAL`：通过去重的告警数（标签：org, alert）
- `ALERT_GROUPING_BATCHES_PENDING`：待处理分组批次数（标签：org）

---

## 6. 通知投递通道

### 6.1 通知发送入口

通知发送入口位于 `alert.rs:1042-1141` 的 `AlertExt::send_notification`：

```rust
async fn send_notification(
    &self,
    rows: &[Map<String, Value>],
    rows_end_time: i64,
    start_time: Option<i64>,
    evaluation_timestamp: i64,
) -> Result<(String, String), AlertError>
```

#### 遍历目的地列表：

```rust
for dest_name in self.destinations.iter() {
    // 获取目的地及关联模板
    let (dest, dest_template) = destinations::get_with_template(&self.org_id, dest_name).await?;
    
    // 选择模板（告警级优先）
    let template = match (&alert_template, &dest_template) { ... };
    
    // 发送通知
    match send_notification(self, &destination_type, template, rows, ...).await {
        Ok(resp) => success_message.push_str(...),
        Err(e) => err_message.push_str(...),
    }
}
```

#### 错误处理策略：

- 部分目的地失败不会导致整体失败
- 仅当所有目的地都失败时才返回 `SendNotificationError`
- 成功和错误消息分别累积返回

### 6.2 支持的通知通道

#### HTTP Webhook（`alert.rs:1227-1321`）：

```rust
async fn send_http_notification(endpoint: &Endpoint, msg: String) -> Result<String, anyhow::Error>
```

**关键特性：**
- 支持 GET/POST/PUT 方法
- 自定义 Headers（自动添加 `Content-type: application/json` 如未指定）
- SSRF 防护（`SsrfGuard::validate_url_with_config_async`）
- TLS 证书验证可跳过（`skip_tls_verify`）
- 企业版支持 Action 集成（`action_id`）
- 请求/响应日志记录（DEBUG 级别）

#### Email（`alert.rs:1323-1355`）：

```rust
pub async fn send_email_notification(
    email_subject: &str,
    email: &Email,
    msg: String,
) -> Result<String, anyhow::Error>
```

**关键特性：**
- 通过全局 SMTP 配置发送（`SMTP_CLIENT`）
- 支持多个收件人
- 支持 Reply-To 配置
- HTML + Plain Text 多部分格式

#### AWS SNS（`alert.rs:1357-1387`）：

```rust
async fn send_sns_notification(
    alert_name: &str,
    aws_sns: &AwsSns,
    msg: String,
) -> Result<String, anyhow::Error>
```

**关键特性：**
- 自动添加 `AlertName` 消息属性
- 使用全局 AWS 配置

### 6.3 事故通知通道

当 `creates_incident = true` 时使用事故专用通知，位于 `incidents.rs:298-416`：

```rust
async fn send_incident_notifications(
    alert: &Alert,
    incident_id: &str,
    event: &str,
    triggered_at: i64,
    dest_names: &[String],
)
```

#### 事故通知特点：

1. **目的地合并**：收集事故中所有关联告警的目的地并去重
2. **统一负载**：使用事故中心化的 JSON 格式，**不包含告警结果行数据**
3. **事件类型**：
   - `new_incident_created`：新事故创建
   - `new_alert_correlated`：新告警类型关联
   - `severity_changed`：严重级别变更

#### 事故通知负载结构：

```json
{
  "incident": {
    "id": "incident_id",
    "title": "incident_title",
    "event": "new_incident_created",
    "service": "service_name",
    "severity": "P3",
    "alert": {
      "name": "alert_name",
      "stream": { "name": "stream_name", "type": "logs" }
    },
    "time": "2026-05-23T10:00:00.000Z",
    "url": "https://.../web/alerts/incidents/..."
  }
}
```

> **注意**：事故通知负载中完全没有告警结果行的数据，只有事故和告警的元数据。

### 6.4 分组通知发送（重要补充）

分组通知通过异步任务发送，位于 `src/job/alert_grouping.rs`：

#### 只使用主告警的配置（重要发现）

> **关键发现**：分组发送时**只使用批次中第一个告警（主告警）的配置**，其他告警的配置被完全忽略。

**主告警选择逻辑**（`alert_grouping.rs:88-94`）：
```rust
// Get the first alert (primary) and grouping config
let primary_alert = &batch.alerts[0].alert;
let grouping_config = primary_alert
    .deduplication
    .as_ref()
    .and_then(|d| d.grouping.as_ref())
    .ok_or_else(|| anyhow::anyhow!("Grouping config not found"))?;
```

**被忽略的配置**：
1. 批次中其他告警的 `template`（模板）
2. 批次中其他告警的 `destinations`（目的地列表）
3. 批次中其他告警的 `row_template`（行模板）
4. 批次中其他告警的 `context_attributes`（上下文属性，部分被注入）

**实际使用的配置**：
- 通知模板：只使用 `primary_alert.template`
- 目的地列表：只使用 `primary_alert.destinations`
- 上下文属性：`primary_alert.context_attributes` + 分组相关注入（`grouped_alerts`、`alert_count`、`grouped_summary`、`is_grouped`）

#### 结果行合并策略（`alert_grouping.rs:158-171`）：

```rust
let combined_rows = match send_strategy {
    SendStrategy::FirstWithCount | SendStrategy::Summary => {
        // 只使用第一个告警的数据行
        batch.alerts[0].rows.clone()
    }
    SendStrategy::All => {
        // 合并所有告警的所有数据行
        let mut all_rows = Vec::new();
        for batched in &batch.alerts {
            all_rows.extend(batched.rows.clone());
        }
        all_rows
    }
};
```

#### 发送失败后的批次去向（重要发现）

> **关键发现**：分组发送失败后**没有重试机制，批次被永久丢弃**。

**发送失败处理逻辑**（`alert_grouping.rs:205-255`）：
```rust
match notification_alert.send_notification(...).await {
    Ok((success_msg, err_msg)) => {
        if !err_msg.is_empty() {
            // 部分目的地失败：记录错误和指标，返回 Err，不重试
            config::metrics::ALERT_GROUPING_SEND_ERRORS_TOTAL
                .with_label_values(&[batch.org_id.as_str(), "partial_failure"])
                .inc();
            return Err(anyhow::anyhow!("Partial failure: {}", err_msg));
        }
        // 全部成功：记录成功指标
        Ok(())
    }
    Err(e) => {
        // 全部失败：记录错误和指标，返回 Err，不重试
        config::metrics::ALERT_GROUPING_SEND_ERRORS_TOTAL
            .with_label_values(&[batch.org_id.as_str(), "send_failed"])
            .inc();
        Err(anyhow::anyhow!("Send failed: {}", e))
    }
}
```

**批次生命周期**：
1. 批次从 `PENDING_BATCHES` 中移除（通过 `get_ready_batch()` 或 `get_expired_batches()`）
2. 调用 `send_grouped_notification_sync(batch)` 发送
3. 无论成功或失败，批次都不会被放回 `PENDING_BATCHES`
4. 没有重试队列，没有持久化，失败即永久丢失

**相关指标**：
- `ALERT_GROUPING_SEND_ERRORS_TOTAL`：发送错误数（标签：org, type=partial_failure/send_failed）
- `ALERT_GROUPING_NOTIFICATIONS_SENT_TOTAL`：成功发送数（标签：org, strategy, reason=expired/max_size）
- `ALERT_GROUPING_WAIT_TIME`：等待时长直方图
- `ALERT_GROUPING_BATCH_SIZE`：批次大小直方图

---

## 7. 重试与延迟阈值的真实规则（重要修正 + 补充）

### 7.1 最大重试次数配置

```rust
// infra/src/scheduler/mod.rs:235-237
pub fn get_scheduler_max_retries() -> (bool, i32) {
    let max_retries = config::get_config().limit.scheduler_max_retries;
    (max_retries > 0, max_retries.unsigned_abs() as i32)
}
```

- 来自配置项 `limit.scheduler_max_retries`
- 如果 `max_retries > 0`，第一个返回值为 `true` 表示启用重试

### 7.2 调度拉取条件（重要补充）

调度器通过 `pull()` 方法从数据库拉取待执行的触发器，核心 SQL 位于 `sqlite.rs:405-418`：

```sql
UPDATE scheduled_jobs
SET status = 'Processing', start_time = $2,
    end_time = CASE
        WHEN module = $3 THEN $4
        ELSE $5
    END
WHERE id IN (
    SELECT id
    FROM scheduled_jobs
    WHERE status = 'Waiting' 
      AND next_run_at <= $7 
      AND NOT (is_realtime = $8 AND is_silenced = $9)
    ORDER BY next_run_at
    LIMIT $10
)
RETURNING *;
```

**拉取条件总结**：
1. `status = 'Waiting'`：状态必须是等待中
2. `next_run_at <= now`：下次执行时间已到
3. `NOT (is_realtime = true AND is_silenced = false)`：排除实时且未静默的告警（这些由事件驱动）
4. 按 `next_run_at` 排序，最老的先执行
5. 限制并发数（`alert_schedule_concurrency`）

**调度拉取节奏**：
- 由 `SchedulerJobPuller` 负责，`poll_interval_secs` 控制拉取间隔（默认10秒）
- 每次拉取数量不超过可用工作线程数
- 拉取后立即将状态改为 `Processing` 并设置 `start_time` 和 `end_time`（超时时间）

**超时监控**（`watch_timeout` 后台任务）：
- 定期扫描状态为 `Processing` 的触发器
- 如果 `now - start_time > timeout`，将状态改回 `Waiting` 并 `retries + 1`
- 超时后会被重新拉取执行

### 7.3 失败后重试触发节奏（重要补充）

> **关键发现**：重试触发节奏**不是**告警频率，而是调度拉取间隔（约10秒）。

**状态流转**：
```
评估失败
    │
    ▼
retries + 1 < max_retries ?
    ├─ 是 → update_status(Waiting, retries+1)
    │       │
    │       ▼
    │   下次 pull() 时被重新拉取（约10秒后）
    │       │
    │       ▼
    │   再次执行评估
    │
    └─ 否 → 计算下一个调度时间，retries 清零
            │
            ▼
        跳到下一个正常周期执行
```

**重试触发的完整流程**（`handlers.rs:716-728`）：
```rust
// 未超过最大重试次数
db::scheduler::update_status(
    &new_trigger.org,
    new_trigger.module,
    &new_trigger.module_key,
    db::scheduler::TriggerStatus::Waiting,  // 状态改回 Waiting
    trigger.retries + 1,                    // 重试计数 +1
    None,
    true,
    &query_trace_id,
).await?;
```

**关键要点**：
1. `update_status` **不会修改 `next_run_at`**，保持原有的执行时间
2. 由于 `next_run_at` 已经是过去的时间（因为执行失败了），下次 `pull()` 时会立即满足 `next_run_at <= now` 条件
3. 所以重试会在**下一次调度拉取时**被触发，间隔约等于 `poll_interval_secs`（默认10秒）
4. 重试间隔与告警频率无关，只与调度拉取间隔有关

### 7.4 评估失败时的重试逻辑（`handlers.rs:741-800`）

```rust
if result.is_err() {
    let err = result.err().unwrap();
    // ... 记录错误 ...
    
    if trigger.retries + 1 >= max_retries {
        // 超过最大重试次数
        if get_config().limit.pause_alerts_on_retries {
            // 自动禁用告警
            let mut alert_curr = get_by_id_db(&trigger.org, alert.id.unwrap()).await?;
            alert_curr.enabled = false;
            set_without_updating_trigger(&trigger.org, alert_curr).await?;
        }
        // 跳到下一个调度周期，重试计数清零
        new_trigger.next_run_at = alert.trigger_condition.get_next_trigger_time(
            true, alert.tz_offset, false, None
        )?;
        trigger_data.reset();
        new_trigger.data = json::to_string(&trigger_data).unwrap();
        db::scheduler::update_trigger(new_trigger, true, &query_trace_id).await?;
    } else {
        // 未超过最大重试次数，retries + 1，重新排队等待执行
        db::scheduler::update_status(
            &new_trigger.org,
            new_trigger.module,
            &new_trigger.module_key,
            db::scheduler::TriggerStatus::Waiting,
            trigger.retries + 1,  // 重试计数 +1
            None,
            true,
            &query_trace_id,
        ).await?;
    }
    
    publish_triggers_usage(trigger_data_stream);
    return Err(err);
}
```

**重试规则总结**：
1. 评估失败时，检查 `retries + 1 >= max_retries`
2. **未超过**：`retries + 1`，状态改为 `Waiting`，重新排队
3. **已超过**：
   - 可选自动禁用告警（`pause_alerts_on_retries = true`）
   - 计算下一个正常调度时间，重试计数清零
   - 触发器数据重置（`trigger_data.reset()`）
4. 成功时：重试计数自动清零（`trigger_data.reset()` 包含重试计数重置）

### 7.3 延迟阈值的真实情况

> **重要修正**：`_get_max_considerable_delay()` 函数定义了但**从未被调用**，是死代码。

**实际的延迟处理逻辑**：
- 没有基于阈值的智能跳过判断
- 只要 `delay > frequency`，就会跳过所有中间的执行时间点
- 跳过的时间戳会被记录到 `triggers` 流用于审计
- 最终使用当前时间作为评估窗口的结束时间

---

## 8. 完整执行流程图（修正版）

```
调度器触发 (handlers.rs:346)
    │
    ▼
获取告警配置 (handlers.rs:370-494)
    │
    ├─ 告警不存在 → 删除触发器，记录失败
    ├─ 告警未启用 → 延后7天，设置静默
    ├─ 超过最大重试 → 跳过，记录失败（可选禁用告警）
    └─ 正常 → 继续
    │
    ▼
计算评估窗口 (handlers.rs:629-702)
    │
    ├─ 跳过积压执行点（delay > frequency 时）
    ├─ 计算 start_time = end_time - period
    └─ 记录 skipped_alerts_count 到 triggers 流
    │
    ▼
执行告警评估 (handlers.rs:732-738 → alert.rs:1012)
    │
    ├─ 实时告警：evaluate_realtime()
    └─ 调度告警：evaluate_scheduled()
    │
    ▼
评估结果处理 (handlers.rs:803)
    │
    ├─ 评估失败 → retries+1 重新排队，或超过最大重试则跳到下一周期
    └─ 评估成功 → 继续
    │
    ▼
静默期处理 (handlers.rs:827-834)
    │
    └─ silence > 0 → 设置 is_silenced，延后下次执行
    │
    ▼
有匹配结果？(handlers.rs:848)
    │
    ├─ 无匹配 → 更新触发器，结束
    └─ 有匹配 → 继续
    │
    ▼
分组启用？(handlers.rs:851-964)
    │
    ├─ 是 → 计算指纹（基于first_row）→ add_to_batch()
    │      ├─ 批次满/超时 → 立即发送分组通知
    │      └─ 否则 → 等待，直接 return（不执行去重！）
    │
    └─ 否 → 继续
    │
    ▼
去重启用？(handlers.rs:968-1019 → deduplication.rs:160)
    │
    ├─ 是 → apply_deduplication()
    │      ├─ 每条结果行计算指纹
    │      ├─ 存在且在窗口内 → 更新 last_seen_at 和 count，抑制
    │      ├─ **全部被抑制 → 直接 return（跳过事故关联和通知！）**
    │      └─ 部分/全部通过 → 继续
    │
    └─ 否 → 继续
    │
    ▼
事故关联启用？(handlers.rs:1051-1095 → incidents.rs:482)
    │
    ├─ 是 → correlate_alert_to_incident(alert, first_row, &data, ...)
    │      ├─ 只从 first_row 提取维度（其他行维度被忽略！）
    │      ├─ 新事故/新告警类型 → 发送事故通知（无结果行数据）→ 结束
    │      ├─ 重复告警 → 抑制 → 结束
    │      └─ 关联失败 → 降级为直接通知
    │
    └─ 否 → 继续
    │
    ▼
直接发送通知 (handlers.rs:1138-1175 → alert.rs:1042)
    │
    ├─ 遍历目的地列表
    │   ├─ 获取目的地 + 模板
    │   ├─ 选择模板（告警级优先）
    │   ├─ 渲染行模板 → 渲染目的地模板
    │   └─ 按通道类型发送（HTTP/Email/SNS）
    │
    └─ 更新触发器状态，记录结果
```

---

## 9. 关键代码索引

### 9.1 评估窗口
- 窗口计算：`src/service/alerts/scheduler/handlers.rs:266-330`
- 延迟处理（死代码）：`src/service/alerts/scheduler/handlers.rs:333-344`
- 评估入口：`src/service/alerts/alert.rs:1012-1040`

### 9.2 抑制策略
- 静默期：`src/service/alerts/scheduler/handlers.rs:827-834`
- 分组批处理（互斥分支）：`src/service/alerts/scheduler/handlers.rs:851-964`
- 分组逻辑：`src/service/alerts/grouping.rs`
- 分组配置：`src/config/src/meta/alerts/deduplication.rs:148-200`
- 事故关联抑制：`src/service/alerts/incidents.rs:630-656`
- 事故关联首行维度提取：`src/service/alerts/incidents.rs:489-500`
- 去重全抑制跳过事故关联：`src/service/alerts/scheduler/handlers.rs:977-999`

### 9.3 模板渲染
- 行模板渲染：`src/service/alerts/alert.rs:1389-1511`
- 目的地模板渲染：`src/service/alerts/alert.rs:1520`
- 模板管理：`src/service/alerts/templates.rs`
- 系统模板初始化：`src/service/alerts/templates.rs:108-189`

### 9.4 历史去重
- 去重配置：`src/config/src/meta/alerts/deduplication.rs:55-140`
- 去重应用：`src/service/alerts/deduplication.rs:160-313`
- 指纹计算：`src/service/alerts/deduplication.rs:34-48`
- 去重状态实体（含死字段）：`src/infra/src/table/entity/alert_dedup_state.rs:20-40`
- 去重状态保存（只更新两个字段）：`src/service/alerts/deduplication.rs:71-97`
- 去重状态清理：`src/job/alert_manager.rs:100-185`
- 去重全抑制返回：`src/service/alerts/scheduler/handlers.rs:977-999`

### 9.5 通知投递
- 通知入口：`src/service/alerts/alert.rs:1042-1141`
- HTTP 通道：`src/service/alerts/alert.rs:1227-1321`
- Email 通道：`src/service/alerts/alert.rs:1323-1355`
- SNS 通道：`src/service/alerts/alert.rs:1357-1387`
- 事故通知（无结果行数据）：`src/service/alerts/incidents.rs:298-416`
- 分组通知发送（主告警配置）：`src/job/alert_grouping.rs:68-256`
- 分组结果行合并策略：`src/job/alert_grouping.rs:158-171`
- 分组发送失败处理：`src/job/alert_grouping.rs:205-255`

### 9.6 重试逻辑
- 最大重试次数配置：`src/infra/src/scheduler/mod.rs:235-237`
- 评估失败重试：`src/service/alerts/scheduler/handlers.rs:741-800`
- 调度拉取 SQL：`src/infra/src/scheduler/sqlite.rs:405-418`
- 调度拉取条件：`src/infra/src/scheduler/mod.rs:157-164`
- 重试状态更新（不改 next_run_at）：`src/service/alerts/scheduler/handlers.rs:716-728`
- 超时监控：`src/infra/src/scheduler/mod.rs:189-198`

---

## 10. 理解偏差修正汇总

| 之前的理解 | 实际代码实现 | 业务影响 |
|-----------|-------------|---------|
| 分组和去重是顺序执行 | 分组和去重是**互斥**分支，分组启用时不执行去重 | 分组时无法享受到去重的历史抑制能力 |
| 延迟阈值 `min(1h, 频率×20%)` 有效 | `_get_max_considerable_delay()` 是**死代码**，从未调用 | 只要延迟超过一个周期就会跳过所有积压点 |
| 事故关联合并所有结果行维度 | 只使用 `first_row` 提取维度，其他行维度被**完全忽略** | 多结果行时可能导致错误的事故聚合 |
| 事故通知包含结果行数据 | 事故通知只有**事故元数据**，不包含任何告警结果行 | 接收方无法从事故通知中获取具体的告警数据 |
| `notification_sent` 字段控制通知发送 | 该字段是**死字段**，从未被读取或更新 | 去重判断完全依赖时间窗口，与通知状态无关 |
| 评估失败时指数退避重试 | 评估失败时 `retries+1` 重新排队，超过最大重试则跳到下一周期 | 没有退避，重试间隔等于调度拉取间隔（~10秒） |
| **新增**：重试间隔等于告警频率 | 重试触发由调度拉取间隔决定，约等于 `poll_interval_secs`（默认10秒） | 重试比预期更频繁，可能增加系统负载 |
| **新增**：分组发送合并所有告警的模板和目的地 | 分组只使用**第一个告警**的模板、目的地配置，其他告警的配置被忽略 | 批次中其他告警的通知可能发错目的地或用错模板 |
| **新增**：分组发送失败有重试机制 | 分组发送失败后**没有重试**，批次被永久丢弃 | 发送失败时会丢失告警通知 |
| **新增**：去重全抑制时仍会进入事故关联 | 去重全抑制时**直接 return**，跳过事故关联和所有通知 | 事故系统无法感知到被去重抑制的告警触发 |
| **新增**：调度拉取只看 `next_run_at` | 拉取条件：`status=Waiting AND next_run_at<=now`，重试时不修改 `next_run_at` | 失败后立即被重新拉取，间隔约10秒 |

---

## 11. 注意事项与设计权衡

### 11.1 企业版 vs OSS 版本差异
- 历史去重、分组批处理、事故关联均为企业版特性
- OSS 版本仅保留基础的告警评估和通知发送功能
- 代码中大量使用 `#[cfg(feature = "enterprise")]` 条件编译

### 11.2 分布式环境考虑
- 告警管理器角色选举（`LOCAL_NODE.is_alert_manager()`）
- 超级集群支持（`super_cluster.enabled`）
- 分布式锁保护模板初始化
- 触发器状态持久化到数据库

### 11.3 可观测性
- 完整的指标埋点（Prometheus 格式）
- 详细的日志分级（DEBUG/INFO/WARN/ERROR）
- 触发记录写入 `triggers` 流用于审计
- Trace ID 贯穿整个评估链路

### 11.4 容错设计
- 去重失败时降级为不过滤，避免丢失告警
- 事故关联失败时降级为直接通知
- 部分目的地失败不影响其他目的地
- 重试机制：评估失败时 retries+1 重新排队，超过最大重试则跳到下一周期

### 11.5 潜在改进点
1. **死代码清理**：删除未使用的 `_get_max_considerable_delay()` 函数和 `notification_sent` 字段
2. **事故关联维度**：考虑使用所有结果行的维度并集，而非仅第一行
3. **分组与去重整合**：分组时也能应用去重逻辑，避免重复告警
4. **事故通知内容**：在事故通知中可选包含关键的告警结果行数据
5. **智能延迟处理**：实际启用最大可接受延迟逻辑，避免过度跳过
6. **分组配置一致性**：分组发送时应考虑所有告警的目的地并集，或至少记录被忽略的告警
7. **分组发送重试**：分组发送失败后应有重试机制，避免永久丢失告警通知
8. **去重与事故关联协同**：去重全抑制时也应通知事故系统更新告警计数，保持数据一致性
9. **重试间隔可配置**：重试间隔应可独立配置，而非固定等于调度拉取间隔
10. **持久化分组批次**：分组批次应持久化到数据库，避免进程重启时丢失待发送批次
