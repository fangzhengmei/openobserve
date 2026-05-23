# 告警评估器对接通知投递通道链路分析

## 1. 整体架构概览

告警评估器的完整链路由以下核心模块组成：

```
调度器触发 → 告警评估 → 分组批处理 → 历史去重 → 事件关联(可选) → 模板渲染 → 通知投递
```

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

#### 延迟处理机制：

- **最大可接受延迟**：`min(1小时, 频率 × 20%)`，参考 `handlers.rs:333-344`
- 超过延迟的告警会被跳过，并记录到 `triggers` 使用率流中
- 跳过的告警会记录 `skipped_alerts_count` 指标

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

#### 分组工作流程（`handlers.rs:851-964`）：

1. 评估完成后检查分组是否启用
2. 计算告警指纹（与去重共用指纹算法）
3. 调用 `grouping::add_to_batch()` 添加到批次
4. 批次满（达到 `max_group_size`）或超时（达到 `group_wait_seconds`）时触发发送
5. 否则等待下一次评估或定时扫描

#### 待处理批次管理：

使用内存中的 `DashMap` 存储待处理批次，键为指纹，值为 `PendingBatch`：

```rust
// grouping.rs:28-30
static PENDING_BATCHES: Lazy<Arc<DashMap<String, PendingBatch>>> = 
    Lazy::new(|| Arc::new(DashMap::new()));
```

### 3.3 事件关联抑制（Incident Correlation）

当 `alert.creates_incident = true` 时启用，位于 `handlers.rs:1051-1095`：

#### 抑制规则（`incidents.rs:630-656`）：

```rust
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
    pub notification_sent: bool,       // 是否已发送通知
    pub created_at: i64,               // 创建时间
}
```

### 5.4 去重应用流程

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
3. 若存在且在时间窗口内：
   - 更新 occurrence_count + 1
   - 更新 last_seen_at = now
   - 标记为抑制（不加入结果集）
   - 记录指标 ALERT_DEDUP_SUPPRESSED_TOTAL
4. 若不存在或超出窗口：
   - 创建新的去重状态记录
   - 加入结果集（发送通知）
   - 记录指标 ALERT_DEDUP_PASSED_TOTAL
```

#### 时间窗口判断：

```rust
// deduplication.rs:100-105
pub fn is_within_window(state: &Model, time_window_minutes: i64) -> bool {
    o2_enterprise::enterprise::alerts::dedup::is_within_time_window(
        state.last_seen_at,
        time_window_minutes,
    )
}
```

### 5.5 去重状态清理

定时清理任务位于 `alert_manager.rs:100-185`：

- 每小时执行一次（3600秒）
- 清理超过24小时的去重状态记录
- 企业版特性，OSS 版本为 no-op

### 5.6 指标监控

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
2. **统一负载**：使用事故中心化的 JSON 格式，而非告警行模板
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

### 6.4 分组通知发送

分组通知通过异步任务发送，位于 `src/job/alert_grouping.rs`（未在截取部分显示）：

- `send_grouped_notification_sync(batch)`：同步发送分组通知
- 根据 `SendStrategy` 决定最终发送内容

---

## 7. 完整执行流程图

```
调度器触发 (handlers.rs:346)
    │
    ▼
获取告警配置 (handlers.rs:370-494)
    │
    ├─ 告警不存在 → 删除触发器，记录失败
    ├─ 告警未启用 → 延后7天，设置静默
    ├─ 超过最大重试 → 跳过，记录失败
    └─ 正常 → 继续
    │
    ▼
计算评估窗口 (handlers.rs:629-702)
    │
    ├─ 处理延迟跳过（> 最大可接受延迟）
    ├─ 计算 start_time = end_time - period
    └─ 处理跳过的时间戳（告警积压）
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
    ├─ 评估失败 → 更新重试，记录错误
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
    ├─ 是 → 计算指纹 → add_to_batch()
    │      ├─ 批次满/超时 → 立即发送
    │      └─ 否则 → 等待，结束
    │
    └─ 否 → 继续
    │
    ▼
去重启用？(handlers.rs:968-1019 → deduplication.rs:160)
    │
    ├─ 是 → apply_deduplication()
    │      ├─ 全部被抑制 → 结束
    │      └─ 部分/全部通过 → 继续
    │
    └─ 否 → 继续
    │
    ▼
事故关联启用？(handlers.rs:1051-1095 → incidents.rs:482)
    │
    ├─ 是 → correlate_alert_to_incident()
    │      ├─ 新事故/新告警类型 → 发送事故通知 → 结束
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

## 8. 关键代码索引

### 8.1 评估窗口
- 窗口计算：`src/service/alerts/scheduler/handlers.rs:266-330`
- 延迟处理：`src/service/alerts/scheduler/handlers.rs:333-344`
- 评估入口：`src/service/alerts/alert.rs:1012-1040`

### 8.2 抑制策略
- 静默期：`src/service/alerts/scheduler/handlers.rs:827-834`
- 分组批处理：`src/service/alerts/grouping.rs`
- 分组配置：`src/config/src/meta/alerts/deduplication.rs:148-200`
- 事故关联抑制：`src/service/alerts/incidents.rs:630-656`

### 8.3 模板渲染
- 行模板渲染：`src/service/alerts/alert.rs:1389-1511`
- 目的地模板渲染：`src/service/alerts/alert.rs:1520`
- 模板管理：`src/service/alerts/templates.rs`
- 系统模板初始化：`src/service/alerts/templates.rs:108-189`

### 8.4 历史去重
- 去重配置：`src/config/src/meta/alerts/deduplication.rs:55-140`
- 去重应用：`src/service/alerts/deduplication.rs:160-313`
- 指纹计算：`src/service/alerts/deduplication.rs:34-48`
- 去重状态实体：`src/infra/src/table/entity/alert_dedup_state.rs:20-40`
- 去重状态清理：`src/job/alert_manager.rs:100-185`

### 8.5 通知投递
- 通知入口：`src/service/alerts/alert.rs:1042-1141`
- HTTP 通道：`src/service/alerts/alert.rs:1227-1321`
- Email 通道：`src/service/alerts/alert.rs:1323-1355`
- SNS 通道：`src/service/alerts/alert.rs:1357-1387`
- 事故通知：`src/service/alerts/incidents.rs:298-416`

---

## 9. 注意事项与设计权衡

### 9.1 企业版 vs OSS 版本差异
- 历史去重、分组批处理、事故关联均为企业版特性
- OSS 版本仅保留基础的告警评估和通知发送功能
- 代码中大量使用 `#[cfg(feature = "enterprise")]` 条件编译

### 9.2 分布式环境考虑
- 告警管理器角色选举（`LOCAL_NODE.is_alert_manager()`）
- 超级集群支持（`super_cluster.enabled`）
- 分布式锁保护模板初始化
- 触发器状态持久化到数据库

### 9.3 可观测性
- 完整的指标埋点（Prometheus 格式）
- 详细的日志分级（DEBUG/INFO/WARN/ERROR）
- 触发记录写入 `triggers` 流用于审计
- Trace ID 贯穿整个评估链路

### 9.4 容错设计
- 去重失败时降级为不过滤，避免丢失告警
- 事故关联失败时降级为直接通知
- 部分目的地失败不影响其他目的地
- 指数退避重试机制
