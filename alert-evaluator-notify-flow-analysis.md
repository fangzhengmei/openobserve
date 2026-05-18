# 告警评估器完整流程说明

## 一、系统架构总览

告警系统采用**分布式调度 + 多模块协作**的架构设计，主要由以下核心组件构成：

| 组件层级 | 模块 | 主要职责 |
|---------|------|---------|
| 入口层 | `alert_manager.rs` | 告警管理器启动、节点角色检查、后台任务编排 |
| 调度层 | `scheduler/` | 触发器拉取、任务分发、工作池管理 |
| 执行层 | `handlers.rs` | 告警触发处理、超时重试、指标上报 |
| 评估层 | `mod.rs` / `alert.rs` | 规则解析、查询执行、条件评估、阈值判断 |
| 控制层 | `deduplication.rs` / `grouping.rs` | 去重抑制、批量分组（企业版） |
| 投递层 | `alert.rs` / `destinations.rs` | 模板渲染、多通道通知发送 |

---

## 二、告警管理器启动流程

### 2.1 启动入口 (`alert_manager.rs:22-138`)

1. **节点角色检查**：首先验证当前节点是否为 `alert_manager` 角色，非该角色直接返回
2. **超集群检查**（企业版）：在超集群部署中，确保只有一个集群运行告警管理器
3. **核心任务启动**：
   - 调度器主循环 (`run_schedule_jobs` -> `scheduler::run()`)
   - 超时任务监控 (`watch_timeout`)
   - 搜索作业工作池（企业版）
   - 去重状态清理（每小时执行一次）

### 2.2 调度器核心结构 (`scheduler/worker.rs`)

调度器采用**生产者-消费者**模式：

```
SchedulerJobPuller (生产者)
        ↓ pull() 定期拉取
   mpsc::channel
        ↓ recv()
SchedulerWorker [n] (消费者)
```

- **SchedulerJobPuller**：按 `poll_interval_secs` 间隔（默认60秒）从数据库拉取待执行的触发器
- **SchedulerWorker**：多Worker并发处理，并发数由 `alert_schedule_concurrency` 控制
- **保活机制**：每个Job在处理期间持续发送心跳，防止被超时监控误判

---

## 三、规则解析与查询调度

### 3.1 触发器生命周期

触发器（Trigger）是告警调度的基本单位，状态流转如下：

```
Waiting → Processing → Completed
   ↑           ↓
   └──── Retry <─── Failed
```

**触发器拉取条件** (`scheduler/mod.rs:157-174`)：
- `next_run_at <= now` 已到执行时间
- `status == "Waiting"` 等待执行
- 实时告警需检查静默状态

### 3.2 告警触发处理主流程 (`handlers.rs:346-1220`)

```
1. 加载告警配置
   ├─ 通过 module_key (KSUID) 从数据库加载 Alert
   └─ 验证告警有效性（存在性、是否启用）

2. 跳过检查
   ├─ 实时告警静默检查
   ├─ 告警启用状态检查
   ├─ 最大重试次数检查
   └─ 延迟过大跳过（超过频率的20%或1小时）

3. 时间窗口计算
   ├─ 对齐时间（align_time=true时按频率边界对齐）
   ├─ 处理跳过的时间戳
   └─ 确定最终评估的 end_time

4. 调用 alert.evaluate() 执行评估
   └─ 见 3.3 节

5. 后处理（含关键分叉逻辑）
   ├─ 【分叉A】分组检查（企业版）→ 入批后直接 return
   │      ├─ 批次已满 → 立即同步发送分组通知
   │      └─ 批次未满 → 等待超时或后续告警
   ├─ 【分叉B】去重检查（企业版）→ 失败降级继续
   │      ├─ 去重成功 → 过滤重复行
   │      └─ 去重失败 → 返回原始 data，继续流程
   ├─ 事件关联（企业版）→ 失败降级继续
   └─ 调用 send_notification() 发送通知

6. 调度下一次执行
   ├─ 计算 next_run_at
   ├─ 静默期设置（silence > 0 时）
   └─ 更新触发器状态
```

### 3.3 查询评估执行 (`mod.rs:82-529`)

`QueryConditionExt::evaluate_scheduled()` 是告警评估的核心入口，支持三种查询模式：

#### 模式1：Custom（自定义条件）
- 适用于日志类告警的简单条件配置
- 通过 `build_sql()` 动态生成SQL查询
- 条件通过 `ConditionListExt::to_sql()` 转换为WHERE子句

#### 模式2：SQL（自定义SQL）
- 用户直接提供完整SQL语句
- 需校验不包含 `SELECT *`
- 支持多流关联查询

#### 模式3：PromQL（指标查询）
- 专为指标类告警设计
- 自动包裹阈值条件：`(promql) operator value`
- 返回Matrix格式结果，转换为行数据后应用阈值判断

**查询执行流程**：
```
构建请求 → grpc_search() → 解析响应 → 应用阈值条件
     ↓
  结果集长度 与 threshold 比较（>, >=, <, <=, ==, !=）
     ↓
  满足条件 → 返回 TriggerEvalResults { data: Some(rows) }
```

### 3.4 规则解析引擎

条件评估支持两种格式（V1 / V2），通过 `AlertConditionParams` 统一抽象：

**条件表达式树**：
```
ConditionList
  ├─ OrNode { or: [ConditionList] }
  ├─ AndNode { and: [ConditionList] }
  ├─ NotNode { not: ConditionList }
  ├─ EndCondition(Condition)
  └─ LegacyConditions(Vec<Condition>)
```

**条件运算** (`mod.rs:664-731`)：
- 支持字符串、数值、布尔三种数据类型
- 运算符：`==`, `!=`, `>`, `>=`, `<`, `<=`, `contains`, `not contains`
- 支持忽略大小写比较
- V2格式支持运算符优先级（AND优先于OR）

**SQL生成** (`mod.rs:1031-1116`)：
- 根据Schema字段类型生成类型安全的SQL表达式
- 支持聚合查询（AVG/MAX/MIN/SUM/COUNT/P50/P75/P90/P95/P99）
- 支持 GROUP BY 分组

---

## 四、后处理关键分叉逻辑（企业版）

### 4.1 分组开启后入批并提前返回 (`handlers.rs:851-965`)

**执行顺序**：分组检查在去重检查之前，且分组开启后直接返回，跳过后续所有流程。

```
trigger_results.data.is_some() && !data.is_empty()
     ↓
检查 grouping_enabled = alert.deduplication.grouping.enabled
     ├─ 未开启 → 继续执行后续去重逻辑
     └─ 已开启 → 执行入批逻辑
           ↓
     计算指纹（优先使用 dedup_config 的 fingerprint_fields，否则使用告警唯一键）
           ↓
     调用 add_to_batch() 加入批次
           ↓
     【关键分支】
     ├─ 批次已满（batch_ready = true）
     │    ├─ get_ready_batch() 获取批次
     │    └─ send_grouped_notification_sync() 同步发送分组通知
     └─ 批次未满（batch_ready = false）
          └─ 仅记录日志，等待后台超时检测或后续告警填满
           ↓
     【统一提前返回】
     ├─ 更新 trigger_data_stream 标记 grouped=true
     ├─ 更新触发器到数据库
     └─ return Ok(())  →  跳过去重、事件关联、直接通知
```

**入批核心实现** (`grouping.rs:106-181`)：
- 使用 `DashMap` 并发安全的内存缓存 `PENDING_BATCHES`
- `add_to_batch()` 返回值：
  - `true` = 批次已满，需要立即发送
  - `false` = 批次未满，等待中
- 新批次创建时启动计时器 `timer_started_at`
- 后台定期通过 `get_expired_batches()` 扫描超时批次并发送

**批次内存结构**：
```rust
pub struct PendingBatch {
    fingerprint: String,           // 分组键
    org_id: String,                // 组织ID
    alerts: Vec<BatchedAlert>,     // 已入批的告警列表
    timer_started_at: i64,         // 批次创建时间（微秒）
    group_wait_seconds: i64,       // 最大等待秒数
    max_group_size: usize,         // 最大批次大小
}
```

### 4.2 去重报错后降级继续通知 (`handlers.rs:968-1019`)

**设计原则**：**Fail Open（宁可重复，不可漏报）**

```
调用 apply_deduplication(db, &alert, data.clone()).await
     ↓
 【match 匹配结果】
  ├─ Ok((deduplicated_data, deduplicated))
  │    ├─ data 为空且被去重 → return Ok(()) 跳过通知
  │    └─ 否则 → 使用 deduplicated_data 继续后续流程
  │
  └─ Err(e)  →  【降级路径】
       ├─ log::error! 记录错误
       └─ 返回原始 data（不做过滤），继续后续流程
```

**降级触发场景**：
1. **ORM 客户端不可用** (`ORM_CLIENT.get() == None`) → 记录 warn，返回原始 data
2. **去重逻辑执行异常**（如数据库查询失败、指纹计算错误）→ 记录 error，返回原始 data
3. **去重状态表操作失败**（如 `alert_dedup_state` 读写异常）→ 返回原始 data

**去重成功路径**：
```
对每条结果行计算 fingerprint
     ↓
查询 alert_dedup_state 表
     ├─ 存在且在窗口内 → 跳过该行，更新 occurrence_count
     └─ 不存在或超窗口 → 保留该行，插入新状态
           ↓
返回过滤后的 deduplicated_data
```

### 4.3 事件关联失败降级 (`handlers.rs:1051-1095`)

与去重逻辑一致，事件关联也采用 **Fail Open** 策略：
- `correlate_alert_to_incident()` 返回 `Err(e)` 时
- 记录错误日志，但 `incident_handled_notification = false`
- 继续执行后续的 `send_notification()` 直接通知

### 4.4 首行驱动 vs 逐行处理的协作机制

后处理的三个企业版模块（分组、去重、事件关联）在**处理粒度**上存在根本差异，这种设计直接影响通知行为。

#### 4.4.1 两种处理视角对比

| 模块 | 处理视角 | 代码位置 | 粒度 |
|------|---------|---------|------|
| 分组（Grouping） | **首行驱动** | `handlers.rs:890-898` | 整批结果 → 单一指纹 |
| 事件关联（Incidents） | **首行驱动** | `handlers.rs:1055` | 整批结果 → 单一事件关联 |
| 去重（Deduplication） | **逐行处理** | `deduplication.rs:214-310` | 每行结果 → 独立指纹检查 |

---

#### 4.4.2 分组与事件关联为何依赖首行

**分组指纹计算** (`handlers.rs:890-898`)：
```rust
if let Some(first_row) = data.first() {
    calculate_fingerprint(
        &alert,
        first_row,      // 🔴 只取第一行！
        dedup_config,
        org_config.as_ref(),
        &semantic_groups,
    )
} else {
    alert.get_unique_key()
}
```

**事件关联** (`handlers.rs:1055`)：
```rust
let incident_handled_notification = if alert.creates_incident
    && ...
    && let Some(first_row) = data.first()  // 🔴 只取第一行！
{
    correlate_alert_to_incident(
        &alert,
        first_row,  // 🔴 只传第一行！
        &data,      // 整批数据仅作上下文
        triggered_at,
    )
    ...
}
```

**设计原因**：
- **分组的语义目标**：将"同一类告警"批量聚合。分类由 `fingerprint_fields` 定义的维度决定，同一次查询返回的所有行在这些维度上应是同质的（否则说明查询条件或分组字段设计有问题）。
- **事件关联的语义目标**：判断"这条告警属于哪个已有事件"。事件是粗粒度实体，只需根据第一行的关键维度（如服务名、错误码）即可确定关联关系，后续行属于同一上下文。
- **性能考量**：指纹计算和事件关联都是相对昂贵的操作，整批执行一次即可满足业务需求。

---

#### 4.4.3 去重为何逐行执行

**去重实现** (`deduplication.rs:214-310`)：
```rust
for row in result_rows {          // 🟡 遍历每一行！
    let fingerprint = calculate_fingerprint(
        alert, &row, dedup_config, org_config, semantic_groups
    );

    let should_send = match get_dedup_state(db, &fingerprint).await? {
        Some(existing_state) => {
            if is_within_window(&existing_state, time_window_minutes) {
                // 在窗口内，抑制该行
                save_dedup_state(..., occurrence_count + 1).await;
                false
            } else {
                true  // 窗口过期，放行
            }
        }
        None => true,  // 新指纹，放行
    };

    if should_send {
        save_dedup_state(..., occurrence_count = 1).await;
        deduplicated_rows.push(row);  // 只保留放行的行
    }
}
```

**设计原因**：
- **去重的语义目标**：精确抑制"完全相同的告警重复出现"。即使在同一次查询结果中，不同行也可能具有不同的指纹（例如聚合查询返回的多行结果，每行代表不同主机/服务的告警）。
- **去重状态的粒度**：`alert_dedup_state` 表以 `fingerprint` 为主键，每行独立维护 `occurrence_count`（发生次数）。如果整批只计算一次，会导致非首行的指纹永远无法被抑制。
- **用户预期**：用户配置 `fingerprint_fields` 时期望的是"对每个匹配结果都应用去重规则"，而非"只对第一个结果应用"。

---

#### 4.4.4 视角差异对通知行为的影响

这种设计差异在实际运行中会产生以下行为差异：

**场景1：单次查询返回3行，指纹分别为 FP1、FP2、FP3**

| 模块 | 处理结果 | 通知影响 |
|------|---------|---------|
| 分组开启 | 取 FP1（首行）作为分组键，3行作为整体入批 | 收到1条分组通知，包含全部3行 |
| 去重开启 | 逐行检查：FP1、FP2、FP3 各自独立判断 | 可能部分行被抑制，通知只包含未被抑制的行 |
| 事件关联开启 | 用 FP1（首行）关联事件，3行整体归属同一事件 | 关联到同一事件，或作为新事件创建 |

**场景2：分组与去重同时启用（注意优先级）**

由于分组检查在去重检查之前，且分组后直接 `return`，实际执行顺序为：
```
分组检查 → 入批 → return ✂️
（去重逻辑永远不会执行）
```

这意味着：
- 开启分组后，去重配置被**隐式忽略**
- 整批数据不会经过逐行去重过滤
- 如果需要去重，必须在分组通知发送时由分组逻辑自行处理

**场景3：事件关联与去重同时启用**

执行顺序为：
```
去重（逐行）→ 过滤后的数据 → 事件关联（首行驱动）
```

这意味着：
- 事件关联看到的是**已经去重过滤后的数据**
- 如果去重过滤掉了首行，事件关联会使用**新的首行**（原第二行）计算关联
- 可能导致关联决策与原始查询结果的首行不一致

---

#### 4.4.5 关键设计权衡

| 维度 | 首行驱动（分组/事件关联） | 逐行处理（去重） |
|------|--------------------------|-----------------|
| **语义需求** | 批量/聚合决策 | 精确行级抑制 |
| **性能开销** | O(1) 次指纹计算/关联查询 | O(N) 次数据库查询 |
| **一致性** | 假设同批结果同质 | 每行独立判断，无假设 |
| **容错性** | 首行异常会影响整批 | 单行异常不影响其他行 |
| **用户可控性** | 只能通过 fingerprint_fields 间接控制 | 每行独立遵循配置规则 |

这种差异化设计是告警系统在**性能**、**语义**、**用户预期**三者之间的权衡结果：分组和事件关联追求效率与聚合语义，去重追求精确与行级公平。

---

## 五、通知投递通道

### 5.1 通知发送流程 (`alert.rs:1042-1387`)

```
Alert.send_notification(rows, ...)
     ↓
  1. 加载告警级模板（如果有）
     ↓
  2. 遍历所有 destinations
     ├─ 加载目标配置和模板
     ├─ 模板优先级：alert.template > destination.template
     ├─ 行模板渲染（process_row_template）
     ├─ 目标模板渲染（process_dest_template）
     └─ 按通道类型发送
          ├─ HTTP → send_http_notification
          ├─ Email → send_email_notification
          └─ SNS → send_sns_notification
```

### 5.2 模板渲染系统

**两层模板机制**：
1. **行模板** (`row_template`)：对每条匹配结果行单独渲染
   - 支持 `{column_name}` 变量替换
   - 支持 `{alert_name}`, `{alert_count}`, `{alert_start_time}` 等元变量
   - 支持 JSON 格式输出（`row_template_type = Json`）

2. **目标模板** (`destination.template.body`)：整体通知内容模板
   - 支持 `{rows}` 批量行结果占位符
   - 支持 `{rows:N}` 限制行数
   - 支持 `{...rows}` 数组展开
   - 智能识别JSON上下文，避免双重序列化

**特殊变量**：
- `{alert_url}`：自动生成告警跳转短链
- `{alert_trigger_time}`：触发时间戳（多种格式）
- `{context_attributes.*}`：用户自定义属性

### 5.3 通道实现

#### HTTP 通道 (`send_http_notification`)
- 支持 GET/POST/PUT 方法
- 自定义 Headers
- 可配置跳过TLS证书校验
- **SSRF防护**：通过 `SsrfGuard` 校验目标URL，防止内网地址访问
- 支持企业版 Action 集成

#### Email 通道 (`send_email_notification`)
- 基于SMTP配置发送
- 支持多收件人
- 自动转换为HTML/PlainText多部分格式

#### SNS 通道 (`send_sns_notification`)
- AWS SNS 主题发布
- 自动添加 AlertName 消息属性

---

## 六、模块协作关系图

```
┌────────────────────────────────────────────────────────────────────┐
│                       Alert Manager 节点                          │
│                                                                   │
│  ┌───────────────┐    ┌──────────────────┐                        │
│  │ JobPuller     │───▶│ SchedulerWorker  │──┐                     │
│  │ (pull jobs)   │    │ Pool (N workers) │  │                     │
│  └───────────────┘    └──────────────────┘  │                     │
│                                             ▼                     │
│                                   handle_alert_triggers           │
│                                         │                         │
│                      ┌──────────────────┼──────────────────┐      │
│                      │                  │                  │      │
│                      ▼                  ▼                  ▼      │
│              alert.evaluate()     grouping check     dedup check  │
│                      │                  │  return        │  fail  │
│                      │                  │                ▼        │
│                      │                  │            original data │
│                      │                  └───────────┐    │        │
│                      ▼                              ▼    ▼        │
│                TriggerEvalResults           send_notification()   │
│                                                     │             │
│                                         ┌───────────┼───────────┐ │
│                                         ▼           ▼           ▼ │
│                                       HTTP         Email        SNS│
└────────────────────────────────────────────────────────────────────┘
```

**分叉逻辑标注**：
- 🔴 分组开启 → 调用 `add_to_batch()` 后 **直接 return**，跳过后续流程
- 🟡 去重失败 → **降级返回原始 data**，继续通知流程
- 🟠 事件关联失败 → **降级返回 false**，继续直接通知

---

## 七、关键数据流向

### 7.1 单次告警执行数据链

```
Trigger (from DB)
    ↓ module_key
Alert (from DB)
    ↓ query_condition
QueryRequest → Search Engine
    ↓ hits
Vec<Map<String, Value>>  (匹配行)
    ↓ 阈值判断
TriggerEvalResults { data: Some(rows) }
    ↓ 【分叉点1: 分组检查】
    ├─ 分组开启 → add_to_batch() → return ✂️
    └─ 分组关闭 → 继续
          ↓ 【分叉点2: 去重检查】
          ├─ 去重成功 → filtered_rows
          └─ 去重失败 → original_rows（降级）
                ↓ 【分叉点3: 事件关联】
                ├─ 关联成功 → 内部处理通知 → return ✂️
                └─ 关联失败 → 继续
                      ↓
                FinalRows
                      ↓
                模板渲染
                      ↓
                Notification Payload
                      ↓
                通道发送
                      ↓
                Destination Response
```

### 7.2 触发器数据字段

| 字段 | 含义 |
|------|------|
| `module_key` | 告警ID（KSUID），关联Alert表 |
| `next_run_at` | 下次执行时间（微秒） |
| `start_time` | 本次执行开始时间 |
| `status` | Waiting / Processing / Completed / Failed |
| `retries` | 已重试次数 |
| `data` | JSON格式扩展数据，包含 `period_end_time`, `last_satisfied_at` 等 |
| `is_realtime` | 是否为实时告警 |
| `is_silenced` | 是否处于静默期 |

---

## 八、错误处理与重试机制

### 8.1 告警评估失败
- 重试次数 < 最大重试次数 → `retries + 1`，状态重置为 Waiting
- 达到最大重试次数 → 计算下次 `next_run_at`，重置重试计数
- 配置 `pause_alerts_on_retries=true` 时，达到最大重试后自动禁用告警

### 8.2 通知发送失败
- 部分成功 / 部分失败 → 记录错误，不重试
- 全部失败且未达最大重试 → 重试
- 全部失败且达最大重试 → 跳过，调度下次

### 8.3 保活与超时
- Job处理期间持续发送心跳
- `watch_timeout` 监控任务执行时间，超时后重置为 Waiting

### 8.4 后处理降级策略（企业版）
| 模块 | 失败处理 | 影响 |
|------|---------|------|
| 分组（grouping）| 批次发送失败仅记录日志 | 可能丢失分组通知，但不影响告警触发 |
| 去重（deduplication）| 返回原始数据，继续流程 | 可能收到重复告警，但不会漏报 |
| 事件关联（incidents）| 标记为未处理，执行直接通知 | 不关联事件，但正常发送通知 |

---

## 九、关键配置参数

| 配置项 | 默认值 | 含义 |
|--------|--------|------|
| `alert_schedule_concurrency` | 10 | 告警并发执行数 |
| `alert_schedule_timeout` | 600 | 告警执行超时（秒） |
| `alert_schedule_interval` | 60 | 调度拉取间隔（秒） |
| `alert_considerable_delay` | 20 | 可容忍延迟百分比（超过则跳过） |
| `scheduler_max_retries` | 0 | 最大重试次数（0为无限） |
| `pause_alerts_on_retries` | false | 达最大重试后是否禁用告警 |
| `group_wait_seconds` | 配置项 | 分组等待窗口（秒） |
| `max_group_size` | 配置项 | 最大批次大小 |
