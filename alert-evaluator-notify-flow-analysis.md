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

### 3.2 告警触发处理流程 (`handlers.rs:346-1220`)

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

5. 后处理
   ├─ 分组检查（企业版）
   ├─ 去重检查（企业版）
   ├─ 事件关联（企业版）
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

## 四、抑制与去重机制（企业版）

### 4.1 分组聚合 (`grouping.rs`)

**设计意图**：短时间内多条相同指纹的告警合并为一条通知，减少告警风暴。

```
相同指纹告警 → PENDING_BATCHES (内存缓存)
     ↓
  等待条件满足：
  ├─ 批次达到 max_group_size
  └─ 等待超时 group_wait_seconds
     ↓
  批量发送通知
```

**关键参数**：
- `group_wait_seconds`：等待窗口（秒）
- `max_group_size`：最大批次大小

### 4.2 去重抑制 (`deduplication.rs`)

**设计意图**：在时间窗口内抑制重复告警，避免重复通知。

```
每条结果行 → calculate_fingerprint()
     ↓
  查询 alert_dedup_state 表
     ├─ 存在且在窗口内 → 抑制，更新 occurrence_count
     └─ 不存在或超窗口 → 发送，保存新状态
```

**指纹计算**：
- 基于 `fingerprint_fields` 配置的字段值哈希
- 支持语义分组（semantic_groups）跨字段关联
- 支持跨告警去重（cross_alert_dedup）

**状态存储** (`alert_dedup_state` 表)：
- `fingerprint`：指纹主键
- `first_seen_at` / `last_seen_at`：时间边界
- `occurrence_count`：发生次数
- `notification_sent`：是否已发送

**清理策略**：每小时清理超过24小时的旧记录

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
┌─────────────────────────────────────────────────────────────────────┐
│                        Alert Manager 节点                           │
│  ┌───────────────┐    ┌──────────────────┐    ┌─────────────────┐  │
│  │ JobPuller     │───▶│ SchedulerWorker  │───▶│ handle_alert_   │  │
│  │ (pull jobs)   │    │ Pool (N workers) │    │ triggers        │  │
│  └───────────────┘    └──────────────────┘    └────────┬────────┘  │
│                                                         │           │
│  ┌───────────────┐    ┌──────────────────┐              │           │
│  │ Watch Timeout │    │ Dedup Cleanup    │              ▼           │
│  │ (监控超时)    │    │ (清理旧状态)     │    ┌─────────────────┐  │
│  └───────────────┘    └──────────────────┘    │ alert.evaluate()│  │
│                                              └────────┬────────┘  │
│                                                       │           │
│                          ┌───────────┬───────────────┘           │
│                          ▼           ▼                           │
│                ┌─────────────┐  ┌─────────────┐                 │
│                │ grouping.rs │  │ dedup.rs    │                 │
│                │ (批量分组)  │  │ (去重抑制)  │                 │
│                └──────┬──────┘  └──────┬──────┘                 │
│                       ▼                ▼                        │
│                ┌──────────────────────────────┐                  │
│                │    send_notification()       │                  │
│                │  ┌─────┐  ┌─────┐  ┌─────┐  │                  │
│                │  │HTTP │  │Email│  │ SNS │  │                  │
│                │  └─────┘  └─────┘  └─────┘  │                  │
│                └──────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────────┘
```

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
    ↓ 分组/去重过滤
FinalRows
    ↓ 模板渲染
Notification Payload
    ↓ 通道发送
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

1. **告警评估失败**：
   - 重试次数 < 最大重试次数 → `retries + 1`，状态重置为 Waiting
   - 达到最大重试次数 → 计算下次 `next_run_at`，重置重试计数
   - 配置 `pause_alerts_on_retries=true` 时，达到最大重试后自动禁用告警

2. **通知发送失败**：
   - 部分成功 / 部分失败 → 记录错误，不重试
   - 全部失败且未达最大重试 → 重试
   - 全部失败且达最大重试 → 跳过，调度下次

3. **保活与超时**：
   - Job处理期间持续发送心跳
   - `watch_timeout` 监控任务执行时间，超时后重置为 Waiting

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
