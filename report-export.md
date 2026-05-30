# OpenObserve 报表服务与仪表盘导出 — 代码链路梳理

## 一、整体架构概览

报表导出链路横跨前端、主服务、调度器、独立报表服务四大模块，核心流程：

```
前端创建/触发 Report
  → 主服务 HTTP Handler 接收请求
    → 持久化到 DB + 注册调度 Trigger
      → Scheduler Worker 拉取到期 Trigger
        → 调用 Report.send_subscribers()
          → 两条分支：
            ├─ 本地模式：内嵌 chromiumoxide 直接渲染 + SMTP 发邮件
            └─ 远程模式：HTTP PUT 转发到独立 report_server
                → report_server 调 chromiumoxide 渲染 + SMTP 发邮件
```

---

## 二、任务排期（Schedule / Trigger）链路

### 2.1 创建报表时注册调度

| 步骤 | 代码位置 | 说明 |
|------|----------|------|
| HTTP 入口 | `src/handler/http/request/dashboards/reports.rs:115` `create_report()` | V1 接口 `POST /{org_id}/reports` |
| HTTP 入口 | `src/handler/http/request/dashboards/reports.rs:535` `create_report_v2()` | V2 接口 `POST /v2/{org_id}/reports` |
| 业务校验+持久化 | `src/service/dashboards/reports.rs:115` `save()` | 校验 SMTP/Chrome/用户名密码、名称合法性、Cron 表达式、Dashboard/Tab 存在性 |
| DB 创建 + 注册 Trigger | `src/service/db/dashboards/reports.rs:52` `create()` | ① `create_without_updating_trigger()` → ORM 写入 `reports` 表 ② `db::scheduler::push(trigger)` → 写入 `scheduled_jobs` 表 |

**Trigger 结构**（`src/config/src/meta/triggers.rs:62`）：

```rust
pub struct Trigger {
    pub org: String,
    pub module: TriggerModule,       // = Report
    pub module_key: String,          // = report_id (KSUID)
    pub next_run_at: i64,            // 下次执行时间 (微秒)
    pub status: TriggerStatus,       // Waiting / Processing / Completed
    pub retries: i32,
    pub data: String,                // JSON: ScheduledTriggerData
}
```

### 2.2 调度器拉取 & 执行

| 步骤 | 代码位置 | 说明 |
|------|----------|------|
| Job Puller 拉取 | `src/service/db/scheduler.rs:111` `pull()` → `infra_scheduler::pull()` | 从 DB 批量拉取 status=Waiting 且 next_run_at ≤ now 的 Trigger，**原子更新**为 Processing |
| Worker 消费 | `src/service/alerts/scheduler/worker.rs:76` `SchedulerWorker::run()` | 从 mpsc channel 接收 job，调用 `handle_triggers()` |
| 路由分发 | `src/service/alerts/scheduler/handlers.rs:64` `handle_triggers()` | 根据 `trigger.module` 分发，Report 走 `handle_report_triggers()` |

**pull() 原子更新细节**（`src/infra/src/scheduler/sqlite.rs:405`）：
```sql
UPDATE scheduled_jobs
SET status = 'Processing', start_time = now,
    end_time = CASE WHEN module = 'Report' THEN now + report_timeout ELSE now + alert_timeout END
WHERE id IN (
    SELECT id FROM scheduled_jobs
    WHERE status = 'Waiting' AND next_run_at <= now
    ORDER BY next_run_at LIMIT concurrency
)
RETURNING *;
```

### 2.3 handle_report_triggers 核心逻辑

**位置**：`src/service/alerts/scheduler/handlers.rs:1341`

1. 前置检查：
   - `trigger.retries >= max_retries` → 跳过本次，直接推进 `next_run_at` 到下周期
   - 从 DB 读取 report 配置，失败则根据重试次数决定重试或跳到下周期
   - `report.enabled = false` → +7 天后再检查
   - Cloud 版本 free trial 过期 → 禁用 report 并删除 trigger
2. 根据 `report.frequency.frequency_type` 预计算 `next_run_at`：
   - `Hours` / `Days` / `Weeks` / `Months` → 当前时间 + interval
   - `Once` → +7天，并标记 `run_once=true`
   - `Cron` → `Schedule::from_str().upcoming().next()`
3. 若 `align_time=true`，则对齐到频率边界
4. 调用 `report.send_subscribers()`（关键分支点，见下文）
5. 成功：
   - `run_once=true` → 设置 `status=Completed`，`report.enabled=false`
   - 否则 → 设置 `next_run_at` 到下一周期，`retries=0`，`status=Waiting`
6. 失败：
   - `retries + 1 < max_retries` → `status=Waiting`，`retries+1`，`next_run_at` 不变（立即重试）
   - 达到最大重试 → `next_run_at` 推进到下一周期，放弃本次
7. 发布 TriggerData 到自上报流

### 2.4 手动触发 vs 定时触发 — 流程关系

#### 定时触发（完整调度链路）
```
scheduled_jobs (status=Waiting)
  ↓ pull() [原子 SQL]
scheduled_jobs (status=Processing, start_time=now, end_time=now+timeout)
  ↓
handle_report_triggers()
  ├─ 检查 retries、enabled、free trial
  ├─ 预计算 next_run_at
  ├─ report.send_subscribers()
  ├─ 根据结果更新 trigger 状态/重试次数/next_run_at
  └─ publish_triggers_usage()
```

#### 手动触发（极简链路）

**位置**：`src/service/dashboards/reports.rs:349` `trigger()` / `trigger_by_id()`

```rust
// src/service/dashboards/reports.rs:349-364
pub async fn trigger(org_id: &str, folder_id: &str, name: &str) -> Result<(), ReportError> {
    let conn = ORM_CLIENT.get_or_init(connect_to_orm).await;
    let report = match db::dashboards::reports::get(conn, org_id, folder_id, name).await {
        Ok(report) => report,
        _ => return Err(ReportError::ReportNotFound),
    };
    report.send_subscribers().await?;  // 直接调用，无后续副作用
    Ok(())
}
```

```
HTTP PUT /{org_id}/reports/{name}/trigger
  ↓
service::dashboards::reports::trigger()
  ├─ 从 DB 读取 report
  └─ 直接调用 report.send_subscribers()  ◀── 仅此一步！
  ↓
返回成功/失败（无状态持久化）
```

**关键差异表**：

| 维度 | 定时触发 | 手动触发 |
|------|----------|----------|
| 入口 | Scheduler Worker | HTTP API |
| Trigger 状态管理 | ✅ 完整生命周期（Waiting→Processing→Waiting/Completed） | ❌ 无状态变更 |
| 重试机制 | ✅ 基于 retries 字段，失败自动重试 | ❌ 无重试，直接返回错误 |
| next_run_at 更新 | ✅ 成功/失败后都推进到下周期 | ❌ 完全不影响 |
| run_once 自动禁用 | ✅ 成功后自动设置 `report.enabled = false` | ❌ 不修改 report 任何字段 |
| TriggerData 自上报 | ✅ 完整指标 | ❌ 不上报 |
| 代码位置 | `src/service/alerts/scheduler/handlers.rs:1341` | `src/service/dashboards/reports.rs:349` |

> **重要**：手动触发和定时触发**共享同一个 `report.send_subscribers()` 核心逻辑**，仅外层包装不同。
> 
> **send_subscribers 是纯函数**：方法签名为 `async fn send_subscribers(&self)`，使用 `&self` 不可变引用，**不会修改 Report 自身的任何字段**。
> 
> 手动触发是"旁路调用"，对调度器状态机 **零副作用**：
> - 不修改 scheduled_jobs 表（status/retries/next_run_at 都不变）
> - 不修改 reports 表（enabled 字段不变）
> - 即使是 run_once 的 report，手动触发成功也不会自动禁用

### 2.5 状态流转与重试机制

#### TriggerStatus 枚举

**位置**：`src/config/src/meta/triggers.rs:22`
```rust
pub enum TriggerStatus {
    Waiting,     // 0 — 待执行
    Processing,  // 1 — 执行中
    Completed,   // 2 — 已完成（仅用于 run_once）
}
```

#### 状态流转图

```
         pull() 原子更新
Waiting ──────────────────→ Processing
  ↑                              │
  │                              │ 成功
  │  run_once=true               ├────────→ Completed (停留在该状态)
  │                              │
  │  run_once=false              │
  ├──────────────────────────────┘ (retries=0, next_run_at 前进)
  │
  │ 失败 & retries+1 < max_retries
  ├──────────────────────────────┐ (retries+1, next_run_at 不变)
  │                              │
  │ 失败 & retries+1 >= max_retries
  └──────────────────────────────┘ (retries 不变, next_run_at 前进)
```

#### 失败路径不一致分析 — 两处失败分支的行为差异

代码中存在**两处独立的失败分支**，行为不一致，需要特别注意：

| 失败场景 | 达到重试上限的处理 | 未达到重试上限的处理 |
|---------|-------------------|---------------------|
| **获取 report 配置失败** (`handlers.rs:1375-1398`) | `next_run_at = now + 5 分钟`（硬编码） | `update_status(status=Waiting, retries+1)`，next_run_at 不变 |
| **send_subscribers 执行失败** (`handlers.rs:1628-1656`) | `next_run_at = 按频率计算的下一周期` | `update_status(status=Waiting, retries+1)`，next_run_at 不变 |

**代码证据 1 — 获取 report 配置失败**（`src/service/alerts/scheduler/handlers.rs:1375-1398`）：
```rust
Err(e) => {
    // if trigger max retries is reached, update the next run at
    if trigger.retries + 1 >= max_retries {
        // next run at is after 5mins  ← 硬编码 5 分钟！
        let next_run_at = now + Duration::minutes(5).num_microseconds().unwrap();
        new_trigger.next_run_at = next_run_at;
        db::scheduler::update_trigger(new_trigger, true, &query_trace_id).await?;
    } else {
        // Mark the trigger as failed
        db::scheduler::update_status(..., TriggerStatus::Waiting, trigger.retries + 1, ...).await?;
    }
}
```

**代码证据 2 — send_subscribers 执行失败**（`src/service/alerts/scheduler/handlers.rs:1628-1656`）：
```rust
Err(e) => {
    if trigger.retries + 1 >= max_retries && !run_once {
        // next_run_at 推进到按频率计算的下一周期 (new_trigger 在之前已预计算)
        db::scheduler::update_trigger(new_trigger, true, &query_trace_id).await?;
    } else {
        if run_once {
            report.enabled = true;  // run_once 失败时恢复 enabled=true
        }
        db::scheduler::update_status(..., TriggerStatus::Waiting, trigger.retries + 1, ...).await?;
    }
}
```

> **设计不一致性**：获取 report 配置失败时，达到重试上限后 5 分钟后重试；send_subscribers 失败时，达到重试上限后推进到下一频率周期。前者是"快速重试"策略，后者是"跳过本次"策略。

#### 重试次数配置

**位置**：`src/infra/src/scheduler/mod.rs:235` `get_scheduler_max_retries()`
```rust
pub fn get_scheduler_max_retries() -> (bool, i32) {
    let max_retries = config::get_config().limit.scheduler_max_retries;
    (max_retries > 0, max_retries.unsigned_abs() as i32)
}
```

- `ZO_SCHEDULER_MAX_RETRIES`：默认 3 次
- `ZO_SCHEDULER_MAX_RETRIES > 0` → 启用重试限制，最多重试 N 次
- `ZO_SCHEDULER_MAX_RETRIES <= 0` → 不限制重试（无限重试）

**Report 执行超时配置**（`src/config/src/config.rs:1677`）：
```rust
#[env_config(name = "ZO_REPORT_SCHEDULE_TIMEOUT", default = 300)]
pub report_schedule_timeout: i64,
```

- 配置项：`ZO_REPORT_SCHEDULE_TIMEOUT`（注意是 SCHEDULE，不是 SCHEDULER）
- 默认值：300 秒（5 分钟）
- 用途：pull() 时设置 end_time = now + report_schedule_timeout，用于超时检测

#### 超时检测（watch_timeout）

**位置**：`src/infra/src/scheduler/sqlite.rs:523`

后台任务每 30 秒执行：
```sql
UPDATE scheduled_jobs
SET status = 'Waiting', retries = retries + 1
WHERE status = 'Processing' AND end_time <= now;
```

> 执行超过 `end_time` 的任务会被自动重置为 `Waiting`，重试次数 +1，等待下次 pull。

#### 已完成任务清理（clean_complete）

**位置**：`src/infra/src/scheduler/sqlite.rs:498`

```sql
DELETE FROM scheduled_jobs
WHERE (status = 'Completed' OR retries >= max_retries)
  AND module != 'Alert';
```

> `run_once` 的 report 成功后进入 `Completed` 状态，会被后台清理任务删除。

---

## 三、渲染抓取（Render / Capture）链路

### 3.1 分支判断

**入口**：`src/service/dashboards/reports.rs:556` `Report::send_subscribers()`

```rust
if !cfg.common.report_server_url.is_empty() {
    // 远程模式：HTTP PUT 到独立 report_server
} else {
    // 本地模式：内嵌 chromiumoxide 直接渲染
}
```

### 3.2 本地模式 — 内嵌渲染

**位置**：`src/service/dashboards/reports.rs:728` `generate_report()`

1. **启动浏览器**：`Browser::launch(get_chrome_launch_options())` — 使用 `chromiumoxide` 库
2. **登录**：导航到 `{web_url}/login?login_as_internal_user=true`，填写 email + password
3. **构造 Dashboard URL**（关键参数根据 `search_type_params` 变化）：
   - Relative 时间：`period={period}&timezone={timezone}`
   - Absolute 时间：`from={from}&to={to}&timezone={timezone}`
   - 附加参数：`print=true`、`var-Dynamic+filters=%255B%255D`、`refresh=Off`
   - **search_type 参数**（根据收件人数量动态切换）：
     ```rust
     // src/service/dashboards/reports.rs:801
     let search_type_params = if no_of_recipients == 0 {
         "search_type=ui".to_string()                  // Cache 模式
     } else {
         format!("search_type=reports&report_id={org_id}-{report_name}")  // PDF 模式
     };
     ```
4. **导航**：先切 org（`?org_identifier={org_id}`），再打开 Dashboard URL
5. **等待数据加载**：`wait_for_panel_data_load()` — 轮询查找 `span#dashboardVariablesAndPanelsDataLoaded`，超时由 `ZO_CHROME_SLEEP_SECS` 控制
6. **验证渲染**：检查 `<main>` 和 `div.displayDiv` 元素存在
7. **PDF 抓取**（根据 ReportType 决定）：
   - PDF 模式：`page.pdf(PrintToPdfParams { landscape: true, .. })` — 通过 CDP 协议 `PrintToPdf`
   - Cache 模式：`vec![]` — 返回空字节数组，不生成 PDF
8. **返回**：`(pdf_data: Vec<u8>, email_dashb_url: String)`
   - **email_dashb_url 会经过短链接处理**（本地模式独有）

### 3.3 远程模式 — 独立 report_server

**位置**：`src/report_server/src/`

| 模块 | 代码位置 | 说明 |
|------|----------|------|
| 服务启动 | `src/report_server/src/server.rs:7` `spawn_server()` | 绑定端口，启动 axum 服务 |
| 路由 | `src/report_server/src/router.rs:142` `create_router()` | `PUT /api/:org_id/reports/:name/send` |
| 请求处理 | `src/report_server/src/router.rs:59` `send_report()` | 解析请求，判断 ReportType，调用 generate_report() + send_email() |
| 渲染逻辑 | `src/report_server/src/report.rs:141` `generate_report()` | 与本地模式类似，使用 chromiumoxide 渲染 |

**ReportType 判断**（`src/report_server/src/router.rs:70`）：
```rust
let report_type = if report.email_details.recipients.is_empty() {
    ReportType::Cache
} else {
    ReportType::PDF
};
```

**search_type 参数**（`src/report_server/src/report.rs:233`）：
```rust
let search_type_params = match report_type.clone() {
    ReportType::Cache => "search_type=ui".to_string(),
    _ => format!("search_type=reports&report_id={org_id}-{report_name}"),
};
```

**主服务调用远程的代码**（`src/service/dashboards/reports.rs:569`）：
```rust
let url = format!("{}/api/{}/reports/{}/send", &cfg.common.report_server_url, &self.org_id, &self.name);
Client::builder()
    .danger_accept_invalid_certs(cfg.common.report_server_skip_tls_verify)
    .build().unwrap()
    .put(url)
    .query(&[("timezone", &self.timezone)])
    .json(&report_data)
    .send().await
```

### 3.4 Cache 与 PDF 模式 — 渲染参数差异

两种模式由 `report.destinations` 是否为空决定，共享浏览器渲染流程，但参数和行为有显著差异：

| 维度 | Cache 模式（无收件人） | PDF 模式（有收件人） |
|------|----------------------|---------------------|
| 触发条件 | `no_of_recipients == 0` | `no_of_recipients > 0` |
| ReportType 枚举 | `ReportType::Cache` | `ReportType::PDF` |
| search_type | `search_type=ui` | `search_type=reports&report_id={org_id}-{report_name}` |
| PDF 生成 | `vec![]`（空数组，跳过 CDP 调用） | `page.pdf()`（实际生成 PDF 字节） |
| 浏览器行为 | 完整导航 + 等待数据加载 | 完整导航 + 等待数据加载 + 调用 PrintToPdf |
| 邮件发送 | `send_email()` 中 `recipients.is_empty()` 直接返回 `Ok(())` | 构建邮件 + PDF 附件 + SMTP 发送 |
| 用途 | 预热仪表盘数据到缓存，加速用户访问 | 定时推送报表邮件给用户 |

### 3.5 浏览器配置

**位置**：`src/report_server/src/report.rs:34` / `config` crate 的 `get_chrome_launch_options()`

- `ZO_CHROME_PATH`：指定 Chrome 路径，否则自动检测或下载
- `ZO_CHROME_NO_SANDBOX`：no-sandbox 模式
- `ZO_CHROME_WITH_HEAD`：是否有头模式
- `ZO_CHROME_WINDOW_WIDTH` / `ZO_CHROME_WINDOW_HEIGHT`：视口尺寸
- `ZO_CHROME_AUTO_DOWNLOAD`：是否自动下载 Chrome
- `ZO_CHROME_SLEEP_SECS`：等待页面数据加载超时

---

## 四、文件落盘（File Storage / 邮件投递）链路

### 4.1 本地模式邮件发送

**位置**：`src/service/dashboards/reports.rs:631` `send_email()`

1. 检查 `smtp_enabled`
2. 检查 `recipients.is_empty()` — 如果为空（Cache 模式），直接返回 `Ok(())`
3. 构建 `lettre::Message`：
   - From: `ZO_SMTP_FROM_EMAIL`
   - To: `report.destinations` 中的所有 Email 地址
   - Reply-To: `ZO_SMTP_REPLY_TO`
   - 主体：`MultiPart::mixed()` + HTML 正文 + PDF 附件
4. 附件内容：
   - 文件名：`{sanitize_filename(report.title)}.pdf`
   - MIME: `application/pdf`
   - 数据：`pdf_data: &[u8]`（从 `generate_report()` 返回的 `Vec<u8>`，Cache 模式为空）
5. 发送：`SMTP_CLIENT.send(email).await`

**关键前置：短链接生成**（`src/service/dashboards/reports.rs:943`）
```rust
// convert to short_url
let email_dashb_url = match short_url::shorten(org_id, &email_dashb_url).await {
    Ok(short_url) => short_url,
    Err(e) => {
        log::error!("Error shortening email dashboard url: {e}");
        email_dashb_url
    }
};
```
> **本地模式独有**：邮件中的 Dashboard URL 会经过短链接服务缩短，失败时回退到原始 URL。

### 4.2 远程模式邮件发送

**位置**：`src/report_server/src/report.rs:396` `send_email()`

逻辑与本地模式类似，但有两个关键差异：

1. **不做短链接处理**：`email_dashboard_url` 直接从 `generate_report()` 返回的原始 URL 传给 `send_email()`，**不调用 `short_url::shorten()`**
2. **独立 SMTP 客户端**：使用 `report_server/src/report.rs:115` 初始化的独立 `SMTP_CLIENT` 静态实例，与主服务的 SMTP_CLIENT 是独立的

**邮件正文 HTML**（`src/report_server/src/report.rs:421`）：
```rust
format!(
    "{}\n\n<p><a href='{}' target='_blank'>Link to dashboard</a></p>",
    email_details.message, email_details.dashb_url  // 这里是原始长 URL
)
```

### 4.3 短链接处理逻辑

**位置**：`src/service/short_url.rs`

#### shorten() 流程（`src/service/short_url.rs:64`）：

1. 基于原始 URL 生成 XXHash 64 位 short_id
2. 检查 DB 中是否已存在相同 short_id → 存在且 URL 匹配则复用
3. 不存在则存储到 DB：
   - 冲突（UniqueViolation）→ 加入时间戳重新生成 short_id，重试存储
4. 构造短链接格式：`{base_url}api/{org_id}/short/{short_id}`
5. 保留期由 `ZO_SHORT_URL_RETENTION_DAYS` 控制，到期后后台清理

#### 短链接重定向：
- HTTP 路由：`GET /{org_id}/short/{short_id}` → `short_url::retrieve()`
- 从 DB 取出 original_url，返回 302 重定向

### 4.4 本地模式 vs 远程模式 — 短链接差异

| 维度 | 本地模式 | 远程模式 |
|------|----------|----------|
| 短链接生成 | ✅ 调用 `short_url::shorten()` | ❌ 不生成，直接用原始 URL |
| 邮件中的链接 | `https://.../api/{org}/short/abc123` | `https://.../dashboards/view?org_identifier=...&dashboard=...&from=...&to=...` |
| 失败回退 | ✅ 短链接失败回退到原始 URL | N/A |
| 依赖 | 依赖主服务的 `short_urls` 表 | 无依赖 |
| 代码位置 | `src/service/dashboards/reports.rs:943` | 无（直接透传 URL） |

### 4.5 仪表盘 JSON 导出（前端直接导出）

**位置**：`web/src/components/dashboards/ExportDashboard.vue`

这是另一个"导出"概念 — 将仪表盘配置导出为 `.dashboard.json` 文件：

1. 调用 `getDashboard()` 获取仪表盘完整 JSON
2. 清空 `owner` 字段
3. 使用 `data:text/json` URI + `<a>` 标签触发浏览器下载

**注意**：这与报表 PDF 导出是完全不同的链路，不经过后端。

---

## 五、关键衔接关系总览

```
┌──────────────────────────────────────────────────────────────────────┐
│                           前端 (Vue)                                  │
│  reports.ts → POST/PUT /api/{org}/reports  (创建/更新)                │
│  reports.ts → PUT /api/{org}/reports/{name}/trigger  (手动触发)       │
│  ExportDashboard.vue → 浏览器端 JSON 下载（独立链路）                   │
└───────────────────────────┬────────────────────────────────────────────┘
                            │ HTTP
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    主服务 (OpenObserve Server)                          │
│                                                                      │
│  【创建/更新链路】                                                    │
│  HTTP Handler (reports.rs)                                            │
│    → service::dashboards::reports::save()                             │
│      → db::dashboards::reports::create()                              │
│        ├─ ORM: INSERT INTO reports                                    │
│        └─ db::scheduler::push(Trigger{module=Report, status=Waiting})│
│           └─ ORM: INSERT INTO scheduled_jobs                          │
│                                                                      │
│  【定时触发链路】                                                    │
│  Scheduler Worker                                                     │
│    → pull() [原子 SQL] 从 scheduled_jobs 拉取                          │
│       (status: Waiting → Processing, 同时设置 start_time/end_time)    │
│    → handle_report_triggers()                                         │
│       ├─ 前置检查 (retries/enabled/free trial)                        │
│       ├─ 预计算 next_run_at                                           │
│       ├─ report.send_subscribers()  ◄── 关键分支点 ───┐              │
│       │   ├─ report_server_url 非空?                   │              │
│       │   │   YES → HTTP PUT → report_server           │              │
│       │   │   NO  → generate_report() (内嵌)            │              │
│       │   │       └─ [本地模式独有] short_url::shorten()│              │
│       │   └─ send_email()                               │              │
│       ├─ 根据结果更新 trigger (status/retries/next_run_at)           │
│       └─ publish_triggers_usage()                                     │
│                                                                      │
│  【手动触发链路】                                                    │
│  HTTP PUT /{org}/reports/{name}/trigger                               │
│    → service::dashboards::reports::trigger()                          │
│       ├─ 从 DB 读 report                                              │
│       └─ 直接调用 report.send_subscribers()  (旁路调用，不触状态机)   │
└─────────────────────────────┬──────────────────────────────────────────┘
                              │ HTTP PUT (远程模式)
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                   独立报表服务 (report_server)                          │
│                                                                      │
│  PUT /api/:org_id/reports/:name/send                                  │
│    → 判断 recipients 数量 → ReportType (Cache/PDF)                    │
│    → generate_report()                                                │
│       ├─ Browser::launch() → 登录 → 导航到 Dashboard                  │
│       ├─ wait_for_panel_data_load() (轮询 span 元素)                  │
│       ├─ ReportType::PDF  → page.pdf() (CDP PrintToPdf)               │
│       └─ ReportType::Cache → vec![]  (不生成 PDF)                     │
│    → ReportType::Cache → 直接返回 200 OK (不发邮件)                   │
│    → ReportType::PDF → send_email()                                   │
│       └─ lettre SMTP → PDF 附件投递到收件人 (使用原始长 URL)          │
└──────────────────────────────────────────────────────────────────────┘
```

### 三大核心衔接点

1. **send_subscribers() 公共入口**：手动触发和定时触发最终都调用这个方法
2. **search_type_params 动态切换**：根据收件人数量，URL 参数在 `search_type=ui` 和 `search_type=reports` 间切换
3. **本地/远程模式分支**：`ZO_REPORT_SERVER_URL` 配置决定是内嵌渲染还是转发到独立服务

---

## 六、关键文件索引

| 职责 | 文件路径 |
|------|----------|
| HTTP 请求处理 (V1/V2) | `src/handler/http/request/dashboards/reports.rs` |
| HTTP 响应模型 | `src/handler/http/models/reports.rs` |
| 业务逻辑（save/get/trigger/enable/send_subscribers） | `src/service/dashboards/reports.rs` |
| DB 层（create/update/delete + 同步 Trigger） | `src/service/db/dashboards/reports.rs` |
| 数据模型（Report/ReportDashboard/ReportFrequency 等） | `src/config/src/meta/dashboards/reports.rs` |
| Trigger 模型（Trigger/TriggerStatus/TriggerModule） | `src/config/src/meta/triggers.rs` |
| Scheduler DB 操作封装 | `src/service/db/scheduler.rs` |
| Scheduler 核心实现（pull/update_status/clean_complete） | `src/infra/src/scheduler/mod.rs` |
| Scheduler SQLite 实现 | `src/infra/src/scheduler/sqlite.rs` |
| Scheduler Postgres 实现 | `src/infra/src/scheduler/postgres.rs` |
| 调度 Worker（循环拉取+分发） | `src/service/alerts/scheduler/worker.rs` |
| Trigger 处理路由 + Report 触发逻辑 | `src/service/alerts/scheduler/handlers.rs:1341` |
| 短链接服务（shorten/retrieve） | `src/service/short_url.rs` |
| 短链接 DB 层 | `src/service/db/short_url.rs` |
| 短链接数据表定义 | `src/infra/src/table/short_urls.rs` |
| 独立报表服务 - 入口 | `src/report_server/src/server.rs` |
| 独立报表服务 - 路由 | `src/report_server/src/router.rs` |
| 独立报表服务 - 渲染+邮件 | `src/report_server/src/report.rs` |
| 独立报表服务 - 数据模型（ReportType/Report/EmailDetails） | `src/report_server/src/models.rs` |
| ORM 实体 | `src/infra/src/table/entity/reports.rs` |
| DB 迁移 | `src/infra/src/table/migration/m20250611_000001_create_reports_table.rs` |
| 前端服务层 | `web/src/services/reports.ts` |
| 前端仪表盘 JSON 导出组件 | `web/src/components/dashboards/ExportDashboard.vue` |
| 前端报表接口定义 | `web/src/ts/interfaces/report.ts` |

---

## 七、重要设计细节

### 7.1 本地 vs 远程模式的决定因素

```rust
// src/service/dashboards/reports.rs:569
if !cfg.common.report_server_url.is_empty() {
    // 远程模式：ZO_REPORT_SERVER_URL 已配置
} else {
    // 本地模式：主服务内嵌 Chrome
}
```

- 本地模式要求：`ZO_CHROME_ENABLED=true` + `ZO_CHROME_PATH` + `ZO_REPORT_USER_NAME` + `ZO_REPORT_USER_PASSWORD`
- 远程模式要求：`ZO_REPORT_SERVER_URL` 已配置，独立服务要求 `ZO_REPORT_USER_EMAIL` + `ZO_REPORT_USER_PASSWORD`

### 7.2 ReportType 判断逻辑

两种模式判断 ReportType 的逻辑**等价但实现不同**：

| 模式 | 判断逻辑 | 代码位置 |
|------|----------|----------|
| 本地模式 | `no_of_recipients == 0` | `src/service/dashboards/reports.rs:801` |
| 远程模式 | `report.email_details.recipients.is_empty()` | `src/report_server/src/router.rs:70` |

> **修正**：之前文档说"Cache 类型报表"的判断，但实际上没有显式的 `ReportType::Cache` 标记传递给本地模式的 `generate_report()`。本地模式通过 `no_of_recipients` 参数隐式决定行为，只有 `search_type_params` 和是否跳过 `page.pdf()` 两个差异点。

### 7.3 search_type 参数的真实含义

| 值 | 含义 | 影响 |
|----|------|------|
| `search_type=ui` | 用户手动访问 | 前端埋点统计为普通用户访问 |
| `search_type=reports` | 报表服务自动访问 | 前端埋点统计为报表抓取，附带 `report_id` |

> **修正**：之前文档笼统说 `search_type=reports`，实际上根据是否有收件人在两种模式间动态切换。Cache 模式伪装成普通用户访问来预热缓存。

### 7.4 短链接处理差异

| 模式 | 邮件中的 URL | 代码位置 |
|------|-------------|----------|
| 本地模式 | 短链接 `https://.../api/{org}/short/abc123` | `src/service/dashboards/reports.rs:943` |
| 远程模式 | 原始长 URL（包含完整 from/to/period 参数） | `src/report_server/src/report.rs:392` |

> **重要**：远程模式不调用 `short_url::shorten()`，这意味着独立报表服务不依赖主服务的 `short_urls` 表，可以完全独立部署。

### 7.5 状态机完整性 — run_once 的特殊处理

当 `frequency_type = Once` 时：

1. 触发前标记 `run_once = true`
2. `send_subscribers()` 成功后：
   - `new_trigger.status = Completed`
   - `report.enabled = false`（通过 `update_without_updating_trigger` 禁用，不影响已有的 trigger 状态）
3. 后台 `clean_complete` 任务会删除 `status = Completed` 的 trigger

### 7.6 仅支持单 Tab

当前代码中 `dashboard.tabs` 虽然是 `Vec<String>`，但实际只使用 `tabs[0]`（`src/service/dashboards/reports.rs:749` / `src/report_server/src/report.rs:166`）。

### 7.7 媒体类型扩展

`ReportMediaType` 支持 PDF（默认）、PNG、CSV 三种类型（`src/config/src/meta/dashboards/reports.rs:29`），但当前渲染抓取链路仅实现了 PDF 路径（`page.pdf()`）。PNG/CSV 的抓取逻辑尚未在 `generate_report()` 中实现。

### 7.8 邮件附件模式

`ReportEmailAttachmentType` 支持 Standard（默认附件）和 Inline（内嵌）两种模式。Inline 仅对 PNG 类型生效，PDF 使用 Inline 会返回错误 `InlineAttachmentTypeNotSupportedForPdf`。

### 7.9 错误结论修正汇总

| 之前的结论 | 修正后的准确描述 | 代码证据 |
|----------|-----------------|----------|
| "send_subscribers 生成 PDF 作为附件" | 根据 `no_of_recipients` 判断：0 → Cache 模式返回 `vec![]`，不生成 PDF；>0 → 生成 PDF | `src/service/dashboards/reports.rs:801` |
| "report_server 的 send_email 使用 SMTP_CLIENT" | 正确，但需补充：使用独立的静态 SMTP_CLIENT，与主服务是两个实例 | `src/report_server/src/report.rs:115` |
| "Cache 模式仅预加载数据" | 正确，但需补充：search_type=ui 伪装成普通用户访问，不生成 report_id 标记 | `src/service/dashboards/reports.rs:801` |
| 未提到手动触发 | 手动触发是独立旁路调用，直接调用 `send_subscribers()`，不经过调度器状态机，**零副作用** | `src/service/dashboards/reports.rs:349-364` |
| 未提到短链接差异 | 本地模式生成短链接，远程模式使用原始 URL | `src/service/dashboards/reports.rs:943` vs `src/report_server/src/report.rs:392` |
| "定时触发更新状态" | pull() 原子更新 Waiting→Processing；成功后 run_once→Completed，否则→Waiting 前进；失败重试→Waiting retries+1 | `src/infra/src/scheduler/sqlite.rs:405` |
| "ZO_REPORT_SCHEDULER_TIMEOUT"（配置名错误） | 正确配置名是 `ZO_REPORT_SCHEDULE_TIMEOUT`（SCHEDULE，不是 SCHEDULER），默认 300 秒 | `src/config/src/config.rs:1677` |
| "默认 600 秒"（超时默认值错误） | 默认值是 300 秒（5 分钟），不是 600 秒 | `src/config/src/config.rs:1677` |
| 未提到失败路径不一致 | 获取 report 配置失败→5 分钟后重试；send_subscribers 失败→推进到下一周期 | `handlers.rs:1380-1384` vs `handlers.rs:1632-1639` |
| "run_once 手动触发也会自动禁用" | 手动触发**不会**自动禁用 run_once 的 report，只有定时触发成功才会 | `src/service/dashboards/reports.rs:349-364` |

### 7.10 关键配置项索引

| 配置项 | 作用 | 默认值 | 代码位置 |
|--------|------|--------|----------|
| `ZO_REPORT_SERVER_URL` | 非空则启用远程模式 | 空 | - |
| `ZO_CHROME_ENABLED` | 本地模式必需 | - | - |
| `ZO_CHROME_PATH` | Chrome 可执行文件路径 | - | - |
| `ZO_REPORT_USER_NAME` | 本地模式登录用户名 | - | - |
| `ZO_REPORT_USER_PASSWORD` | 本地模式登录密码 | - | - |
| `ZO_REPORT_USER_EMAIL` | 远程模式登录邮箱 | - | - |
| `ZO_SCHEDULER_MAX_RETRIES` | 最大重试次数（<=0 无限重试） | 3 | `src/config/src/config.rs:1681` |
| `ZO_REPORT_SCHEDULE_TIMEOUT` | Report 执行超时（秒） | 300 | `src/config/src/config.rs:1677` |
| `ZO_CHROME_SLEEP_SECS` | 等待页面数据加载超时 | - | - |
| `ZO_SHORT_URL_RETENTION_DAYS` | 短链接保留天数 | - | - |
