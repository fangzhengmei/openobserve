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
| Job Puller 拉取 | `src/service/db/scheduler.rs:111` `pull()` → `infra_scheduler::pull()` | 从 DB 批量拉取 status=Waiting 且 next_run_at ≤ now 的 Trigger |
| Worker 消费 | `src/service/alerts/scheduler/worker.rs:76` `SchedulerWorker::run()` | 从 mpsc channel 接收 job，调用 `handle_triggers()` |
| 路由分发 | `src/service/alerts/scheduler/handlers.rs:64` `handle_triggers()` | 根据 `trigger.module` 分发，Report 走 `handle_report_triggers()` |

### 2.3 handle_report_triggers 核心逻辑

**位置**：`src/service/alerts/scheduler/handlers.rs:1341`

1. 从 DB 读取 report 配置（`db::dashboards::reports::get_by_id()`）
2. 根据 `report.frequency.frequency_type` 计算 `next_run_at`：
   - `Hours` / `Days` / `Weeks` / `Months` → 当前时间 + interval
   - `Once` → +7天，并标记 `run_once=true`
   - `Cron` → `Schedule::from_str().upcoming().next()`
3. 若 `align_time=true`，则对齐到频率边界
4. 调用 `report.send_subscribers()`（关键分支点，见下文）
5. 成功/失败后更新 Trigger 状态（`update_trigger` / `update_status`）
6. 若 `run_once=true` 且发送成功，自动 `report.enabled = false`
7. 发布 TriggerData 到自上报流（`publish_triggers_usage`）

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
3. **构造 Dashboard URL**：
   - Relative 时间：`period={period}&timezone={timezone}`
   - Absolute 时间：`from={from}&to={to}&timezone={timezone}`
   - 附加参数：`print=true`、`var-Dynamic+filters=%255B%255D`、`search_type=reports`
4. **导航**：先切 org（`?org_identifier={org_id}`），再打开 Dashboard URL
5. **等待数据加载**：`wait_for_panel_data_load()` — 轮询查找 `span#dashboardVariablesAndPanelsDataLoaded`，超时由 `ZO_CHROME_SLEEP_SECS` 控制
6. **验证渲染**：检查 `<main>` 和 `div.displayDiv` 元素存在
7. **PDF 抓取**：`page.pdf(PrintToPdfParams { landscape: true, .. })` — 通过 CDP 协议 `PrintToPdf`
8. **返回**：`(pdf_data: Vec<u8>, email_dashb_url: String)`

### 3.3 远程模式 — 独立 report_server

**位置**：`src/report_server/src/`

| 模块 | 代码位置 | 说明 |
|------|----------|------|
| 服务启动 | `src/report_server/src/server.rs:7` `spawn_server()` | 绑定端口，启动 axum 服务 |
| 路由 | `src/report_server/src/router.rs:142` `create_router()` | `PUT /api/:org_id/reports/:name/send` |
| 请求处理 | `src/report_server/src/router.rs:59` `send_report()` | 解析请求，调用 `generate_report()` + `send_email()` |
| 渲染逻辑 | `src/report_server/src/report.rs:141` `generate_report()` | 与本地模式几乎相同，使用 chromiumoxide 渲染 |

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

### 3.4 浏览器配置

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
2. 构建 `lettre::Message`：
   - From: `ZO_SMTP_FROM_EMAIL`
   - To: `report.destinations` 中的所有 Email 地址
   - Reply-To: `ZO_SMTP_REPLY_TO`
   - 主体：`MultiPart::mixed()` + HTML 正文 + PDF 附件
3. 附件内容：
   - 文件名：`{sanitize_filename(report.title)}.pdf`
   - MIME: `application/pdf`
   - 数据：`pdf_data: &[u8]`（从 `generate_report()` 返回的 `Vec<u8>`）
4. 发送：`SMTP_CLIENT.send(email).await`
5. URL 缩短：`short_url::shorten(org_id, &email_dashb_url)` 缩短邮件中的 Dashboard 链接

### 4.2 远程模式邮件发送

**位置**：`src/report_server/src/report.rs:396` `send_email()`

逻辑与本地模式一致，使用 `SMTP_CLIENT` 全局静态实例（`src/report_server/src/report.rs:115`）。

### 4.3 仪表盘 JSON 导出（前端直接导出）

**位置**：`web/src/components/dashboards/ExportDashboard.vue`

这是另一个"导出"概念 — 将仪表盘配置导出为 `.dashboard.json` 文件：

1. 调用 `getDashboard()` 获取仪表盘完整 JSON
2. 清空 `owner` 字段
3. 使用 `data:text/json` URI + `<a>` 标签触发浏览器下载

**注意**：这与报表 PDF 导出是完全不同的链路，不经过后端。

---

## 五、关键衔接关系总览

```
┌──────────────────────────────────────────────────────────────────┐
│                        前端 (Vue)                                │
│  reports.ts → POST/PUT /api/{org}/reports                       │
│  reports.ts → PUT /api/{org}/reports/{name}/trigger             │
│  ExportDashboard.vue → 浏览器端 JSON 下载（独立链路）             │
└───────────────────────────┬──────────────────────────────────────┘
                            │ HTTP
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│                  主服务 (OpenObserve Server)                      │
│                                                                  │
│  HTTP Handler (reports.rs)                                       │
│    → service::dashboards::reports::save()                        │
│      → db::dashboards::reports::create()                         │
│        ├─ ORM: INSERT INTO reports                               │
│        └─ db::scheduler::push(Trigger{module=Report})            │
│           └─ ORM: INSERT INTO scheduled_jobs                     │
│                                                                  │
│  Scheduler Worker                                                │
│    → pull() 从 scheduled_jobs 拉取到期任务                        │
│    → handle_report_triggers()                                    │
│      → 计算 next_run_at，更新 Trigger                            │
│      → report.send_subscribers()  ◄──── 关键分支点 ────┐        │
│        ├─ report_server_url 非空?                       │        │
│        │   YES → HTTP PUT → report_server               │        │
│        │   NO  → generate_report() (内嵌)               │        │
│        └─ send_email()                                  │        │
└─────────────────────────────┬────────────────────────────────────┘
                              │ HTTP PUT (远程模式)
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│              独立报表服务 (report_server)                          │
│                                                                  │
│  PUT /api/:org_id/reports/:name/send                             │
│    → generate_report()                                           │
│      → Browser::launch() → 登录 → 导航到 Dashboard               │
│      → wait_for_panel_data_load() (轮询 span 元素)               │
│      → page.pdf() (CDP PrintToPdf)                               │
│    → send_email()                                                │
│      → lettre SMTP → PDF 附件投递到收件人                          │
└──────────────────────────────────────────────────────────────────┘
```

---

## 六、关键文件索引

| 职责 | 文件路径 |
|------|----------|
| HTTP 请求处理 (V1/V2) | `src/handler/http/request/dashboards/reports.rs` |
| HTTP 响应模型 | `src/handler/http/models/reports.rs` |
| 业务逻辑（save/get/trigger/enable） | `src/service/dashboards/reports.rs` |
| DB 层（create/update/delete + 同步 Trigger） | `src/service/db/dashboards/reports.rs` |
| 数据模型（Report/ReportDashboard/ReportFrequency 等） | `src/config/src/meta/dashboards/reports.rs` |
| Trigger 模型 | `src/config/src/meta/triggers.rs` |
| Scheduler DB 操作 | `src/service/db/scheduler.rs` |
| 调度 Worker | `src/service/alerts/scheduler/worker.rs` |
| Trigger 处理路由 + Report 触发逻辑 | `src/service/alerts/scheduler/handlers.rs:1341` |
| 独立报表服务 - 入口 | `src/report_server/src/server.rs` |
| 独立报表服务 - 路由 | `src/report_server/src/router.rs` |
| 独立报表服务 - 渲染+邮件 | `src/report_server/src/report.rs` |
| 独立报表服务 - 数据模型 | `src/report_server/src/models.rs` |
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

### 7.2 Cache 类型报表

当 `report.destinations` 为空时，报表仍会触发浏览器渲染但不生成 PDF 也不发邮件（`ReportType::Cache`），仅为了预加载仪表盘数据到缓存。

### 7.3 仅支持单 Tab

当前代码中 `dashboard.tabs` 虽然是 `Vec<String>`，但实际只使用 `tabs[0]`（`src/service/dashboards/reports.rs:749` / `src/report_server/src/report.rs:166`）。

### 7.4 媒体类型扩展

`ReportMediaType` 支持 PDF（默认）、PNG、CSV 三种类型（`src/config/src/meta/dashboards/reports.rs:29`），但当前渲染抓取链路仅实现了 PDF 路径（`page.pdf()`）。PNG/CSV 的抓取逻辑尚未在 `generate_report()` 中实现。

### 7.5 邮件附件模式

`ReportEmailAttachmentType` 支持 Standard（默认附件）和 Inline（内嵌）两种模式。Inline 仅对 PNG 类型生效，PDF 使用 Inline 会返回错误 `InlineAttachmentTypeNotSupportedForPdf`。
