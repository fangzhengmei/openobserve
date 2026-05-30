# 限流与路由分层协作机制分析

## 一、整体架构概览

限流系统采用 **四层分层架构**，与路由分发紧密协作，在 Router 节点入口处完成限流检查。

```
┌─────────────────────────────────────────────────────────────┐
│                     Router Node                              │
│  ┌───────────┐    ┌─────────────┐    ┌──────────────────┐   │
│  │ 限流中间件 │ →  │  资源提取器  │ →  │  路由分发器       │   │
│  │(o2_ratelimit)│   │(rule_extractor)│  │(dispatch/proxy) │   │
│  └───────────┘    └─────────────┘    └──────────────────┘   │
│         ↓                 ↓                 ↓               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         四层限流规则匹配（优先级从高到低）              │  │
│  │  用户级 → 角色级 → 组织级 → 全局默认                 │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**关键特性**：
- 限流仅在 **Router 节点** 启用，非 Router 节点直接处理请求
- 限流检查发生在 **路由分发之前**，被限流的请求不会到达后端节点
- 依赖 `o2_ratelimit` 企业级库提供核心限流能力
- 必须启用 Nats 作为消息队列（用于分布式计数同步）

---

## 二、配额计数机制实现

### 2.1 限流规则数据结构

**核心数据结构**：`RatelimitRule` ([config/src/meta/ratelimit.rs:32](src/config/src/meta/ratelimit.rs#L32-L54))

```rust
pub struct RatelimitRule {
    pub org: String,                     // 组织标识
    pub rule_type: Option<String>,       // 规则类型: Exact / Regex
    pub rule_id: Option<String>,         // 规则唯一ID
    pub user_role: Option<String>,       // 用户角色
    pub user_id: Option<String>,         // 用户ID (邮箱)
    pub api_group_name: Option<String>,  // API分组名称
    pub api_group_operation: Option<String>, // API操作类型
    pub threshold: i32,                  // 阈值（请求数）
    pub stat_interval_ms: Option<i64>,   // 统计时间窗口（毫秒）
}
```

**资源标识格式**：
```
org:rule_type:user_role:api_group_name:api_group_operation:user_id
```

### 2.2 四层限流规则

| 层级 | 匹配条件 | 优先级 | 说明 |
|------|---------|--------|------|
| 用户级 | `user_id = 用户邮箱` | 最高 | 针对具体用户的限流 |
| 角色级 | `user_role = 角色名` + `user_id = ".*"` | 高 | 针对特定角色的限流 |
| 组织级 | `user_role = ".*"` + `user_id = ".*"` | 中 | 整个组织的限流 |
| 全局默认 | `org = ".*"` | 最低 | 系统默认限流规则 |

### 2.3 计数机制

**滑动窗口算法**：通过 `stat_interval_ms` 配置时间窗口大小，支持分层时间窗口：
- 1000ms = 1秒（默认）
- 60000ms = 1分钟
- 3600000ms = 1小时

**规则缓存**：`RATELIMIT_RULES_CACHE` ([router/ratelimit/resource_extractor.rs:23](src/router/ratelimit/resource_extractor.rs#L23))
- 内存缓存所有限流规则
- 定期从数据库刷新（由 `o2_ratelimit` 内部管理）
- 刷新间隔 ≥ 2秒（可配置）

**分布式同步**：
- 通过 Nats 消息队列在超集群间同步规则变更
- 支持 `RatelimitAdd` / `RatelimitUpdate` / `RatelimitDelete` 三种消息类型
- 处理逻辑见 [super_cluster_queue/ratelimit.rs:22](src/super_cluster_queue/ratelimit.rs#L22-L82)

### 2.4 规则匹配流程

**匹配函数**：`find_matching_rule()` ([router/ratelimit/resource_extractor.rs:264](src/router/ratelimit/resource_extractor.rs#L264-L306))

```rust
async fn find_matching_rule(
    org: &str,
    resource: &str,
    search_default: bool,  // true=搜索全局默认, false=搜索组织规则
) -> Option<RatelimitRule>
```

**匹配逻辑**：
1. 从缓存中过滤出对应组织（或全局 `.*`）的规则
2. 按顺序遍历规则：
   - **Exact 匹配**：资源标识完全相等
   - **Regex 匹配**：资源标识满足正则表达式
3. 返回第一个匹配的规则

---

## 三、路由分发与限流的协作流程

### 3.1 中间件挂载时机

**挂载位置**：`get_app()` ([handler/http/router/mod.rs:1102](src/handler/http/router/mod.rs#L1102-L1110))

```rust
// 仅在 Router 节点启用限流中间件
if config::cluster::LOCAL_NODE.is_router() {
    #[cfg(feature = "enterprise")]
    {
        router_routes = router_routes.layer(
            o2_ratelimit::middleware::RateLimitLayer::new_with_extractor(Some(
                crate::router::ratelimit::resource_extractor::default_extractor,
            )),
        );
    }
}
```

**执行顺序**（从外到内）：
```
请求 → DefaultBodyLimit → CORS → 限流中间件 → 路由分发
```

> **重要修正**：Router 节点的限流中间件在认证中间件之前执行。实际上 Router 节点的 `/api/*` 等路由**没有**挂载认证和审计中间件，认证由后端节点（Ingester/Querier）完成。
>
> **代码证明**：
> - [handler/http/router/mod.rs:1096-1110](src/handler/http/router/mod.rs#L1096-L1110)：`router_routes` 先 merge 路由，再 layer 限流中间件
> - [router/http/mod.rs:645-655](src/router/http/mod.rs#L645-L655)：`create_router_routes()` 返回的路由直接指向 `dispatch()`，无认证中间件

### 3.2 资源提取器工作流程

**入口函数**：`default_extractor()` ([router/ratelimit/resource_extractor.rs:134](src/router/ratelimit/resource_extractor.rs#L134-L139))

```
┌───────────────────────────────────────────────────────────┐
│                rule_extractor 执行流程                     │
├───────────────────────────────────────────────────────────┤
│  1. 解析请求路径，提取 org_id                              │
│     → extract_org_id()                                    │
│                                                           │
│  2. 匹配 OpenAPI 路径，确定 API 分组                       │
│     → extract_openapi_path()                              │
│     → find_group_by_openapi()                             │
│                                                           │
│  3. 解析认证信息，获取用户邮箱和角色                       │
│     → extract_auth_str_from_headers()                     │
│     → get_user_email_from_auth_str()                      │
│     → get_user_roles()                                    │
│                                                           │
│  4. 查找四层限流规则                                       │
│     → find_default_and_custom_rules()                     │
│       ├─ 全局默认规则 (org=".*")                          │
│       ├─ 用户级规则 (user_id=邮箱)                        │
│       ├─ 角色级规则 (user_role=角色, user_id=".*")       │
│       └─ 组织级规则 (user_role=".*", user_id=".*")       │
│                                                           │
│  5. 选择最终规则（按优先级）                               │
│     → select_final_rule_resource()                        │
│                                                           │
│  6. 返回四元组结果                                        │
│     (DefaultOrgGlobal, OrgLevel, UserRole, UserId)        │
└───────────────────────────────────────────────────────────┘
```

### 3.3 规则选择优先级

**选择函数**：`select_final_rule_resource()` ([router/ratelimit/resource_extractor.rs:203](src/router/ratelimit/resource_extractor.rs#L203-L262))

**优先级逻辑**：
1. **UserId（用户级）**：匹配 `user_role` + `user_id=用户邮箱`
2. **UserRole（角色级）**：匹配 `user_role` + `user_id=".*"`，多角色时选 `threshold` 最大的
3. **OrgLevel（组织级）**：匹配 `user_role=".*"` + `user_id=".*"`
4. **DefaultOrgGlobal（全局默认）**：匹配 `org=".*"`

**返回值结构**：`ExtractorRuleResult`
```rust
(
    ExtractorRule::DefaultOrgGlobal(Option<RatelimitRule>),
    ExtractorRule::OrgLevel(Option<RatelimitRule>),
    ExtractorRule::UserRole(Option<RatelimitRule>),
    ExtractorRule::UserId(Option<RatelimitRule>),
)
```

### 3.4 与路由分发的衔接

**路由分发入口**：`dispatch()` ([router/http/mod.rs:84](src/router/http/mod.rs#L84-L114))

```
┌───────────────────────────────────────────────────────────┐
│           请求处理完整流程（Router 节点）                  │
├───────────────────────────────────────────────────────────┤
│                                                           │
│  客户端请求                                                │
│      │                                                    │
│      ▼                                                    │
│  DefaultBodyLimit [L1140]                                 │
│      │  限制请求体大小                                    │
│      ▼                                                    │
│  CORS [L1139]                                             │
│      │  处理跨域请求                                      │
│      ▼                                                    │
│  限流中间件 (RateLimitLayer) [L1105]                       │
│      │  • 调用 default_extractor 提取规则                  │
│      │  • o2_ratelimit 内部执行配额计数检查                 │
│      │                                                    │
│      ├─ 超过阈值 → 短路返回 429 ◀──────────────┐           │
│      │                                         │           │
│      ▼                                         │           │
│  路由匹配                                      │           │
│      │                                         │           │
│      ├─ /api/*, /aws/*, /gcp/*, /rum/*       │           │
│      │    → dispatch() → 转发到后端节点        │           │
│      │      （后端节点执行认证/审计）           │           │
│      │                                         │           │
│      ├─ /config/*                              │           │
│      │    → config_routes() 直接处理           │           │
│      │                                         │           │
│      └─ /proxy/*                               │           │
│           → proxy_auth_middleware → proxy()   │           │
│              → 转发到后端节点                  │           │
│                （限流后仍有认证）              │           │
│                                                        │  │
│      ▼                                         │           │
│  返回响应给客户端 ◀─────────────────────────────┘           │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

**关键衔接点**：
- 限流检查在 **路由分发之前**，被限流的请求不会消耗后端资源
- Router 节点本身**不做认证/审计**（`/proxy/*` 除外），认证由后端节点完成
- 只有 `/proxy/*` 路由在限流后还有 `proxy_auth_middleware`
- 限流通过后，才会执行 `resolve_target()` 进行路由决策
- 对于需要解析请求体的路由（如搜索），限流在 body 解析前完成

---

## 四、降级响应逻辑

### 4.1 触发条件

当任意一层限流规则的计数超过阈值时，触发降级响应。

| 行为 | 代码内可验证 | 依赖外部库需确认 | 代码证据 |
|------|-------------|-----------------|---------|
| 对四层规则分别进行计数累加 | | ✓ | 由 `o2_ratelimit` 内部实现 |
| 检查是否有任意一层超过阈值 | | ✓ | 由 `o2_ratelimit` 内部实现 |
| 只要有一层超过阈值，立即触发降级 | | ✓ | 由 `o2_ratelimit` 内部实现 |
| 限流通过后才会执行后续路由 | ✅ | | [handler/http/router/mod.rs:1105](src/handler/http/router/mod.rs#L1105)：限流 layer 包裹 router_routes |

> **代码证明 - 短路机制**：
> axum 中间件通过 `next.run(request)` 调用后续流程。如果限流中间件不调用 `next.run()`，后续的路由匹配和分发逻辑**完全不会执行**。这一点由 axum 中间件机制保证，是可验证的。
>
> 项目代码将 `default_extractor` 传入 `RateLimitLayer`，但**不参与**计数和阈值检查逻辑：
> ```rust
> // [handler/http/router/mod.rs:1105-1109]
> RateLimitLayer::new_with_extractor(Some(
>     crate::router::ratelimit::resource_extractor::default_extractor,
> ))
> ```

---

### 4.2 降级响应内容

| 响应细节 | 代码内可验证 | 依赖外部库需确认 | 说明 |
|---------|-------------|-----------------|------|
| **状态码 429** | | ✅ | 理论上是标准的 429 Too Many Requests，但项目代码中无直接证据 |
| **Retry-After 响应头** | | ✅ | 项目代码中无直接证据，由 `o2_ratelimit` 内部实现 |
| **错误详情 JSON** | | ✅ | 项目代码中无直接证据，由 `o2_ratelimit` 内部实现 |
| **不调用 next.run()** | ✅ | | 由 axum 中间件机制保证，不调用 next 即短路 |
| **项目代码无 429 处理** | ✅ | | 全项目搜索无 `StatusCode::TOO_MANY_REQUESTS` 或 `429` 引用 |

> **重要说明**：项目代码中**没有任何地方**直接处理 429 状态码。所有 429 响应的构建完全由 `o2_ratelimit` 库内部完成。因此，关于 429 响应的具体内容（状态码、响应头、响应体格式）属于外部库行为，需要查阅 `o2_ratelimit` 文档或源码确认。

---

### 4.3 限流拒绝路径（429）完整流程

```
客户端请求
    │
    ▼
1. DefaultBodyLimit [L1140] ✅ 代码可验证
    │  限制请求体大小
    │  代码：app.layer(DefaultBodyLimit::max(...))
    │
    ▼
2. CORS [L1139] ✅ 代码可验证
    │  处理跨域请求，添加 CORS 响应头
    │  代码：app.layer(cors_layer())
    │
    ▼
3. RateLimitLayer [L1105]
    │
    ├─ 3.1 调用 default_extractor() ✅ 代码可验证
    │    ├─ [router/ratelimit/resource_extractor.rs:134-139]
    │    ├─ 提取 headers/path/method
    │    ├─ 提取 org_id、用户邮箱、角色
    │    ├─ 匹配 OpenAPI 分组
    │    ├─ 查找四层限流规则
    │    └─ 返回 ExtractorRuleResult 四元组
    │
    ├─ 3.2 o2_ratelimit 内部处理 ⚠️ 依赖外部库
    │    ├─ 对四层规则分别执行配额计数
    │    ├─ 检查每层计数是否超过阈值
    │    └─ 任意一层超过阈值 → 触发短路
    │
    └─ 3.3 短路返回 ✅ 代码可验证（机制上）
         │  不调用 next.run(request)
         │  后续所有流程完全终止
         │
         ├─ 构建 HTTP 响应 ⚠️ 依赖外部库
         │    ├─ 状态码：429（推测）
         │    ├─ 响应头：Retry-After（推测）
         │    └─ 响应体：错误 JSON（推测）
         │
         └─ 直接返回 Response
```

---

### 4.4 限流通过路径完整流程

```
客户端请求
    │
    ▼
1. DefaultBodyLimit [L1140] ✅ 代码可验证
    │
    ▼
2. CORS [L1139] ✅ 代码可验证
    │
    ▼
3. RateLimitLayer [L1105]
    │
    ├─ 3.1 调用 default_extractor() ✅ 代码可验证
    │    └─ （同上，提取规则）
    │
    ├─ 3.2 o2_ratelimit 内部处理 ⚠️ 依赖外部库
    │    ├─ 对四层规则分别执行配额计数
    │    ├─ 检查每层计数是否超过阈值
    │    └─ 全部未超过阈值 → 继续执行
    │
    └─ 3.3 调用 next.run(request) ✅ 代码可验证（机制上）
         │  进入后续路由匹配
         │
         ▼
4. 路由匹配 ✅ 代码可验证
    │
    ├─ 4.1 /api/*, /aws/*, /gcp/*, /rum/*
    │    │  [router/http/mod.rs:649-654]
    │    │  路由直接指向 dispatch()
    │    │  无认证/审计中间件
    │    ▼
    │    dispatch() [router/http/mod.rs:84-114]
    │    ├─ resolve_target() 选择目标节点
    │    └─ proxy_request() / proxy_with_body_routing()
    │       └─ 转发到后端节点
    │          （后端节点执行认证/审计）
    │
    ├─ 4.2 /config/*
    │    │  [handler/http/router/mod.rs:560-570]
    │    │  由 config_routes() 直接处理
    │    │  无认证/审计中间件
    │    ▼
    │    直接响应（如 /config/logout, /config/runtime 等）
    │
    └─ 4.3 /proxy/*
         │  [handler/http/router/mod.rs:455-463]
         │  有 proxy_auth_middleware（限流后认证）
         ▼
         proxy_auth_middleware [L459] ✅ 代码可验证
         │  验证 proxy URL 权限
         ▼
         proxy() [L456]
         └─ 转发到目标 URL
```

> **关键区别**：
> - `/api/*` 等路由：Router 节点不做认证，直接转发
> - `/proxy/*` 路由：Router 节点在限流后还有 `proxy_auth_middleware` 做认证
> - `/config/*` 路由：Router 节点直接处理，不转发

---

### 4.5 分层限流的降级策略

| 策略 | 代码内可验证 | 依赖外部库需确认 | 说明 |
|------|-------------|-----------------|------|
| 四层规则独立匹配 | ✅ | | [router/ratelimit/resource_extractor.rs:141-201] |
| 规则优先级（用户 > 角色 > 组织 > 全局） | ✅ | | [router/ratelimit/resource_extractor.rs:203-262] |
| 四层规则的计数独立累加 | | ✅ | 由 `o2_ratelimit` 内部实现 |
| 快速失败（任意一层超过阈值立即返回） | | ✅ | 由 `o2_ratelimit` 内部实现 |
| 多角色时选 threshold 最大的 | ✅ | | [router/ratelimit/resource_extractor.rs:238] `max_by_key(|rule| rule.threshold)` |

**示例场景**：
```
用户级规则：threshold=10, interval=1s
角色级规则：threshold=1000, interval=1s
组织级规则：threshold=10000, interval=1s
全局默认：threshold=100000, interval=1s

当用户1秒内发起11次请求：
  ⚠️ 用户级计数=11 → 超过阈值10 → 触发429（依赖外部库确认计数逻辑）
  （角色级、组织级、全局默认的检查是否继续执行，依赖外部库实现）
```

---

### 4.6 初始化与配置验证

**初始化入口**：`main.rs` ([main.rs:1584](src/main.rs#L1584-L1587)) ✅ 代码可验证
```rust
if o2cfg.rate_limit.rate_limit_enabled 
   && o2_openfga::config::get_config().enabled {
    o2_ratelimit::init(openapi_info()).await?;
}
```

**配置验证**：`check_ratelimit_config()` ([main.rs:1593](src/main.rs#L1593-L1610)) ✅ 代码可验证
1. 启用限流式必须使用 Nats 作为队列存储
2. 规则刷新间隔必须 ≥ 2秒

---

## 五、核心代码参考

### 5.1 限流规则管理

| 模块 | 文件 | 主要功能 |
|------|------|---------|
| 配置层 | [config/src/meta/ratelimit.rs](src/config/src/meta/ratelimit.rs) | 数据结构定义、资源标识生成 |
| 服务层 | [service/ratelimit/rule.rs](src/service/ratelimit/rule.rs) | 规则CRUD、超集群同步 |
| 持久层 | [infra/src/table/ratelimit.rs](src/infra/src/table/ratelimit.rs) | 数据库操作、事务处理 |
| API层 | [handler/http/request/ratelimit/mod.rs](src/handler/http/request/ratelimit/mod.rs) | HTTP接口、参数校验 |

### 5.2 限流与路由协作

| 模块 | 文件 | 主要功能 |
|------|------|---------|
| 资源提取 | [router/ratelimit/resource_extractor.rs](src/router/ratelimit/resource_extractor.rs) | 从请求提取限流规则、四层匹配 |
| 限流中间件 | [handler/http/router/mod.rs:1106](src/handler/http/router/mod.rs#L1106) | 挂载 RateLimitLayer |
| 路由分发 | [router/http/mod.rs](src/router/http/mod.rs) | 请求转发、节点选择 |
| 超集群同步 | [super_cluster_queue/ratelimit.rs](src/super_cluster_queue/ratelimit.rs) | 规则变更消息处理 |

---

## 六、关键设计决策

1. **Router 节点单一入口**：限流集中在 Router 节点，避免后端节点重复检查
2. **四层分层设计**：从全局到用户的灵活配置，满足不同粒度的限流需求
3. **限流前置**：在路由分发前完成检查，保护后端节点免受流量冲击
4. **Nats 依赖**：利用消息队列实现分布式计数的最终一致性
5. **缓存优先**：规则内存缓存，避免每次请求都查询数据库

---

## 七、附录：完整调用链路深度解析

### A.1 中间件执行顺序详解

**axum 中间件洋葱模型**：后添加的 layer 先执行（从外到内）。

**完整调用链（从请求到响应）**：
```
客户端请求
    │
    ▼
[App 最外层中间件]
    │
    ├─ 1. DefaultBodyLimit [L1140]
    │    代码：app.layer(DefaultBodyLimit::max(cfg.limit.req_payload_limit))
    │    作用：限制请求体大小，防止超大请求攻击
    │
    ├─ 2. cors_layer [L1139]
    │    代码：app.layer(cors_layer())
    │    作用：处理跨域请求，添加 CORS 响应头
    │
    ▼
[router_routes 层中间件]
    │
    ├─ 3. RateLimitLayer [L1105-L1109]
    │    代码：router_routes.layer(RateLimitLayer::new_with_extractor(...))
    │    作用：限流检查（本章节核心）
    │    ├─ 调用 default_extractor 提取规则
    │    ├─ o2_ratelimit 内部配额计数
    │    └─ 超过阈值 → 短路返回 429
    │
    ▼
[路由匹配与处理]
    │
    ├─ 4. 路由匹配
    │    ├─ /api/* → dispatch() [router/http/mod.rs:84]
    │    ├─ /aws/* → dispatch()
    │    ├─ /gcp/* → dispatch()
    │    ├─ /rum/* → dispatch()
    │    ├─ /config/* → config_routes() [L560-L570]
    │    └─ /proxy/* → proxy_auth_middleware → proxy() [L455-L463]
    │
    └─ 5. dispatch() / proxy() 转发到后端节点
         后端节点执行认证、审计等中间件
```

**代码证明**：
| 序号 | 中间件 | 代码位置 | 执行时机 |
|------|--------|---------|---------|
| 1 | DefaultBodyLimit | [handler/http/router/mod.rs:1140](src/handler/http/router/mod.rs#L1140) | 最先执行（最外层） |
| 2 | cors_layer | [handler/http/router/mod.rs:1139](src/handler/http/router/mod.rs#L1139) | DefaultBodyLimit 之后 |
| 3 | RateLimitLayer | [handler/http/router/mod.rs:1105-1109](src/handler/http/router/mod.rs#L1105-L1109) | CORS 之后，路由分发之前 |
| 4 | proxy_auth_middleware | [handler/http/router/mod.rs:459](src/handler/http/router/mod.rs#L459) | 仅 `/proxy/*` 路由，限流之后 |

> **关键结论**：限流中间件是 Router 节点的 **第一道业务关卡**，在路由分发和认证之前执行。

---

### A.2 配额计数责任归属

**责任划分表**（✅ 代码内可验证 / ⚠️ 依赖外部库）：

| 责任模块 | 项目代码（openobserve） | o2_ratelimit 企业库 | 验证状态 |
|---------|------------------------|---------------------|---------|
| **规则数据结构** | ✅ `RatelimitRule` 定义 | | ✅ 代码可验证 |
| | [config/src/meta/ratelimit.rs:32-54](src/config/src/meta/ratelimit.rs#L32-L54) | | |
| **资源标识生成** | ✅ `get_resource_from_params()` | | ✅ 代码可验证 |
| | [config/src/meta/ratelimit.rs:163-172](src/config/src/meta/ratelimit.rs#L163-L172) | | |
| **请求信息提取** | ✅ 从 Request 提取 headers/path/method | | ✅ 代码可验证 |
| | [router/ratelimit/resource_extractor.rs:134-139](src/router/ratelimit/resource_extractor.rs#L134-L139) | | |
| **org_id 提取** | ✅ `extract_org_id()` 解析路径 | | ✅ 代码可验证 |
| | [router/ratelimit/resource_extractor.rs:40-59](src/router/ratelimit/resource_extractor.rs#L40-L59) | | |
| **OpenAPI 分组** | ✅ 调用 `find_group_by_openapi()` | ✅ 提供 `find_group_by_openapi` 函数 | ✅ 代码可验证 |
| | [router/ratelimit/resource_extractor.rs:88](src/router/ratelimit/resource_extractor.rs#L88) | [导入自 o2_ratelimit](src/router/ratelimit/resource_extractor.rs#L26) | |
| **认证信息解析** | ✅ `extract_auth_str_from_headers()` | | ✅ 代码可验证 |
| | [router/ratelimit/resource_extractor.rs:76](src/router/ratelimit/resource_extractor.rs#L76) | | |
| | ✅ `get_user_email_from_auth_str()` | | ✅ 代码可验证 |
| | [router/ratelimit/resource_extractor.rs:77-79](src/router/ratelimit/resource_extractor.rs#L77-L79) | | |
| | ✅ `get_user_roles()` 查询用户角色 | | ✅ 代码可验证 |
| | [router/ratelimit/resource_extractor.rs:83](src/router/ratelimit/resource_extractor.rs#L83) | | |
| **规则匹配逻辑** | ✅ `find_matching_rule()` Exact/Regex 匹配 | | ✅ 代码可验证 |
| | [router/ratelimit/resource_extractor.rs:264-306](src/router/ratelimit/resource_extractor.rs#L264-L306) | | |
| **四层规则选择** | ✅ `select_final_rule_resource()` 优先级逻辑 | | ✅ 代码可验证 |
| | [router/ratelimit/resource_extractor.rs:203-262](src/router/ratelimit/resource_extractor.rs#L203-L262) | | |
| **规则缓存** | ✅ 使用 `RATELIMIT_RULES_CACHE` | ✅ 管理缓存刷新 | ✅ 项目代码侧可验证读取 |
| | [router/ratelimit/resource_extractor.rs:23](src/router/ratelimit/resource_extractor.rs#L23) | | |
| **配额计数** | | ✅ 滑动窗口算法实现 | ⚠️ 依赖外部库 |
| **阈值检查** | | ✅ 四层规则独立检查 | ⚠️ 依赖外部库 |
| **429 降级返回** | | ✅ 构建 HTTP 429 响应 | ⚠️ 依赖外部库 |
| **分布式同步** | | ✅ Nats 消息队列同步 | ⚠️ 依赖外部库 |
| **规则 CRUD API** | ✅ HTTP 接口、参数校验 | | ✅ 代码可验证 |
| | [handler/http/request/ratelimit/mod.rs](src/handler/http/request/ratelimit/mod.rs) | | |
| **超集群同步** | ✅ 处理 `RatelimitAdd/Update/Delete` 消息 | | ✅ 代码可验证 |
| | [super_cluster_queue/ratelimit.rs:22-82](src/super_cluster_queue/ratelimit.rs#L22-L82) | | |

---

### A.3 短路返回触发点详解

**触发位置**：`o2_ratelimit::middleware::RateLimitLayer` 内部

**执行流程**（✅ 代码可验证 / ⚠️ 依赖外部库）：
```
RateLimitLayer 收到 Request
    │
    ├─ 1. 调用 extractor 函数（项目代码提供） ✅ 代码可验证
    │     │  [router/ratelimit/resource_extractor.rs:134-139]
    │     └─ default_extractor(req)
    │         └─ rule_extractor(headers, path, method)
    │             └─ 返回 ExtractorRuleResult 四元组 ✅ 代码可验证
    │                [router/ratelimit/resource_extractor.rs:29] 类型来自 o2_ratelimit
    │
    ├─ 2. o2_ratelimit 内部处理 ⚠️ 依赖外部库
    │     │
    │     ├─ 对四层规则分别执行配额计数 ⚠️
    │     │   (DefaultOrgGlobal, OrgLevel, UserRole, UserId)
    │     │
    │     ├─ 检查每层计数是否超过阈值 ⚠️
    │     │
    │     └─ 任意一层超过阈值 → 触发短路 ⚠️
    │
    ├─ 3A. 未超过阈值 → 调用 next.run(request) ✅ 代码可验证（机制上）
    │     │  axum 中间件机制保证
    │     └─ 继续执行后续中间件和路由分发 ✅
    │
    └─ 3B. 超过阈值 → 短路返回 ✅ 代码可验证（机制上）
          │  不调用 next.run(request) → 后续流程完全终止 ✅
          │
          ├─ 构建 429 Too Many Requests 响应 ⚠️ 依赖外部库
          ├─ 设置 Retry-After 响应头 ⚠️ 依赖外部库
          ├─ 构建错误详情 JSON 响应体 ⚠️ 依赖外部库
          └─ 直接返回 Response ✅ 代码可验证（机制上）
```

**代码证明**：
1. **extractor 函数签名** ✅ 代码可验证：
   ```rust
   // [router/ratelimit/resource_extractor.rs:134-139]
   pub fn default_extractor(req: &axum::extract::Request) 
       -> BoxFuture<'_, ExtractorRuleResult>
   ```
   返回 `ExtractorRuleResult` 四元组给 `o2_ratelimit`。

2. **RateLimitLayer 构造** ✅ 代码可验证：
   ```rust
   // [handler/http/router/mod.rs:1105-1109]
   RateLimitLayer::new_with_extractor(Some(
       crate::router::ratelimit::resource_extractor::default_extractor,
   ))
   ```
   项目代码只提供 extractor，计数和检查由库内部完成。

3. **ExtractorRuleResult 类型** ✅ 代码可验证：
   ```rust
   // [router/ratelimit/resource_extractor.rs:29]
   use o2_ratelimit::middleware::{ExtractorRule, ExtractorRuleResult};
   ```
   类型定义来自 `o2_ratelimit`，说明结果返回给库处理。

4. **项目代码无 429 处理** ✅ 代码可验证：
   - 全项目搜索无 `StatusCode::TOO_MANY_REQUESTS` 引用
   - 全项目搜索无 `429` 状态码引用
   - 全项目搜索无 `Retry-After` 响应头引用

> **关键结论**：
> - ✅ 可验证：429 短路返回的**机制**（不调用 next.run 导致后续流程终止）由 axum 中间件保证
> - ⚠️ 需确认：429 响应的**具体内容**（状态码、响应头、响应体）完全在 `o2_ratelimit` 库内部构建，项目代码无直接证据

---

### A.4 限流与路由分发的顺序关系

**Router 节点 vs 非 Router 节点对比**（✅ 代码可验证）：

| 阶段 | Router 节点（代理模式） | 非 Router 节点（直连模式） |
|------|------------------------|--------------------------|
| 1 | DefaultBodyLimit ✅ [L1140] | DefaultBodyLimit ✅ [L1140] |
| 2 | CORS ✅ [L1139] | CORS ✅ [L1139] |
| 3 | **限流检查** (RateLimitLayer) ✅ [L1105] | 预编码处理 ✅ [L1027-1029] |
| 4 | 路由匹配 → dispatch() ✅ [L649-654] | 请求解压 ✅ [L1026] |
| 5 | 转发到后端节点 ✅ [L84-114] | **认证中间件** ✅ [L1025] |
| 6 | （后端节点执行认证） | **审计中间件** ✅ [L1024] |
| 7 | （后端节点执行业务） | 组织拦截 ✅ [L1023] |
| 8 | 返回响应 | 业务处理 |

> **注意**：认证/审计中间件是**非 Router 节点**的 `service_routes()` 的中间件链，不要混淆到 Router 节点。
> 代码：[handler/http/router/mod.rs:1022-1041](src/handler/http/router/mod.rs#L1022-L1041)

**代码证明 - Router 节点无认证中间件** ✅ 代码可验证：
```rust
// [router/http/mod.rs:645-655]
pub fn create_router_routes() -> axum::Router {
    axum::Router::new()
        .route("/config", any(dispatch))
        .route("/config/{*path}", any(dispatch))
        .route("/api/{*path}", any(dispatch))
        .route("/aws/{*path}", any(dispatch))
        .route("/gcp/{*path}", any(dispatch))
        .route("/rum/{*path}", any(dispatch))
    // 注意：这里没有 .layer(auth_middleware)！
    // 认证/审计中间件在 service_routes() 中，非 Router 节点使用
}
```

**代码证明 - dispatch() 直接转发** ✅ 代码可验证：
```rust
// [router/http/mod.rs:84-114]
pub async fn dispatch(req: Request) -> Response {
    // 1. 选择目标节点
    let target = match resolve_target(&path, &cfg.common.base_uri).await {
        Ok(target) => target,
        Err(error) => return (StatusCode::SERVICE_UNAVAILABLE, error).into_response(),
    };
    // 2. 直接转发，不做认证
    if is_querier_route_by_body(&path) {
        proxy_with_body_routing(req, target, start).await
    } else {
        proxy_request(req, target, start).await
    }
}
```

**代码证明 - 非 Router 节点有完整中间件链** ✅ 代码可验证：
```rust
// [handler/http/router/mod.rs:1022-1041] service_routes() 的中间件
router
    .layer(middleware::from_fn(blocked_orgs_middleware))      // 组织拦截
    .layer(middleware::from_fn(audit_middleware))             // 审计
    .layer(middleware::from_fn(auth_middleware))              // 认证
    .layer(RequestDecompressionLayer::new())                   // 解压
    .layer(middleware::from_fn(preprocess_encoding_middleware)) // 预编码处理
    // ...
```

> **关键结论**：
> - ✅ 可验证：Router 节点的限流在路由分发**之前**，且 Router 节点本身不做认证
> - ✅ 可验证：认证/审计中间件是**非 Router 节点**的 `service_routes()` 的中间件链
> - ✅ 可验证：被限流的请求不会触发 `dispatch()`，也不会触发后端节点的认证检查
> - 只有 `/proxy/*` 路由在限流后还有 `proxy_auth_middleware` 做认证 ✅ [L459]

---

### A.5 限流覆盖的路由范围

**经过限流的路由**（RateLimitLayer 包裹）✅ 代码可验证：
| 路由前缀 | 处理器 | 代码位置 |
|---------|--------|---------|
| `/config/*` | `config_routes()` | [L560-L570](src/handler/http/router/mod.rs#L560-L570) |
| `/api/*` | `dispatch()` | [router/http/mod.rs:649](src/router/http/mod.rs#L649) |
| `/aws/*` | `dispatch()` | [router/http/mod.rs:652](src/router/http/mod.rs#L652) |
| `/gcp/*` | `dispatch()` | [router/http/mod.rs:653](src/router/http/mod.rs#L653) |
| `/rum/*` | `dispatch()` | [router/http/mod.rs:654](src/router/http/mod.rs#L654) |
| `/proxy/*` | `proxy()` + `proxy_auth_middleware` | [L455-L463](src/handler/http/router/mod.rs#L455-L463) |

**不经过限流的路由**（在 `basic_routes()` 中，未被 RateLimitLayer 包裹）✅ 代码可验证：
| 路由前缀 | 说明 |
|---------|------|
| `/healthz` | 健康检查 |
| `/schedulez` | 调度检查 |
| `/metrics` | 指标接口 |
| `/node/*` | 节点管理 |
| `/debug/*` | 调试接口（profiling） |
| `/swagger` | Swagger UI |
| `/docs` | API 文档重定向 |
| `/web/*` | UI 前端资源 |

**代码证明** ✅ 代码可验证：
```rust
// [handler/http/router/mod.rs:1092-1113]
if config::cluster::LOCAL_NODE.is_router() {
    let mut router_routes = Router::new();
    router_routes = router_routes
        .merge(crate::router::http::create_router_routes())  // /api/*, /aws/* 等
        .nest("/config", config_routes())                     // /config/*
        .merge(proxy_routes(true));                           // /proxy/*

    // 限流加在 router_routes 上
    router_routes = router_routes.layer(RateLimitLayer::new_with_extractor(...));

    // basic_routes 在外层 merge，不经过限流
    Router::new().merge(basic_routes()).merge(router_routes)
}
```

> **验证方法**：
> 1. ✅ 可验证：`basic_routes()` 返回的路由在 `Router::new().merge(basic_routes()).merge(router_routes)` 中先 merge，说明不在 `router_routes` 的限流 layer 包裹范围内
> 2. ✅ 可验证：`router_routes` 在 merge 完所有业务路由后才 layer 限流中间件，说明这些业务路由都经过限流
> 3. ✅ 可验证：`create_router_routes()` 返回的路由直接指向 `dispatch()`，无认证中间件
> 4. ✅ 可验证：`proxy_routes(true)` 返回的路由有 `proxy_auth_middleware`，在限流之后执行

