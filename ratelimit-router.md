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
│  限流中间件 (RateLimitLayer)                               │
│      │  • 调用 default_extractor 提取规则                  │
│      │  • o2_ratelimit 内部执行配额计数检查                 │
│      │                                                    │
│      ├─ 超过阈值 → 返回 429 Too Many Requests ◀───┐       │
│      │                                            │       │
│      ▼                                            │       │
│  认证中间件                                        │       │
│      │                                            │       │
│      ▼                                            │       │
│  审计中间件                                        │       │
│      │                                            │       │
│      ▼                                            │       │
│  resolve_target() 确定目标节点                     │       │
│      │  • 判断是 Querier 还是 Ingester 路由        │       │
│      │  • 选择对应节点池                           │       │
│      │  • 按策略选择节点（随机/最佳节点）          │       │
│      │                                            │       │
│      ▼                                            │       │
│  proxy_request() / proxy_with_body_routing()       │       │
│      │  • 转发请求到后端节点                       │       │
│      │  • 处理流式响应                             │       │
│      │                                            │       │
│      ▼                                            │       │
│  返回响应给客户端 ◀────────────────────────────────┘       │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

**关键衔接点**：
- 限流检查在 **最外层**，被限流的请求不会消耗后端资源
- 限流通过后，才会执行 `resolve_target()` 进行路由决策
- 对于需要解析请求体的路由（如搜索），限流在 body 解析前完成

---

## 四、降级响应逻辑

### 4.1 触发条件

当任意一层限流规则的计数超过阈值时，触发降级响应。`o2_ratelimit` 中间件内部会：
1. 对四层规则分别进行计数累加
2. 检查是否有任意一层超过阈值
3. 只要有一层超过阈值，立即触发降级

### 4.2 降级响应内容

由 `o2_ratelimit` 库内部处理，返回标准的 HTTP 429 响应：
- **状态码**：`429 Too Many Requests`
- **响应头**：包含 `Retry-After`（建议重试等待时间）
- **响应体**：包含错误详情的 JSON

### 4.3 分层限流的降级策略

**多层规则同时存在时的检查逻辑**：
- **并行检查**：四层规则的计数独立累加，并行检查
- **快速失败**：任意一层超过阈值立即返回
- **最严优先**：实际上用户级规则阈值通常最小，会最先触发

**示例场景**：
```
用户级规则：threshold=10, interval=1s
角色级规则：threshold=1000, interval=1s
组织级规则：threshold=10000, interval=1s
全局默认：threshold=100000, interval=1s

当用户1秒内发起11次请求：
  ✓ 用户级计数=11 → 超过阈值10 → 触发429
  （角色级、组织级、全局默认的检查不再执行）
```

### 4.4 初始化与配置验证

**初始化入口**：`main.rs` ([main.rs:1584](src/main.rs#L1584-L1587))
```rust
if o2cfg.rate_limit.rate_limit_enabled 
   && o2_openfga::config::get_config().enabled {
    o2_ratelimit::init(openapi_info()).await?;
}
```

**配置验证**：`check_ratelimit_config()` ([main.rs:1593](src/main.rs#L1593-L1610))
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

**责任划分表**：

| 责任模块 | 项目代码（openobserve） | o2_ratelimit 企业库 |
|---------|------------------------|---------------------|
| **规则数据结构** | ✓ `RatelimitRule` 定义 | |
| | [config/src/meta/ratelimit.rs:32-54](src/config/src/meta/ratelimit.rs#L32-L54) | |
| **资源标识生成** | ✓ `get_resource_from_params()` | |
| | [config/src/meta/ratelimit.rs:163-172](src/config/src/meta/ratelimit.rs#L163-L172) | |
| **请求信息提取** | ✓ 从 Request 提取 headers/path/method | |
| | [router/ratelimit/resource_extractor.rs:134-139](src/router/ratelimit/resource_extractor.rs#L134-L139) | |
| **org_id 提取** | ✓ `extract_org_id()` 解析路径 | |
| | [router/ratelimit/resource_extractor.rs:40-59](src/router/ratelimit/resource_extractor.rs#L40-L59) | |
| **OpenAPI 分组** | ✓ 调用 `find_group_by_openapi()` | ✓ 提供 `find_group_by_openapi` 函数 |
| | [router/ratelimit/resource_extractor.rs:88](src/router/ratelimit/resource_extractor.rs#L88) | [导入自 o2_ratelimit](src/router/ratelimit/resource_extractor.rs#L26) |
| **认证信息解析** | ✓ `extract_auth_str_from_headers()` | |
| | [router/ratelimit/resource_extractor.rs:76](src/router/ratelimit/resource_extractor.rs#L76) | |
| | ✓ `get_user_email_from_auth_str()` | |
| | [router/ratelimit/resource_extractor.rs:77-79](src/router/ratelimit/resource_extractor.rs#L77-L79) | |
| | ✓ `get_user_roles()` 查询用户角色 | |
| | [router/ratelimit/resource_extractor.rs:83](src/router/ratelimit/resource_extractor.rs#L83) | |
| **规则匹配逻辑** | ✓ `find_matching_rule()` Exact/Regex 匹配 | |
| | [router/ratelimit/resource_extractor.rs:264-306](src/router/ratelimit/resource_extractor.rs#L264-L306) | |
| **四层规则选择** | ✓ `select_final_rule_resource()` 优先级逻辑 | |
| | [router/ratelimit/resource_extractor.rs:203-262](src/router/ratelimit/resource_extractor.rs#L203-L262) | |
| **规则缓存** | ✓ 使用 `RATELIMIT_RULES_CACHE` | ✓ 管理缓存刷新 |
| | [router/ratelimit/resource_extractor.rs:23](src/router/ratelimit/resource_extractor.rs#L23) | |
| **配额计数** | | ✓ 滑动窗口算法实现 |
| **阈值检查** | | ✓ 四层规则独立检查 |
| **429 降级返回** | | ✓ 构建 HTTP 429 响应 |
| **分布式同步** | | ✓ Nats 消息队列同步 |
| **规则 CRUD API** | ✓ HTTP 接口、参数校验 | |
| | [handler/http/request/ratelimit/mod.rs](src/handler/http/request/ratelimit/mod.rs) | |
| **超集群同步** | ✓ 处理 `RatelimitAdd/Update/Delete` 消息 | |
| | [super_cluster_queue/ratelimit.rs:22-82](src/super_cluster_queue/ratelimit.rs#L22-L82) | |

---

### A.3 短路返回触发点详解

**触发位置**：`o2_ratelimit::middleware::RateLimitLayer` 内部

**执行流程**：
```
RateLimitLayer 收到 Request
    │
    ├─ 1. 调用 extractor 函数（项目代码提供）
    │     │
    │     └─ default_extractor(req)
    │         └─ rule_extractor(headers, path, method)
    │             └─ 返回 ExtractorRuleResult 四元组
    │
    ├─ 2. o2_ratelimit 内部处理（黑盒）
    │     │
    │     ├─ 对四层规则分别执行配额计数
    │     │   (DefaultOrgGlobal, OrgLevel, UserRole, UserId)
    │     │
    │     ├─ 检查每层计数是否超过阈值
    │     │
    │     └─ 任意一层超过阈值 → 触发短路
    │
    ├─ 3A. 未超过阈值 → 调用 next.run(request)
    │     │
    │     └─ 继续执行后续中间件和路由分发
    │
    └─ 3B. 超过阈值 → 短路返回
          │
          ├─ 构建 429 Too Many Requests 响应
          ├─ 设置 Retry-After 响应头
          ├─ 构建错误详情 JSON 响应体
          └─ 直接返回 Response（不调用 next.run）
```

**代码证明**：
1. **extractor 函数签名**：
   ```rust
   // [router/ratelimit/resource_extractor.rs:134-139]
   pub fn default_extractor(req: &axum::extract::Request) 
       -> BoxFuture<'_, ExtractorRuleResult>
   ```
   返回 `ExtractorRuleResult` 四元组给 `o2_ratelimit`。

2. **RateLimitLayer 构造**：
   ```rust
   // [handler/http/router/mod.rs:1105-1109]
   RateLimitLayer::new_with_extractor(Some(
       crate::router::ratelimit::resource_extractor::default_extractor,
   ))
   ```
   项目代码只提供 extractor，计数和检查由库内部完成。

3. **ExtractorRuleResult 类型**：
   ```rust
   // [router/ratelimit/resource_extractor.rs:29]
   use o2_ratelimit::middleware::{ExtractorRule, ExtractorRuleResult};
   ```
   类型定义来自 `o2_ratelimit`，说明结果返回给库处理。

> **关键结论**：429 短路返回完全在 `o2_ratelimit` 库内部触发，项目代码不直接处理 429 响应构建。

---

### A.4 限流与路由分发的顺序关系

**Router 节点 vs 非 Router 节点对比**：

| 阶段 | Router 节点（代理模式） | 非 Router 节点（直连模式） |
|------|------------------------|--------------------------|
| 1 | DefaultBodyLimit | DefaultBodyLimit |
| 2 | CORS | CORS |
| 3 | **限流检查** (RateLimitLayer) | 预编码处理 |
| 4 | 路由匹配 → dispatch() | 请求解压 |
| 5 | 转发到后端节点 | **认证中间件** |
| 6 | （后端节点执行认证） | **审计中间件** |
| 7 | （后端节点执行业务） | 组织拦截 |
| 8 | 返回响应 | 业务处理 |

**代码证明 - Router 节点无认证中间件**：
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
}
```

**代码证明 - dispatch() 直接转发**：
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

> **关键结论**：Router 节点的限流在路由分发**之前**，且 Router 节点本身不做认证，认证由后端节点完成。这意味着被限流的请求甚至不会触发后端节点的认证检查。

---

### A.5 限流覆盖的路由范围

**经过限流的路由**（RateLimitLayer 包裹）：
| 路由前缀 | 处理器 | 代码位置 |
|---------|--------|---------|
| `/config/*` | `config_routes()` | [L560-L570](src/handler/http/router/mod.rs#L560-L570) |
| `/api/*` | `dispatch()` | [router/http/mod.rs:649](src/router/http/mod.rs#L649) |
| `/aws/*` | `dispatch()` | [router/http/mod.rs:652](src/router/http/mod.rs#L652) |
| `/gcp/*` | `dispatch()` | [router/http/mod.rs:653](src/router/http/mod.rs#L653) |
| `/rum/*` | `dispatch()` | [router/http/mod.rs:654](src/router/http/mod.rs#L654) |
| `/proxy/*` | `proxy()` + `proxy_auth_middleware` | [L455-L463](src/handler/http/router/mod.rs#L455-L463) |

**不经过限流的路由**（在 `basic_routes()` 中，未被 RateLimitLayer 包裹）：
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

**代码证明**：
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

