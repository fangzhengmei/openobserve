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
请求 → CORS → 限流中间件 → 认证中间件 → 审计中间件 → 路由分发
```

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

