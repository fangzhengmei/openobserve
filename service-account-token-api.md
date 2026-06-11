# OpenObserve 服务账户 Token 与 API 安全模型分析

## 1. 核心数据模型

### 1.1 UserRole 枚举

定义于 [user.rs](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/config/src/meta/user.rs#L58-L71)：

```rust
pub enum UserRole {
    Root = 0,
    Admin = 1,
    Editor = 2,
    Viewer = 3,
    User = 4,
    ServiceAccount = 5,
    SreAgent = 6,
}
```

服务账户存在两种角色变体：
- **ServiceAccount** (值=5)：用户创建的服务账户，用于自动化系统程序化访问
- **SreAgent** (值=6)：系统管理的 SRE Agent 服务账户，只读访问遥测/告警/事件

`is_service_account()` 方法（[user.rs#L126-L128](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/config/src/meta/user.rs#L126-L128)）对两者均返回 `true`。

### 1.2 SRE Agent 邮件模式

常量定义于 [user.rs#L27-L28](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/config/src/meta/user.rs#L27-L28)：

```rust
pub const SRE_AGENT_EMAIL_PREFIX: &str = "o2-sre-agent.org-";
pub const SRE_AGENT_EMAIL_SUFFIX: &str = "@openobserve.internal";
```

判断函数 `is_system_service_account()`（[organization.rs#L1009-L1011](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/organization.rs#L1009-L1011)）：如果 email 以 `o2-sre-agent.org-` 开头且以 `@openobserve.internal` 结尾，则为系统服务账户。

### 1.3 OrgUserRecord 与 allow_static_token

定义于 [org_users.rs#L44-L52](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/infra/src/table/org_users.rs#L44-L52)：

```rust
pub struct OrgUserRecord {
    pub email: String,
    pub org_id: String,
    pub role: UserRole,
    pub token: String,
    pub rum_token: Option<String>,
    pub created_at: i64,
    pub allow_static_token: bool,  // 关键安全标志
}
```

**`allow_static_token`** 是服务账户安全模型的核心控制点：
- `true`（默认）：允许直接使用静态 token 进行 Basic Auth
- `false`：禁止直接使用静态 token，必须通过 `assume_service_account` API 获取临时会话 token

该字段通过数据库迁移 `m20251230_000001_add_allow_static_token_to_org_users` 添加，默认值为 `true` 以保持向后兼容。

### 1.4 Service Account 请求/响应结构

定义于 [service_account.rs#L19-L40](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/common/meta/service_account.rs#L19-L40)：

- `ServiceAccountRequest`：创建请求，包含 email、first_name、last_name
- `ServiceAccountCreateResponse`：创建响应，包含 code、message、token、user
- `UpdateServiceAccountRequest`：更新请求，包含 first_name、last_name
- `APIToken`：token 轮换响应，包含 token、user

---

## 2. 服务账户创建流程

### 2.1 API 路由

注册于 [router/mod.rs#L838-L841](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/router/mod.rs#L838-L841)：

```
GET    /{org_id}/service_accounts           → list
POST   /{org_id}/service_accounts           → save
PUT    /{org_id}/service_accounts/{email_id} → update
DELETE /{org_id}/service_accounts/{email_id} → delete
DELETE /{org_id}/service_accounts/bulk       → delete_bulk
```

### 2.2 创建逻辑

入口函数 `save()`（[service_accounts/mod.rs#L144-L172](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/service_accounts/mod.rs#L144-L172)）：

1. **全局开关检查**：`config.auth.service_account_enabled` 必须为 `true`（环境变量 `ZO_SERVICE_ACCOUNT_ENABLED`，默认 `true`）
2. **构造 UserRequest**：将 `ServiceAccountRequest` 转为 `UserRequest`，关键设置：
   - `password`：`generate_random_string(16)` — 随机生成，用户不可见
   - `role.base_role`：强制设为 `UserRole::ServiceAccount`
3. **调用 `users::post_user()`**（[users.rs#L73-L235](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L73-L235)）：
   - 邮箱验证与去重
   - 生成 salt（`ider::uuid()`）、密码哈希、**token**（`generate_random_string(16)`）
   - 创建 `DBUser` 记录，存入数据库
   - **Enterprise 特有**：若 OpenFGA 启用，写入 OFGA 关系元组，包括 `get_service_account_creation_tuple`
   - 返回 `ServiceAccountCreateResponse`，**一次性返回明文 token**

### 2.3 系统服务账户（SRE Agent）创建

由 `ensure_sys_rca_agent()`（[organization.rs#L1027-L1079](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/organization.rs#L1027-L1079)）自动创建：

1. 邮件格式：`o2-sre-agent.org-{org_id}@openobserve.internal`（小写）
2. 创建 `users` 表记录（`create_service_account_if_not_exists`）
3. 通过 `db::org_users::add_with_flags()` 添加到组织，**`allow_static_token = true`**
4. 角色：`UserRole::SreAgent`
5. 同步更新 OpenFGA 关系

---

## 3. Token 生成、存储与校验

### 3.1 Token 生成

服务账户的 token 由 `generate_random_string(16)` 生成（[users.rs#L166](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L166)），是一串 16 字符的随机字符串。

与个人用户 token 生成方式完全相同，但语义不同：
- 个人用户的 token 主要用于**摄入端点**（Ingestion Endpoint）
- 服务账户的 token 用于**所有 API 请求**的身份认证

### 3.2 Token 存储

Token 存储在 **`org_users` 表**中（不是 `users` 表），以组织为单位：

```
org_users { org_id, email, role, token, rum_token, allow_static_token, created_at }
```

这意味着：
- 同一服务账户在不同组织中拥有**不同的 token**
- Token 的作用域天然限定在组织级别
- 内存缓存 `ORG_USERS`（DashMap）维护 `org_id/email → OrgUserRecord` 映射

此外，`USERS_RUM_TOKEN` 缓存维护 `org_id/token → OrgUserRecord` 的反向映射，用于通过 token 快速查找用户（[users.rs#L733-L775](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L733-L775)）。

### 3.3 Token 校验流程

核心校验函数 `validate_credentials()`（[validator.rs#L227-L450](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L227-L450)）：

```
请求 → oo_validator_internal() → 解析 Auth 头
    → Basic Auth: base64 解码为 username:password
    → validator() → validate_credentials()
```

服务账户 token 校验的关键路径（[validator.rs#L326-L367](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L326-L367)）：

```rust
if user.role.is_service_account() && user.token.eq(&user_password) {
    // 1. 检查服务账户全局开关
    if !config.auth.service_account_enabled { return invalid; }

    // 2. 检查 allow_static_token（仅非会话请求）
    if !from_session
        && let Ok(org_user) = db::org_users::get(&user.org, &user.email).await
        && !org_user.allow_static_token
    {
        log::warn!("...attempted direct token auth but allow_static_token=false...");
        return invalid;
    }

    return Ok(build_token_validation_response(&user));
}
```

**校验优先级**：服务账户 token 校验在密码校验之前执行，且**不受 native_login_enabled / root_only_login 限制**。

### 3.4 Session 标记机制

从 `assume_service_account` 获取的临时会话 token 在认证系统中以 `Session::` 前缀标记（[auth.rs#L886-L895](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/common/utils/auth.rs#L886-L895)）：

```
Session::<session_id>::<actual_token>
```

此标记使得 `bypass_check = true`，从而绕过 `allow_static_token` 检查（[validator.rs#L909](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L909)），即通过 assume 获取的会话 token 即使原始服务账户设置了 `allow_static_token=false` 也能正常使用。

---

## 4. Token 有效期与吊销机制

### 4.1 静态 Token（无有效期）

服务账户的静态 token（存储在 `org_users.token` 中）**没有内置有效期**，一旦创建，永久有效直到：
- 被轮换（rotate）
- 服务账户被删除

### 4.2 Token 轮换（rotate）

通过 `PUT /{org_id}/service_accounts/{email_id}?rotateToken=true` 触发（[service_accounts/mod.rs#L226-L279](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/service_accounts/mod.rs#L226-L279)）：

1. **权限检查**：
   - Enterprise + OpenFGA：RBAC 中间件已校验
   - 其他情况：显式检查调用者须为 Admin 或 Root
2. **系统账户保护**：SreAgent 角色的服务账户不允许轮换
3. **调用 `update_passcode()`**（[organization.rs#L238-L296](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/organization.rs#L238-L296)）：
   - 生成新的 `generate_random_string(16)` token
   - 调用 `org_users::update_token()` 更新数据库
   - 旧 token 立即失效
4. **返回新 token**：通过 `APIToken` 响应体返回

### 4.3 会话 Token（assume_service_account）

`assume_service_account` API 创建的临时会话 token **有明确的有效期**：

1. **最大有效期**：24 小时（在 enterprise 实现中 cap）
2. **存储**：通过 `db::session::set_with_expiry()` 写入 `sessions` 表（[session.rs#L125-L151](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/db/session.rs#L125-L151)）
3. **过期检查**：每次读取时检查 `expires_at`，过期则从缓存和数据库中删除
4. **清理机制**：`cleanup_expired()` 定期批量清理过期会话

### 4.4 服务账户删除

`DELETE /{org_id}/service_accounts/{email_id}`（[service_accounts/mod.rs#L332-L346](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/service_accounts/mod.rs#L332-L346)）：

- 调用 `users::remove_user_from_org()`
- 系统服务账户（SreAgent）**不可删除**（[users.rs#L966-L975](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L966-L975)）
- 删除后 token 立即失效
- Enterprise：同步删除 OpenFGA 关系（包括 `delete_service_account_from_org`）

### 4.5 吊销路径总结

| 吊销方式 | 影响范围 | 即时性 |
|---------|---------|-------|
| Token 轮换 (rotateToken) | 当前组织内的该服务账户 token | 即时 |
| 删除服务账户 | 所有组织中的该服务账户 | 即时 |
| 会话自然过期 | assume_service_account 的临时会话 | 到期时 |
| `allow_static_token=false` | 禁止直接使用静态 token | 即时（已存在的会话不受影响） |

---

## 5. 组织与角色范围

### 5.1 服务账户的组织作用域

服务账户以**组织为粒度**管理：
- 创建时绑定到特定组织（`/{org_id}/service_accounts`）
- 在不同组织中拥有独立的 token 和角色
- `OrgUserRecord` 中 `org_id + email` 为联合唯一键

### 5.2 角色设定

- 用户创建的服务账户：`UserRole::ServiceAccount`
- 系统管理的 SRE Agent：`UserRole::SreAgent`
- 在 OpenFGA 中，ServiceAccount 和 User 映射为 `allowed_user` 角色（[users.rs#L511-L513](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L511-L513)）
- 支持自定义角色（custom_role），但需 Enterprise + OpenFGA 启用

### 5.3 权限校验流程

```
validate_credentials() → 验证身份
    → validator() → check_permissions()
        → OpenFGA is_allowed() 检查
```

`check_permissions()`（[validator.rs#L1035-L1100](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L1035-L1100)）：
- Root 用户直接 bypass
- 非 Enterprise 版本：始终返回 `true`
- Enterprise + OpenFGA：调用 `o2_openfga::authorizer::authz::is_allowed()` 做 RBAC 检查
- 服务账户在 OFGA 中角色名为 `allowed_user`，与普通 `User` 角色相同

### 5.4 Token 掩码机制

`should_mask_token()`（[organization.rs#L66-L80](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/organization.rs#L66-L80)）：

当 `allow_static_token=false` 时，API 响应中返回 `NOT_AVAILABLE` 而非真实 token，防止静态 token 被暴露。这在组织设置中创建的 tenant admin 服务账户中生效（[organization.rs#L420-L421](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/organization.rs#L420-L421)）。

---

## 6. assume_service_account 机制

### 6.1 API 定义

```
POST /api/_meta/organizations/assume_service_account
```

仅 Enterprise 版本可用（[assume_service_account.rs#L87-L210](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/organization/assume_service_account.rs#L87-L210)）。

### 6.2 请求参数

```json
{
    "org_id": "target_org",
    "service_account": "sa_email@example.com",  // 可选，默认为调用者自身
    "duration_seconds": 3600  // 最大 86400 (24h)
}
```

### 6.3 安全校验

1. **org_id 必须为 `_meta`**：API 只能在 `_meta` 组织上调用
2. **调用者身份**：仅 _meta 组织中的服务账户可以调用
3. **目标服务账户必须在两个组织中均存在**：_meta 和目标组织
4. **角色验证**：服务账户必须在目标组织中拥有适当角色

### 6.4 会话创建流程

1. Enterprise 层处理验证逻辑（`o2_enterprise::enterprise::assume_service_account::process_assume_service_account`）
2. 生成 session_id 和临时 token
3. 创建 Basic Auth 凭证：`Basic base64(service_account_email:temp_token)`
4. 调用 `db::session::set_with_expiry(session_id, auth_token, expires_at)` 存储
5. 同步到协调器和 Super Cluster（如启用）
6. 返回 `AssumeServiceAccountResponse`：包含 session_id、org_id、role_name、expires_at、expires_in

### 6.5 会话使用

客户端使用返回的 session_id 作为认证：
- 在 Cookie 或 Authorization 头中使用 `session <session_id>`
- 系统从 `sessions` 表解析出 `Basic base64(email:token)` 格式的凭证
- 添加 `Session::` 前缀标记，使 `bypass_check=true`，绕过 `allow_static_token` 限制

---

## 7. 服务账户 Token 与个人用户 Token 的区别

| 维度 | 服务账户 Token | 个人用户 Token |
|-----|-------------|------------|
| **角色** | `ServiceAccount` 或 `SreAgent` | `Root/Admin/Editor/Viewer/User` |
| **认证方式** | Basic Auth（email:token） | Basic Auth（email:token）或 Bearer（JWT） |
| **密码** | 随机生成，用户不可见 | 用户自行设置 |
| **Token 用途** | 所有 API 请求 | 主要用于摄入端点 |
| **全局开关** | `ZO_SERVICE_ACCOUNT_ENABLED` 控制 | 无 |
| **allow_static_token** | 可设为 `false` 强制使用 assume | 不适用 |
| **native_login 限制** | 不受影响 | 受 `native_login_enabled` 和 `root_only_login` 限制 |
| **有效期** | 静态 token 无有效期 | 静态 token 无有效期 |
| **会话有效期** | assume_service_account 最大 24h | Dex/OAuth JWT 有 exp 声明 |
| **轮换权限** | Admin/Root 可轮换他人 | 只能轮换自己 |
| **系统账户保护** | SreAgent 不可修改/删除 | Root 不可被他人修改 |
| **Token 列表展示** | 列出时显示脱敏 token（`abcd********`） | 不显示 token |
| **OFGA 映射** | 映射为 `allowed_user` + 额外 service_account 关系 | 直接映射为角色名 |

### 7.1 Token 校验路径差异

个人用户请求的校验流程：

```
Basic Auth → validate_credentials()
    → 查找用户 → 密码哈希比对（password + salt）
    → native_login_enabled 检查
    → root_only_login 检查
    → 返回 TokenValidationResponse
```

服务账户请求的校验流程：

```
Basic Auth → validate_credentials()
    → 查找用户 → user.role.is_service_account() 检查
    → token 直接比对（user.token == user_password）
    → service_account_enabled 检查
    → allow_static_token 检查（非 Session 请求）
    → 返回 TokenValidationResponse（不经过密码/登录限制）
```

**关键差异**：服务账户使用 token 直接比对而非密码哈希比对，且跳过 native_login 限制。

---

## 8. 审计日志如何区分两者

### 8.1 审计系统架构

审计日志通过 Enterprise 模块的 `o2_enterprise::enterprise::common::auditor` 实现（[self_reporting/mod.rs#L444-L446](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/self_reporting/mod.rs#L444-L446)）：

```rust
pub async fn audit(msg: auditor::AuditMessage) {
    auditor::audit(META_ORG_ID, msg, publish_audit).await;
}
```

### 8.2 AuditMessage 结构

```rust
AuditMessage {
    user_email: String,      // 区分身份的关键字段
    org_id: String,
    _timestamp: i64,
    protocol: Protocol,
    response_meta: ResponseMeta {
        http_method: String,
        http_path: String,
        http_body: String,
        http_query_params: String,
        http_response_code: u16,
        error_msg: Option<String>,
        trace_id: Option<String>,
    },
}
```

### 8.3 区分机制

审计日志通过以下方式区分服务账户和个人用户：

1. **user_email 字段**：
   - 系统服务账户的 email 匹配 `o2-sre-agent.org-*@openobserve.internal` 模式
   - 用户创建的服务账户 email 由用户指定
   - 审计日志消费方可通过 email 模式或查询 `is_system_service_account()` 函数判断

2. **Token 交换审计**（[service_accounts.rs#L30-L69](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/users/service_accounts.rs#L30-L69)）：
   - `exchange_token` 端点在审计时从 JWT 中提取 `user_email`
   - 通过 Dex OAuth 流程认证的用户（包括通过 Bearer token 的服务账户），审计日志记录的是 Dex 返回的 user_email

3. **Usage 日志**：
   - `UsageData` 中的 `user_email` 字段记录发起请求的用户/服务账户身份
   - 服务账户的 `user_email` 可被查询方用于过滤统计

4. **is_system 标记**（API 响应层面）：
   - `UserResponse` 中的 `is_system: bool` 字段标识系统服务账户
   - `description: Option<String>` 字段提供说明（如 "Used by the AI SRE Agent."）

### 8.4 间接区分方式

由于 `AuditMessage` 本身没有 `is_service_account` 布尔字段，区分主要依赖：
- 查询时关联 `org_users` 表的 `role` 字段
- Email 模式匹配（系统服务账户）
- API 路径区分（`/service_accounts` 端点的操作明确是服务账户相关）

---

## 9. 安全配置汇总

| 配置项 | 环境变量 | 默认值 | 说明 |
|-------|---------|-------|------|
| 服务账户开关 | `ZO_SERVICE_ACCOUNT_ENABLED` | `true` | 全局启用/禁用服务账户 |
| 会话清理间隔 | `ZO_SESSION_CLEANUP_INTERVAL` | `3600` | 秒，过期会话清理频率 |
| 会话默认过期 | `ZO_SESSION_DEFAULT_EXPIRY_HOURS` | `24` | 小时，会话迁移默认过期时间 |
| 扩展认证盐 | `ZO_EXT_AUTH_SALT` | `openobserve` | password_ext 哈希盐值 |
| Cookie 最大年龄 | `ZO_COOKIE_MAX_AGE` | `2592000` | 秒（30天） |

---

## 10. 关键安全考量

1. **Token 一次性展示**：服务账户创建时 token 仅在响应中返回一次，后续无法再次获取明文
2. **静态 token 风险**：默认 `allow_static_token=true`，静态 token 无有效期，存在泄露风险
3. **assume_service_account 安全**：
   - 仅限 `_meta` 组织调用
   - 会话有最大 24 小时有效期
   - 会话存储在 DB 中，可被主动删除
4. **系统账户保护**：SreAgent 角色账户不可修改（包括 token 轮换）、不可删除
5. **Token 脱敏**：列表 API 中 token 显示为 `abcd********` 格式（[users.rs#L60-L71](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L60-L71)）
6. **RBAC 集成**：Enterprise 版本中，服务账户的权限通过 OpenFGA 精细控制，在 OFGA 中映射为 `allowed_user` 并附加额外 service_account 关系
