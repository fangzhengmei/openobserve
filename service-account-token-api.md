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

**不同创建路径下的默认值差异**：
- 通过 `POST /{org_id}/service_accounts` 路由（普通创建路径）：走 `post_user()` → `add_user_to_org()`，默认 `allow_static_token=true`，token 长度 16 字符
- 通过组织设置附带 `service_account` 字段创建（Tenant Admin 路径，[organization.rs#L402-L433](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/organization.rs#L402-L433)）：直接调用 `db::org_users::add_with_flags()`，**显式设为 `allow_static_token=false`**，token 长度 32 字符（不会被暴露）
- 通过 `ensure_sys_rca_agent()` 创建的 SRE Agent：`allow_static_token=true`

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

核心校验函数 `validate_credentials()`（[validator.rs#L227-L450](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L227-L450)）。

**完整的认证校验调用链路**（从请求到认证结果）：

```
HTTP 请求
  │
  ▼
oo_validator() [L1011-L1019]
  └─► oo_validator_internal() [L880-L949]
        │
        ├─ ① 检查 Session:: 前缀标记 [L886-L895]
        │     格式: Session::<session_id>::<actual_token>
        │     → 匹配: is_from_session=true, auth_str=<actual_token>
        │     → 不匹配: is_from_session=false, auth_str=原字符串
        │
        ├─ ② 分支: Basic Auth [L897-L917]
        │     ├─ base64 解码 → username:password
        │     ├─ 关键: 设置 modified_auth_info.bypass_check
        │     │        = is_from_session || auth_info.bypass_check
        │     │   (bypass_check=true 将跳过 allow_static_token 检查)
        │     └─► validator() [L138-L201]
        │           │
        │           ├─ ③ validate_credentials() [L227-L450]
        │           │     │
        │           │     ├─ ④ 服务账户 token 比对 [L326-L367]
        │           │     │     user.role.is_service_account()
        │           │     │     && user.token.eq(&user_password)
        │           │     │     │
        │           │     │     ├─ 检查 service_account_enabled 全局开关
        │           │     │     ├─ 检查 allow_static_token (仅当 bypass_check=false)
        │           │     │     │    条件: !from_session (= !bypass_check 之前传递)
        │           │     │     │         && db::org_users::get().allow_static_token == false
        │           │     │     │    → 命中: log warn + 返回 is_valid=false [L350-L364]
        │           │     │     └─ 通过: build_token_validation_response()
        │           │     │
        │           │     └─ (后续普通用户密码校验分支不影响 SA)
        │           │
        │           ├─ ⑤ 认证通过后: check_and_create_org() [L166]
        │           │
        │           └─ ⑥ check_permissions() 权限校验 [L186-L192]
        │                 ├─ auth_info.bypass_check=true → 直接通过
        │                 ├─ Root 角色 → 直接通过
        │                 ├─ 非 Enterprise → 始终 true
        │                 └─ Enterprise+OFGA: is_allowed() RBAC 检查
        │                       → 通过: AuthValidationResult
        │                       → 失败: AuthError::Forbidden("Unauthorized Access") [L200]
        │
        ├─ Bearer token 分支 [L918-L920]
        │     └─► token_validator() (Dex/JWT 校验)
        │
        └─ AuthExt 分支 [L921-L944]  (ingestion/proxy 扩展认证)
```

服务账户 token 校验的关键条件（[validator.rs#L326-L367](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L326-L367)）：

```rust
if user.role.is_service_account() && user.token.eq(&user_password) {
    // 1. 检查服务账户全局开关
    if !config.auth.service_account_enabled { return invalid; }

    // 2. 检查 allow_static_token — 条件是 !from_session
    //    from_session 参数值 = auth_info.bypass_check（在 validator() 第 4 步传入）
    //    bypass_check 在 oo_validator_internal 由 Session:: 前缀匹配结果决定
    //    因此: Session 来源的请求 → from_session=true → 跳过此检查
    //          直接 Basic Auth 的请求 → from_session=false → 执行此检查
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

**校验优先级**：服务账户 token 校验在密码校验之前执行，且**不受 native_login_enabled / root_only_login 限制**（这两个限制仅在密码校验分支 [validator.rs#L390-L414](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L390-L414) 生效）。

### 3.4 静态 Token 禁用的拒绝链路（allow_static_token=false）

当服务账户在组织中被设置为 `allow_static_token=false` 时，使用静态 token 直接调用 API 的完整拒绝路径：

```
客户端请求: Basic base64(sa@example.com:<static_token>)
  │
  ▼
oo_validator_internal()
  │  Session:: 前缀不匹配 → is_from_session=false
  ▼
Basic Auth 解码 → username=sa@example.com, password=<static_token>
  │  modified_auth_info.bypass_check = false (因为 is_from_session=false)
  ▼
validator()
  ▼
validate_credentials(user_id=sa@example.com, password=<static_token>, bypass_check=false)
  │
  ├─ user.role.is_service_account() → true ✓
  ├─ user.token.eq(password) → true ✓ (静态 token 匹配)
  ├─ service_account_enabled → true ✓
  │
  ├─ !from_session (= true) → 执行 allow_static_token 检查
  │   │
  │   ├─ db::org_users::get(org, email).await
  │   │    → 返回 OrgUserRecord { allow_static_token: false, ... }
  │   │
  │   └─ !org_user.allow_static_token → true
  │        │
  │        ├─ log::warn! 记录告警 [L350-L354]
  │        │   内容: "Service account '{}' in org '{}' attempted direct token
  │        │          auth but allow_static_token=false. Use assume_service_account
  │        │          API instead."
  │        │
  │        └─ 返回 TokenValidationResponse { is_valid: false, ... }
  │
  ▼
validator() 收到 is_valid=false
  → res.is_valid 为 false，跳过 check_permissions 等后续步骤
  ▼
oo_validator() 收到错误
  ▼
最终响应: 401 Unauthorized / 403 Forbidden (取决于 AuthExtractor 错误处理)
```

**关键洞察**：静态 token 禁用的检查发生在**身份认证阶段**（`validate_credentials`），而不是**权限校验阶段**（`check_permissions`）。这意味着：
- 失败原因是 "token 认证不合法"（因为静态 token 被禁止），不是 "权限不足"
- 错误 HTTP 状态码是 401/403 的 Unauthorized 语义，不是 RBAC 的 Forbidden 语义
- 绕过方式：使用 `assume_service_account` API 获取会话 token，此时 `from_session=true`，`allow_static_token` 检查被跳过

---

## 3.5 系统服务账户（SreAgent）的操作拒绝机制

系统服务账户（`UserRole::SreAgent`，email 匹配 `o2-sre-agent.org-*@openobserve.internal`）在所有修改/删除类操作中受到多层保护，拒绝检查发生在**不同的代码层级**：

### 拒绝层级总览

```
┌──────────────────────────────────────────────────────────────┐
│ 层级 1: HTTP Handler 层面的检查 (最先执行，性能最优)          │
│  L update() L215-L224: 角色+email 双重检查 MODIFY 拦截        │
│  L delete_bulk() L410-L428: HashSet 预过滤 DELETE BULK 拦截  │
├──────────────────────────────────────────────────────────────┤
│ 层级 2: Service 层面的检查 (通用删除逻辑的统一守护)            │
│  L remove_user_from_org() L966-L975: 通用 DELETE SINGLE 拦截 │
├──────────────────────────────────────────────────────────────┤
│ 层级 3: 认证校验层面的隐性保护                                 │
│  rotateToken 走 update() 分支 → 已被层级 1 拦截                │
└──────────────────────────────────────────────────────────────┘
```

### 1. 修改操作（UPDATE / rotateToken）的拒绝链路

**入口**：`PUT /{org_id}/service_accounts/{email_id}` → `update()`（[service_accounts/mod.rs#L202-L304](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/service_accounts/mod.rs#L202-L304)）

```
请求进入 update()
  │
  ▼ [层级 1: Handler L215-L224]
  优先: db::org_users::get(org_id, email_id) 查询 OrgUserRecord
    │
    ├─ 命中且 record.role == UserRole::SreAgent
    │     → 返回 403 Forbidden "System service accounts cannot be modified" ✓
    │
    └─ 查询失败/role 非 SreAgent → Fallback 检查
         │
         └─ is_system_service_account(email_id) → true (email 模式匹配)
              → 返回 403 Forbidden "System service accounts cannot be modified" ✓
  │
  ▼ (通过后才进入后续 rotateToken 或 UpdateUser 逻辑)
```

**设计意图**：采用「角色检查优先 + email 模式兜底」的双重检查策略。优先使用 `org_users.role` 字段判断（因为同 email 在不同组织可能角色不同，比如 A 组织是 SreAgent，B 组织是 ServiceAccount），仅当 DB 查询失败时才退化为 email 模式匹配。

### 2. 单个删除（DELETE SINGLE）的拒绝链路

**入口**：`DELETE /{org_id}/service_accounts/{email_id}` → `delete()` → `remove_user_from_org()`（[users.rs#L924-L1087](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L924-L1087)）

```
remove_user_from_org() 中
  │
  ▼ 前置检查（先执行 4 步通用检查，见 4.4 节）
  │
  ▼ [层级 2: L966-L975] 系统账户检查 — 在判断组织数量之前
  │
  │  user.organizations.iter().any(|o| o.role == SreAgent)
  │     │
  │     ├─ true → 意味着该账户在**任何一个组织**中扮演 SreAgent
  │     │        → 返回 403 "System service accounts cannot be deleted" ✓
  │     │        ▶ 注意：即使当前删除的 org_id 中角色不是 SreAgent，
  │     │          只要任意组织中是 SreAgent 就整体禁止删除
  │     │
  │     └─ false → 继续执行 fallback 检查
  │
  └─ is_system_service_account(&user.email) → true
       → 返回 403 "System service accounts cannot be deleted" ✓
```

**关键特性**：删除操作的拒绝检查是**全局跨组织**的 — 只要该 email 在任一组织中拥有 SreAgent 角色（或匹配系统账户 email 模式），则在所有组织中都**无法被删除**。这是与 UPDATE 操作检查（只检查当前 org_id 中的角色）的本质区别。

### 3. 批量删除（DELETE BULK）的拒绝链路

**入口**：`DELETE /{org_id}/service_accounts/bulk` → `delete_bulk()`（[service_accounts/mod.rs#L372-L455](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/service_accounts/mod.rs#L372-L455)）

```
delete_bulk() 中
  │
  ▼ [层级 1: L404-L428] Handler 层面批量预过滤
  │
  │  Step 1: 一次性 list_users_by_org() 获取该组织所有用户
  │  Step 2: 构建 sre_agent_emails HashSet
  │           (SreAgent 角色的 email 集合)
  │  Step 3: 逐个遍历请求中的 email
  │           │
  │           ├─ sre_agent_emails.contains(email)
  │           │   || is_system_service_account(email)
  │           │   → true:
  │           │      ▪ log::warn!("Attempted to delete system SA ...")
  │           │      ▪ unsuccessful.push(email)
  │           │      ▪ err = "System service accounts cannot be deleted"
  │           │      ▪ continue (不调用 remove_user_from_org)
  │           │
  │           └─ false:
  │                  调用 remove_user_from_org()
  │                  (层级 2 还会再次检查，形成双保险)
```

### 拒绝机制差异总结表

| 操作 | 检查层级 | 检查范围 | 响应方式 | HTTP 状态 |
|-----|---------|---------|---------|----------|
| UPDATE（含 rotateToken） | Handler 层（最先） | 仅当前 org_id 的角色，兜底 email 模式 | 立即返回错误响应 | 403 |
| DELETE SINGLE | Service 层（通用逻辑） | **全局跨组织**：任一组织中 SreAgent 即拒绝 | 立即返回错误响应 | 403 |
| DELETE BULK | Handler 层预过滤 + Service 层兜底 | 当前 org HashSet + email 模式 | 标记 unsuccessful，不中断整体流程 | 200（但该条失败） |
| CREATE | — | — | SreAgent 仅由系统自动创建，无法通过 API 手动创建 | — |

---

## 3.6 Session 标记机制

从 `assume_service_account` 获取的临时会话 token 在认证系统中以 `Session::` 前缀标记（[validator.rs#L886-L895](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L886-L895)）：

```
Session::<session_id>::<actual_token>
```

此标记在 `oo_validator_internal()` 解析时使得：
- `is_from_session = true`
- `modified_auth_info.bypass_check = is_from_session || auth_info.bypass_check = true`

从而在 `validate_credentials()` 中 `from_session=true`，跳过 `allow_static_token` 检查（[validator.rs#L346-L348](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L346-L348)）。即通过 assume 获取的会话 token 即使原始服务账户设置了 `allow_static_token=false` 也能正常使用。

同时，在 `validator()` 末尾的 `check_permissions()` 调用时（[validator.rs#L185-L192](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L185-L192)），`auth_info.bypass_check=true` 也会直接跳过 OpenFGA RBAC 检查。**注意：这意味着 assume 获取的会话 token 在权限校验时也 bypass 了 OpenFGA，其权限完全由创建会话时 enterprise 层验证时的角色决定。**

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

`DELETE /{org_id}/service_accounts/{email_id}`（[service_accounts/mod.rs#L332-L346](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/service_accounts/mod.rs#L332-L346)）调用 `users::remove_user_from_org()`。

**删除范围取决于该账户属于多少个组织**（[users.rs#L924-L1087](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L924-L1087)）：

#### 场景 A：账号仅属于 1 个组织（`orgs.len() == 1`）

**删除整个账号记录**（`db::user::delete(email_id)`），影响范围是**全局全部组织**（虽然此时只有一个组织）：

```
┌──────────────────────────────────────┐
│  1. 从 users 表中删除 DBUser 记录      │
│  2. 从 org_users 表中删除唯一的成员记录  │
│  3. Enterprise: 从 OpenFGA 删除        │
│     - delete_user_from_org()          │
│     - delete_service_account_from_org()│
│  4. 账号从系统中彻底消失，无法再登录     │
└──────────────────────────────────────┘
```

**特例禁止删除**：如果 `is_external=true`（外部用户，如 Dex/LDAP 同步过来的）且仅剩的这个组织中的角色是 `ServiceAccount`，返回 **403 "Not Allowed"**（[users.rs#L994-L995](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L994-L995)）。这是为了保护从外部身份源同步过来的服务账户不被误删。

#### 场景 B：账号属于 2 个或更多组织（`orgs.len() > 1`）

**仅从当前指定的 org_id 中移除**，账号本身保留，其他组织的成员关系和 token 均不受影响：

```
┌──────────────────────────────────────────────┐
│  1. 遍历 organizations，找到匹配 org_id 的条目  │
│  2. orgs.retain(|x| !x.name.eq(org_id))       │
│     （仅从向量中移除目标组织条目）               │
│  3. db::org_users::remove(org_id, email_id)   │
│     （从 org_users 表中删除当前组织的成员关系）   │
│  4. Enterprise: 仅从 OpenFGA 删除当前组织的     │
│     关系元组，其他组织的 OFGA 关系保持不变        │
│  5. 其他组织的 token 继续有效，账号可继续使用     │
└──────────────────────────────────────────────┘
```

**特例禁止删除**：在遍历组织过程中，如果发现 `is_external=true` 的 ServiceAccount，同样返回 403 "Not Allowed"（[users.rs#L1026-L1027](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L1026-L1027)）。

#### 前置拒绝检查（在范围判断之前执行）

在判断是场景 A 还是 B 之前，先执行以下拒绝检查（任一触发即返回）：

1. **调用者无权限**：非 Admin/Root 且无 OpenFGA RBAC → **401 Unauthorized "Not Allowed"**
2. **删除 Root 用户**：尝试删除 root@example.com → **403 Forbidden "Not Allowed"**
3. **删除自身**：`initiating_user.email == email_id` → **403 Forbidden "Not Allowed"**
4. **删除系统服务账户**：任一组织角色是 SreAgent，或 email 匹配系统模式 → **403 Forbidden "System service accounts cannot be deleted"**

#### 批量删除的特殊处理

`DELETE /{org_id}/service_accounts/bulk`（[service_accounts/mod.rs#L372-L455](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/service_accounts/mod.rs#L372-L455)）：

- **不会因单个失败而中断**：每个 email 独立处理
- **系统账户过滤**：构建 `sre_agent_emails` HashSet 后逐个检查，系统账户直接标记为 `unsuccessful`，不调用 `remove_user_from_org()`，错误信息 `err = "System service accounts cannot be deleted"`
- **Enterprise 预检查**：遍历所有 email 先做 `check_permissions()`，任一不通过即整体返回 403
- 返回结构 `BulkDeleteResponse { successful, unsuccessful, err }` 区分成功/失败

### 4.5 吊销路径总结

| 吊销方式 | 影响范围 | 即时性 |
|---------|---------|-------|
| Token 轮换 (rotateToken) | **仅当前组织内**的该服务账户 token（其他组织 token 不受影响） | 即时 |
| 删除服务账户（账号仅属于 1 个组织） | **全局删除整个账号**，连带删除唯一的组织关系 | 即时 |
| 删除服务账户（账号属于多个组织） | **仅当前组织**的成员关系和 token 被删除，账号和其他组织保留 | 即时 |
| 会话自然过期 | assume_service_account 的临时会话 | 到期时 |
| `allow_static_token=false` | **仅当前组织内**禁止直接使用静态 token | 即时（已存在的会话不受影响） |

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
2. **静态 token 风险**：普通路径创建的 SA 默认 `allow_static_token=true`，静态 token 无有效期，存在泄露风险；Tenant Admin 路径创建的 SA 默认 `allow_static_token=false`，强制走 assume 流程
3. **删除范围的危险性**：删除服务账户的影响范围**取决于该账户属于几个组织**：
   - 仅 1 个组织：**全局完全删除**（users 表记录被删除）
   - ≥2 个组织：**仅从当前组织移除**，其他组织继续有效
   - 操作前建议先查询该账户在哪些组织中存在，避免意外全局删除
4. **外部 SA 的删除保护**：`is_external=true` 的 ServiceAccount 无法被删除（返回 403），用于保护 Dex/LDAP 等身份源同步的服务账户
5. **assume_service_account 安全**：
   - 仅限 `_meta` 组织调用
   - 会话有最大 24 小时有效期
   - 会话存储在 DB 中，可被主动删除
   - 会话 token 同时 bypass `allow_static_token` 和 OpenFGA RBAC（双重 bypass），会话创建时的角色验证是唯一权限关卡
6. **系统账户保护的范围差异**：
   - UPDATE/rotateToken：只检查**当前组织**中的角色 → 精确控制
   - DELETE SINGLE：检查**任一组织**中是否为 SreAgent → 全局保护，防止从非 SreAgent 组织间接删除系统账户
   - DELETE BULK：Handler 层预过滤（HashSet，性能最优）+ Service 层双保险
7. **Token 脱敏**：列表 API 中 token 显示为 `abcd********` 格式（[users.rs#L60-L71](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L60-L71)）
8. **RBAC 集成**：Enterprise 版本中，服务账户的权限通过 OpenFGA 精细控制，在 OFGA 中映射为 `allowed_user` 并附加额外 service_account 关系
9. **静态 token 禁用的审计日志**：拒绝时会产生 `log::warn!` 日志，包含 org_id、email 和建议操作指引，但该信息写入应用日志而非审计日志（AuditMessage）。若需合规审计需关注应用日志中的此类警告。
10. **两种创建路径的 token 长度差异**：普通路径 16 字符，Tenant Admin 路径 32 字符，后者的熵更高但因 `allow_static_token=false` 不会暴露给用户。

---

## 11. 本次修正要点摘要

| 修正项 | 之前理解偏差 | 修正后的正确理解 |
|-------|------------|---------------|
| **删除范围** | 删除即全局删除所有组织中的账号 | 分两种场景：仅 1 个组织时全局删除 users 表记录；≥2 个组织时仅从当前 org_id 移除成员关系，账号保留，其他组织的 token 继续有效 |
| **单组织删除特例** | 未考虑 | `is_external=true` 且仅剩 1 个组织且角色为 ServiceAccount 时，返回 403 "Not Allowed"，禁止删除，防止删除外部身份源同步的 SA |
| **allow_static_token 拒绝链路** | 只说明功能，未追踪链路 | 详细追踪了从 `oo_validator_internal()` → `validator()` → `validate_credentials()` 的完整调用链，明确：<br>① `Session::` 前缀 → `bypass_check=true` → 跳过检查<br>② 检查发生在认证阶段（validate_credentials），不是 RBAC 阶段（check_permissions）<br>③ 失败时返回 TokenValidationResponse{is_valid:false}，产生 warn 级应用日志 |
| **系统账户 UPDATE 拒绝** | 笼统说不可修改 | 明确是 Handler 层 L215-L224 的「角色优先 + email 兜底」双重检查，只检查**当前 org_id** 内的角色（非全局），返回 403 |
| **系统账户 DELETE 拒绝** | 笼统说不可删除 | 区分两种删除路径：<br>• 单个删除 → Service 层 L966-L975，**全局跨组织**检查（任一组织有 SreAgent 角色即拒绝）<br>• 批量删除 → Handler 层 HashSet 预过滤，标记 unsuccessful，响应 200 但该条失败<br>两种删除最终都由 Service 层兜底形成双保险 |
| **创建路径默认值** | 未区分 | 明确三种创建路径的 `allow_static_token` 默认值和 token 长度：<br>• 普通 API 路径：allow_static_token=true，16 字符<br>• Tenant Admin 路径：allow_static_token=false，32 字符<br>• SRE Agent 系统创建：allow_static_token=true |
| **bypass_check 双重影响** | 只提 allow_static_token | Session 来源的 bypass_check=true 有双重跳过：<br>① 跳过 allow_static_token 检查（认证阶段）<br>② 跳过 check_permissions 的 OpenFGA RBAC（权限阶段）<br>权限完全依赖 assume 时的 enterprise 层验证 |
| **静态 token 禁用边界** | 完全未涉及 | 见第 12 章：16 类摄入端点通过标准 Basic Auth 路径会检查 allow_static_token，但 RUM 端点（通过 validate_token 而非 validate_credentials）完全不检查；AWS/GCP 云集成端点**会**检查；gRPC 认证路径默认 allow_static_token=true |
| **三类日志覆盖差异** | 完全未涉及 | 见第 13 章：审计中间件在认证中间件**之前**执行，摄入端点/SSE 流式端点完全不进入审计日志；应用 warn/error 日志覆盖安全拒绝事件但不落审计库；UsageData 仅覆盖摄入/搜索/函数事件 |

---

## 12. 静态 Token 禁用边界：各接口的 allow_static_token 检查覆盖矩阵

`allow_static_token=false` 的禁用效果并不是全局均匀的。它是否生效取决于**请求走哪一条认证路径**。核心判断标准：认证流程是否最终调用了 `validate_credentials()` 且传入的 `from_session` 参数值。

### 12.1 中间件执行顺序（关键前提）

主路由的中间件栈顺序（[router/mod.rs#L1017-L1042](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/router/mod.rs#L1017-L1042)）：

```
请求进入
    ▼
blocked_orgs_middleware
    ▼
audit_middleware       ← 注意：审计在认证**之前**执行！
    ▼
auth_middleware        ← oo_validator() → validate_credentials()
    ▼
... (decompression / server headers)
    ▼
handler 执行
```

而 other_service_routes（AWS/GCP/RUM）有各自独立的认证中间件，不在主 auth_middleware 链上。

### 12.2 各接口类别的检查覆盖

| 接口类别 | 认证中间件 | 核心认证函数 | from_session 参数 | allow_static_token 检查？ | 绕过条件 |
|---------|-----------|------------|-----------------|------------------------|---------|
| **1. 标准 REST API**<br>`/api/{org_id}/...` | `auth_middleware` → `oo_validator()` | `validate_credentials()` | `bypass_check` 值（由 Session:: 前缀决定） | ✅ **会检查** | ① 调用来自 assume_session (`Session::` 前缀)<br>② 非服务账户角色 |
| **2. HTTP 摄入端点**<br>`/api/{org_id}/_bulk` 等 INGESTION_EP（16 个） | 主 `auth_middleware` 链 | `validate_credentials()` | 同上（普通 Basic Auth → false） | ✅ **会检查** | assume_session |
| **3. AWS Kinesis Firehose**<br>`/aws/{org_id}/{stream}/_kinesis_firehose` | 独立 `aws_auth_middleware` → `validator_aws()` | `validate_credentials()` | **硬编码 `false`**（[validator.rs#L749](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L749)） | ✅ **会检查** | **无绕过可能**（from_session 恒为 false） |
| **4. GCP Pub/Sub**<br>`/gcp/{org_id}/{stream}/_sub` | 独立 `gcp_auth_middleware` → `validator_gcp()` | `validate_credentials()` | **硬编码 `false`**（[validator.rs#L797](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L797)） | ✅ **会检查** | **无绕过可能**（from_session 恒为 false） |
| **5. RUM 端点**<br>`/rum/v1/{org_id}/rum\|logs\|replay` | 独立 `rum_auth_middleware` → `validator_rum()` | **`validate_token()`**（**NOT** `validate_credentials`） | N/A（无此参数） | ❌ **完全不检查** | 漏洞：RUM 用 rum_token 查询 `USERS_RUM_TOKEN` 反向缓存，不经过 allow_static_token 逻辑 |
| **6. grpc 摄入端点**<br>`grpc/Writer` | `grpc::auth::mod.rs` | 手动构造 OrgUserRecord | 硬编码 `allow_static_token: true`（[grpc/auth/mod.rs#L180](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/grpc/auth/mod.rs#L180)） | N/A（手动构造） | 相当于永久放行，此 flag 永远为 true |
| **7. assume 会话请求**<br>（携带 session_id 或 Session:: 前缀） | `auth_middleware` | `validate_credentials()` | `true`（Session:: 解析后 bypass） | ❌ **跳过检查** | 设计如此，allow_static_token 不影响会话 token |
| **8. Dex/JWT Bearer Token** | `token_validator()`（Dex OAuth 校验） | 基于 JWT 的签名验证 | N/A | 不适用（JWT 有独立 exp 声明） | 非静态 token 路径 |
| **9. Proxy URL**<br>`/proxy/...` | `proxy_auth_middleware` → `validator_proxy_url()` | `validate_credentials()` | 同标准 REST（由 Session:: 决定） | ✅ **会检查** | assume_session |

### 12.3 INGESTION_EP 16 个端点清单

定义于 [ingestion.rs#L149-L166](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/common/meta/ingestion.rs#L149-L166)：

```
_bulk, _json, _multi, traces, write, _kinesis_firehose, _license,
_xpack, _index_template, _data_stream, _sub, logs, metrics, _json_arrow,
_hec, push
```

注意：虽然 `_kinesis_firehose` 和 `_sub` 在此列表中，但 AWS/GCP 集成实际挂载在独立 `/aws` 和 `/gcp` 前缀下，走独立中间件而非主路由。通过主路由访问这两个端点时仍走主中间件链路。

### 12.4 关键安全发现：RUM 端点不检查 allow_static_token

这是之前文档中**完全缺失**的关键边界：

`validator_rum()`（[validator.rs#L817-L878](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L817-L878)）的认证流程：

```
请求: /rum/v1/{org_id}/rum?o2-api-key=<token>
    │
    ▼
从 query 参数获取 token（oo-api-key 或 o2-api-key）
    │
    ▼
validate_token(token, org_id) [L218-L225]
    → 内部调用 users::get_user_by_token(org_id, token)
    → 仅检查 token 是否存在于 USERS_RUM_TOKEN 反向缓存
    → 未调用 validate_credentials()
    → 没有任何 service_account_enabled 检查
    → 没有任何 allow_static_token 检查 ❗
    │
    ▼
成功则返回 AuthValidationResult（含 email/role）
```

**结果**：即使某个服务账户设置了 `allow_static_token=false`，其 rum_token 仍可**直接用于 RUM 端点摄入**，不会被拒绝。这是设计特性（RUM token 设计上就是给浏览器/客户端 SDK 使用的静态 token），但 DevOps 需要意识到这一点：禁用静态 token 并不等于"全面禁用"，RUM 路径仍然开放。

### 12.5 禁用效果完整边界总结

如果 DevOps 将某服务账户的 `allow_static_token` 设为 `false`，实际封禁范围：

| 可以继续使用的路径 | 被封禁的路径 |
|-----------------|-----------|
| ✅ assume_service_account 获取的临时会话 token | ❌ 所有标准 REST API（直接 Basic Auth） |
| ✅ RUM 端点（rum_token 绕过检查） | ❌ HTTP 摄入端点（_bulk/_json/_multi/logs/metrics...） |
| ✅ Dex/JWT Bearer Token（如有 Dex 集成） | ❌ AWS Kinesis Firehose（from_session 硬编码 false） |
| ✅ gRPC 摄入接口（永久放行） | ❌ GCP Pub/Sub（from_session 硬编码 false） |
|  | ❌ Proxy URL 直连代理请求（直接 Basic Auth） |

---

## 13. 三类日志覆盖差异：审计日志 / 应用告警日志 / 使用量记录

### 13.1 三类日志的定位与架构位置

```
                        ┌─────────────────────────────────┐
                        │         HTTP 请求进入             │
                        └──────────────┬──────────────────┘
                                       │
              ┌────────────────────────┼───────────────────────┐
              ▼                        ▼                       ▼
 [审计中间件] audit_middleware   [认证中间件] auth_middleware   其他中间件
  (先于认证执行)                    validate_credentials()
       │                               │
       │ 成功/重定向才写入              ├─ → 安全拒绝 → log::warn!()
       │ 摄入/流式端点一律跳过          ├─ → 一般错误  → log::error!()
       ▼                               ▼
  AuditMessage                    UsageData (仅摄入/搜索/函数)
  (_audit_stream)                 (_usage_stream)
```

### 13.2 审计日志（AuditMessage → _audit_stream）

**触发点**：`audit_middleware()`（[router/mod.rs#L294-L385](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/router/mod.rs#L294-L385)），仅 Enterprise 版本 + `audit_enabled=true`。

#### 覆盖范围（会记录 ✅）

所有**满足以下全部条件**的 HTTP 请求：
1. `audit_enabled` 配置开启
2. 响应状态是 `is_success()`（2xx）或 `is_redirection()`（3xx）— **失败请求（4xx/5xx）不记录**
3. **不**命中以下三项排除规则：

#### 排除规则（不记录 ❌）

| 排除条件 | 匹配值 | 覆盖的场景 |
|---------|--------|----------|
| ① path_columns[1] 以 `_stream` 结尾 | e.g. `/api/org1/streams/default/_json_stream` | SSE 流式查询、AI 流式输出 |
| ② path 以 `ai/chat_stream` 结尾 | AI 流式对话接口 | AI Agent 流式响应 |
| ③ POST 方法 + 路径末尾段 ∈ INGESTION_EP（16 项） | 所有摄入批量/推送接口 | 日志/指标/追踪写入 |

#### 对服务账户的区分能力

审计日志 **本身没有 `is_service_account` 字段**。区分方式完全依赖 `user_email` 字段：
- 系统服务账户：匹配 email 模式 `o2-sre-agent.org-*@openobserve.internal`
- 用户创建的服务账户：通过 email 模式无法区分，需要在分析时关联 `org_users` 表的 `role` 字段
- 审计中间件的 `user_email` 来自**请求 header `user_id`**，而该 header 由 `auth_middleware` 写入。由于 **audit_middleware 在 auth_middleware 之前**顺序执行（L1023-L1025），这里存在一个微妙的时序细节：middleware 层是洋葱结构 — audit 先包一层，认证执行完返回后 audit 再记录，因此 user_id header 已经有值。顺序由 axum 执行模型保证正确性。

#### 明确不进入审计日志的服务账户相关事件

| 事件类型 | 是否进入审计日志 | 原因 |
|---------|---------------|-----|
| 服务账户创建/更新/删除 | ✅ 是（2xx 响应） | 非排除路径，成功才记录 |
| 服务账户创建失败（如邮箱重复） | ❌ 否 | 响应非 2xx/3xx |
| allow_static_token=false 导致的拒绝（401/403） | ❌ 否 | 响应非 2xx/3xx |
| SreAgent 修改保护触发（403） | ❌ 否 | 响应非 2xx/3xx |
| 服务账户调用摄入接口（_bulk/_json 等） | ❌ 否 | 命中排除条件 ③ |
| 服务账户调用 RUM 摄入接口 | ❌ 否 | 不在主路由中间件链上（无 audit_middleware） |
| 服务账户调用 AWS/GCP 摄入 | ❌ 否 | other_service_routes，不走主 audit_middleware |
| 服务账户调用流式查询（_stream 后缀） | ❌ 否 | 命中排除条件 ① |
| assume_service_account 调用成功 | ✅ 是（200） | 非排除路径，成功记录 |
| assume_service_account 失败（400/403） | ❌ 否 | 响应非 2xx/3xx |

### 13.3 应用告警日志（log::warn! / log::error! → stderr / 日志文件）

**触发点**：散布在认证/Handler/Service 各处，由 tracing/log crate 配置决定输出位置。

#### 与服务账户相关的已知告警场景汇总

| 场景 | 日志级别 | 代码位置 | 内容关键字 |
|-----|---------|---------|----------|
| allow_static_token=false，直接用静态 token | **warn** | [validator.rs#L350-L354](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L350-L354) | "attempted direct token auth but allow_static_token=false. Use assume_service_account API instead." |
| 批量删除系统 SA | **warn** | [service_accounts/mod.rs#L422-L424](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/service_accounts/mod.rs#L422-L424) | "Attempted to delete system service account {org_id}/{email} via bulk delete" |
| assume_service_account 非 _meta 组织 | **warn** | [assume_service_account.rs#L113-L116](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/organization/assume_service_account.rs#L113-L116) | "Assume service account rejected: API must be called on _meta org, got '{org_id}'" |
| assume_service_account 失败（enterprise 层） | **error** | [assume_service_account.rs#L186](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/organization/assume_service_account.rs#L186) | "Assume service account failed: {e}" |
| 批量删除 SA 失败 | **error** | [service_accounts/mod.rs#L436-L437, L444](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/request/service_accounts/mod.rs#L436-L437) | "error in deleting service account {org_id}/{email} : ..." |
| 删除 SA 时 DB 操作失败 | **error** | [users.rs#L982-L984, L998-L999, L1067-L1069](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/users.rs#L982-L984) | "error deleting invites when deleting user..." / "error deleting user from db..." |
| RUM 端点认证 token 未找到 | **error** | [validator.rs#L863-L865](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L863-L865) | "validate_token: Token not found for org_id: {}" |
| RUM 端点缺少 api-key 参数 | **error** | [validator.rs#L871-L873](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/handler/http/auth/validator.rs#L871-L873) | "validate_token: Missing api key for rum endpoint org_id: {}" |

#### 重要特性：安全拒绝事件的日志覆盖

应用告警日志最大的价值是覆盖了**审计日志不记录的安全拒绝事件**：

| 安全拒绝事件 | 审计日志 | 应用告警日志 |
|------------|---------|----------|
| allow_static_token=false 的 token 使用尝试 | ❌（401/403 响应） | ✅（warn，含 org、email、建议指引） |
| SreAgent 被尝试修改 | ❌（403 响应） | ⚠️ 仅有 HTTP 403 响应，无明确 warn/error 日志 |
| SreAgent 被尝试批量删除 | ❌（200 但 unsuccessful） | ✅（warn，明确记录 attempt） |
| 删除自身尝试 | ❌（403 响应） | ⚠️ 仅 HTTP 响应，无明确 warn |
| assume 非 _meta 组织 | ❌（400/403 响应） | ✅（warn/error，双重覆盖） |
| 非 Admin 尝试 rotate token | ❌（403 响应） | ⚠️ 仅有 HTTP 响应，无明确 warn |

### 13.4 使用量记录（UsageData → _usage_stream）

**触发点**：`report_request_usage_stats()`（[self_reporting/mod.rs#L90-L222](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/service/self_reporting/mod.rs#L90-L222)），由各个摄入 handler、search handler、function 执行器主动调用。

#### UsageData 中的服务账户识别字段

```rust
UsageData {
    user_email: String,       // 关键：记录发起者身份
    org_id: String,           // 操作所在组织
    event: UsageEvent,        // Ingestion / Search / Functions / ...
    num_records: i64,         // 记录条数
    size: f64,                // 大小/计数
    response_time: i64,       // 响应时长
    stream_name: String,      // 目标流
    stream_type: StreamType,  // Logs/Metrics/Traces...
    // ... 其他资源计量字段
}
```

`user_email` 来源：由 `IngestUser::from_user_email(user_email_str)` 构造（[ingestion.rs#L79-L87](file:///d:/fz/0508-3/solo-dogfeeding/code/201-openobserve/src/common/meta/ingestion.rs#L79-L87)），如果 email 为空会变成 `unknown@system.local`。

#### 使用量记录覆盖的事件类型

仅覆盖**有资源消耗可计量**的请求：

| UsageEvent 类型 | 触发场景 | 服务账户是否可能触发 |
|----------------|---------|------------------|
| **Ingestion** | 所有数据写入：logs/metrics/traces/rum/enrichment 等 | ✅ 最常触发 |
| **Search** | SQL/PromQL 查询、dashboard 查询、alert 评估、anomaly detection 等 | ✅ 程序化查询 |
| **Functions** | VRL/自定义数据转换函数（按调用次数计量） | ✅ 数据处理管道 |
| **NewIncident / IncidentReAnalysis** | 告警事件相关（按数量计数） | ⚠️ 通常由系统触发而非 SA |
| **其他**（AI credits 等 cloud 计费项） | Cloud 版本额外功能 | ✅ 取决于调用方 |

#### 明确不进入 UsageData 的事件

- **认证/授权事件**：token 验证通过/失败不会产生 UsageData
- **用户管理操作**：创建、更新、删除服务账户不会产生 UsageData
- **组织管理操作**：assume_service_account、创建组织等不会产生 UsageData
- **后台系统任务**：如 SelfReporting、ServiceGraph 等使用 `SystemJob` 身份（格式为 `{job}@system.local`）单独计量

#### 与审计日志的互补关系

| 维度 | 审计日志（AuditMessage） | 使用量记录（UsageData） |
|-----|----------------------|-------------------|
| **主要目的** | 合规审计：谁在什么时候做了什么操作 | 计费计量：用了多少资源 |
| **成功才记录** | ✅ 仅 2xx/3xx | ✅ 通常在请求完成后记录（无论成功与否取决于 handler） |
| **服务账户识别** | 仅 user_email 字段，无 role 标记 | 仅 user_email 字段，无 role 标记 |
| **摄入端点覆盖** | ❌ 完全排除（INGESTION_EP 跳过） | ✅ **最详细**（主要覆盖范围） |
| **用户管理 API 覆盖** | ✅ 成功时记录 | ❌ 不记录 |
| **组织管理 API 覆盖** | ✅ 成功时记录 | ❌ 不记录 |
| **请求体/参数** | ✅ 完整保留 http_body + query_params | ❌ 仅 request_body 字符串（通常是事件名） |
| **响应时长** | ❌ 无 | ✅ response_time 字段 |
| **数据量/记录数** | ❌ 无 | ✅ num_records / size / scan_files 等详细字段 |

### 13.5 DevOps 评估建议：三类日志组合使用方案

对于服务账户的完整安全监测，不能依赖单一日志类型：

1. **安全合规审计**：需要三类日志关联查询 —
   - 谁调用了什么管理操作 → 审计日志（_audit_stream）
   - 谁写入了多少数据 → UsageData（_usage_stream）
   - 谁尝试了违规操作（安全拒绝事件） → 应用告警日志（warn/error）

2. **快速发现违规静态 token 使用**：以应用告警日志为主，关键字 `allow_static_token=false`

3. **服务账户成本分摊与用量统计**：以 UsageData 为主，按 `user_email` 过滤服务账户 email 模式

4. **合规盲区注意**：审计日志对 4xx/5xx 的安全拒绝事件完全不记录；如果合规要求记录"所有鉴权失败尝试"，必须额外收集应用 warn 日志。

5. **RUM 端点的盲区**：既不进入审计日志，也不受 allow_static_token 控制 — RUM token 的安全性完全依赖 token 本身的保密性。
