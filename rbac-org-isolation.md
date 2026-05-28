# OpenObserve RBAC 与 Organization 隔离代码走向分析

## 一、角色独立存储实现

### 1.1 角色定义层

**核心文件：`src/config/src/meta/user.rs`**

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
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

**关键特性：**
- 角色以枚举形式定义，每个角色对应一个整数值（用于数据库存储）
- 实现了 `From<i16>` 和 `Into<i16>` trait，用于数据库与内存表示的转换
- 实现了 `PartialOrd` trait，定义了角色权限层级：`Root > Admin > Editor > Viewer > User > ServiceAccount`
- 实现了 `FromStr` trait，支持从字符串解析角色
- 提供 `is_service_account()` 方法区分普通用户与服务账号

### 1.2 用户-组织-角色关联模型

**核心文件：`src/config/src/meta/user.rs`**

```rust
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct DBUser {
    pub email: String,
    pub first_name: String,
    pub last_name: String,
    pub password: String,
    pub salt: String,
    pub organizations: Vec<UserOrg>,  // 多组织角色关联
    pub is_external: bool,
    pub password_ext: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UserOrg {
    pub name: String,           // org_id
    pub org_name: String,       // org 显示名称
    pub token: String,          // 该组织下的访问 token
    pub rum_token: Option<String>,
    pub role: UserRole,         // 该组织下的角色
}
```

**设计要点：**
- 一个用户可以属于多个组织，在每个组织中有独立的角色
- `DBUser::get_user(org_id)` 方法根据组织 ID 过滤出用户在该组织的角色信息
- `DBUser::get_all_users()` 方法将一个 DBUser 展开为多个 User 对象（每个组织一个）

### 1.3 API 层角色请求模型

**核心文件：`src/common/meta/user.rs`**

```rust
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UserOrgRole {
    #[serde(rename = "role")]
    pub base_role: UserRole,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub custom_role: Option<Vec<String>>,  // 企业版自定义角色
}

#[derive(Clone, Debug, Eq, PartialEq, Default, Serialize, Deserialize, ToSchema)]
pub struct UserRoleRequest {
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "custom_role")]
    pub custom: Option<Vec<String>>,
}
```

### 1.4 数据库持久层 - org_users 表（关键修正！）

**数据库实体定义：`src/infra/src/table/entity/org_users.rs`**

```rust
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "org_users")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,              // 主键：KSUID (27字符)
    pub email: String,           // 用户邮箱
    pub org_id: String,          // 组织 ID
    pub role: i16,               // 角色编码
    pub token: String,           // 访问 token
    pub rum_token: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub allow_static_token: bool,
}
```

**数据库迁移文件：`src/infra/src/table/migration/m20241227_000300_create_org_users_table.rs`**

**⚠️ 重要修正：主键与唯一性约束的真实实现**

```sql
-- 1. 主键：单独的 id 字段（KSUID，27字符），不是复合主键
CREATE TABLE IF NOT EXISTS "org_users" (
    "id" char(27) NOT NULL PRIMARY KEY,    -- 主键：KSUID
    "email" varchar(100) NOT NULL,
    "org_id" varchar(256) NOT NULL,
    "role" smallint NOT NULL,
    "token" varchar(256) NOT NULL,
    "rum_token" varchar(256),
    "created_at" bigint NOT NULL,
    "updated_at" bigint NOT NULL,
    -- 外键约束
    CONSTRAINT "org_users_org_id_fk" FOREIGN KEY ("org_id") REFERENCES "organizations" ("identifier"),
    CONSTRAINT "org_users_user_email_fk" FOREIGN KEY ("email") REFERENCES "users" ("email")
);

-- 2. 唯一性约束：通过唯一索引实现，注意顺序是 (email, org_id)！
CREATE UNIQUE INDEX IF NOT EXISTS "org_users_id_email_idx" ON "org_users" ("email", "org_id");

-- 3. rum_token 普通索引（非唯一）
CREATE INDEX IF NOT EXISTS "org_users_rum_token_idx" ON "org_users" ("rum_token");
```

**主键 ID 生成逻辑：`src/config/src/ider.rs:65`**

```rust
pub fn uuid() -> String {
    Ksuid::new(None, None).to_string()  // 生成 27 字符的 KSUID
}
```

**数据库记录结构：`src/infra/src/table/org_users.rs`**

```rust
#[derive(Debug, Clone)]
pub struct OrgUserRecord {
    pub email: String,
    pub org_id: String,
    pub role: UserRole,
    pub token: String,
    pub rum_token: Option<String>,
    pub created_at: i64,
    pub allow_static_token: bool,
}
```

### 1.5 数据库操作 - CRUD 实现

**核心文件：`src/infra/src/table/org_users.rs`**

#### 1.5.1 添加用户到组织（add_with_flags）

```rust
pub async fn add_with_flags(
    org_id: &str,
    user_email: &str,
    role: UserRole,
    token: &str,
    rum_token: Option<String>,
    allow_static_token: bool,
) -> Result<(), errors::Error> {
    let now = chrono::Utc::now().timestamp_micros();
    let role: i16 = role.into();
    let record = ActiveModel {
        org_id: Set(org_id.to_string()),
        email: Set(user_email.to_string()),
        role: Set(role),
        token: Set(token.to_string()),
        rum_token: Set(rum_token),
        created_at: Set(now),
        updated_at: Set(now),
        id: Set(ider::uuid()),  // 生成 KSUID 作为主键
        allow_static_token: Set(allow_static_token),
    };

    let _lock = get_lock().await;  // SQLite 写入锁
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;

    match Entity::insert(record).exec(client).await {
        Ok(_) => Ok(()),
        Err(e) => match e.sql_err() {
            // ⚠️ 幂等性处理：唯一约束冲突时静默成功
            Some(SqlErr::UniqueConstraintViolation(_)) => Ok(()),
            _ => Err(Error::DbError(DbError::SeaORMError(e.to_string()))),
        },
    }
}
```

#### 1.5.2 更新用户在组织中的角色（update）

```rust
pub async fn update(
    org_id: &str,
    email: &str,
    role: UserRole,
    token: &str,
    rum_token: Option<String>,
) -> Result<(), errors::Error> {
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;
    // 注释明确说明：There can be only one record with one org_id and email.
    Entity::update_many()
        .col_expr(Column::Role, Expr::value(role as i16))
        .col_expr(Column::Token, Expr::value(token.to_string()))
        .col_expr(Column::RumToken, Expr::value(rum_token))
        .col_expr(Column::UpdatedAt, Expr::value(chrono::Utc::now().timestamp_micros()))
        .filter(Column::OrgId.eq(org_id))
        // email 大小写不敏感匹配
        .filter(Expr::expr(Func::lower(Expr::col(Column::Email))).eq(email.to_lowercase()))
        .exec(client)
        .await
        .map_err(|e| Error::DbError(DbError::SeaORMError(e.to_string())))?;
    Ok(())
}
```

#### 1.5.3 按组织和用户查询（get）

```rust
pub async fn get(org_id: &str, email: &str) -> Result<OrgUserRecord, errors::Error> {
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;
    let record = Entity::find()
        .filter(Column::OrgId.eq(org_id))
        .filter(Expr::expr(Func::lower(Expr::col(Column::Email))).eq(email.to_lowercase()))
        .one(client)
        .await
        .map_err(|e| Error::DbError(DbError::SeaORMError(e.to_string())))?
        .ok_or_else(|| Error::DbError(DbError::SeaORMError("User not found".to_string())))?;
    Ok(OrgUserRecord::from(record))
}
```

#### 1.5.4 列出用户所属的所有组织（list_orgs_by_user）

```rust
pub async fn list_orgs_by_user(email: &str) -> Result<Vec<UserOrgExpandedRecord>, errors::Error> {
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;
    let records = Entity::find()
        .filter(Expr::expr(Func::lower(Expr::col((Entity, Column::Email)))).eq(email.to_lowercase()))
        .order_by(Column::CreatedAt, Order::Desc)
        .inner_join(super::entity::organizations::Entity)  // JOIN 组织表获取 org_name
        .select_only()
        .column(Column::Email)
        .column(Column::OrgId)
        .column(Column::Role)
        .column(Column::Token)
        .column(Column::RumToken)
        .column(Column::CreatedAt)
        .column(organizations::Column::OrgName)
        .column(organizations::Column::OrgType)
        .column(Column::AllowStaticToken)
        .into_model::<UserOrgExpandedRecord>()
        .all(client)
        .await
        .map_err(|e| Error::DbError(DbError::SeaORMError(e.to_string())))?;
    Ok(records)
}
```

#### 1.5.5 按 org_id 删除用户（remove）

```rust
pub async fn remove(org_id: &str, email: &str) -> Result<(), errors::Error> {
    let _lock = get_lock().await;
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;
    Entity::delete_many()
        .filter(Column::OrgId.eq(org_id))
        .filter(Expr::expr(Func::lower(Expr::col(Column::Email))).eq(email.to_lowercase()))
        .exec(client)
        .await
        .map_err(|e| Error::DbError(DbError::SeaORMError(e.to_string())))?;
    Ok(())
}
```

### 1.6 缓存层设计

**核心文件：`src/service/db/org_users.rs`**

```rust
// 缓存键格式："{org_id}/{user_email_lowercase}"
pub fn get_cached_user_org(org_id: &str, user_email: &str) -> Option<User> {
    let cache_key = user_email.to_lowercase();
    match ORG_USERS.get(&format!("{org_id}/{cache_key}")) {
        Some(org_user) => match USERS.get(&cache_key) {
            Some(user) => Some(User {
                email: user.email.clone(),
                password: user.password.clone(),
                role: org_user.role.clone(),
                salt: user.salt.clone(),
                first_name: user.first_name.clone(),
                last_name: user.last_name.clone(),
                password_ext: user.password_ext.clone(),
                token: org_user.token.clone(),
                rum_token: org_user.rum_token.clone(),
                org: org_user.org_id.clone(),
                is_external: user.user_type.is_external(),
            }),
            None => None,
        },
        None => None,
    }
}
```

**缓存更新机制（watch 函数）：**
- 监听 `/org_users/` 前缀的数据库变更事件
- `single/{org_id}/{user_email}`：单个用户-组织关系变更
- `many/user/{email}`：用户的所有组织关系变更
- 自动更新 `ORG_USERS`、`USERS_RUM_TOKEN`、`ROOT_USER` 缓存

---

## 二、API 层权限上下文切换实现

### 2.1 完整调用链路（按代码执行顺序）

```
HTTP 请求到达
    ↓
1. auth_middleware (router/mod.rs)
   ├─ 提取 RequestData（同步，确保 Future Send）
   └─ AuthExtractor::from_request_parts
       ├─ 从 Authorization 头提取凭证
       ├─ 从 URL 路径提取 org_id（如 /api/{org_id}/streams）
       ├─ 解析资源类型（streams/dashboards/alerts 等）
       └─ 解析 HTTP 方法（GET/POST/PUT/DELETE）
    ↓
2. oo_validator (auth/validator.rs:1011)
   └─ oo_validator_internal (auth/validator.rs:120)
       ├─ 提取 user_id 和 password
       ├─ 调用 validate_credentials (auth/validator.rs:227)
       │   ├─ 从 URL 解析 org_id
       │   ├─ is_root_user(user_id)？Root 用户用 DEFAULT_ORG 查询
       │   ├─ users::get_user(Some(org_id), user_id)
       │   │   ├─ get_cached_user_org(org_id, user_id)（先查缓存）
       │   │   └─ db::user::get(Some(org_id), user_id)（缓存未命中查 DB）
       │   ├─ 用户不存在？尝试 license 特殊处理
       │   ├─ ServiceAccount token 验证（检查 allow_static_token）
       │   ├─ Token 验证（token == password）
       │   ├─ 【企业版分岔点1】native_login_enabled / root_only_login 检查
       │   └─ 密码验证（get_hash + 比对）
       │
       ├─ 凭证验证通过？→ 调用 check_and_create_org (auth/validator.rs:579)
       │   ├─ 解析 URL，跳过 node/profile 前缀
       │   ├─ org 已存在？直接通过
       │   ├─ org 不存在？
       │   │   ├─ create_org_through_ingestion 未开启？→ 返回 404
       │   │   ├─ 是 Root 用户 + POST + 摄入端点？
       │   │   │   └─ service::organization::check_and_create_org(org_id)
       │   │   │       ├─ organizations::add() 写入数据库
       │   │   │       ├─ put_into_db_coordinator 同步
       │   │   │       └─ 【企业版分岔点2】save_org_tuples(org_id) → OpenFGA
       │   │   └─ 其他情况 → 返回 404
       │   └─ org 存在或创建成功 → 继续
       │
       ├─ 【企业版分岔点3】Viewer 角色自我更新特殊处理
       │
       └─ 调用 check_permissions (auth/validator.rs:1035/1092)
           ├─ auth_info.bypass_check？直接通过
           ├─ 【企业版】
           │   ├─ OpenFGA 未启用？直接通过
           │   ├─ block_feature_for_report_failure？直接通过
           │   ├─ role == Root？直接通过
           │   ├─ 创建组织场景？用 META_ORG 检查
           │   └─ o2_openfga::authorizer::authz::is_allowed(
           │          org_id, user_id, method, obj_str, parent_id, role)
           └─ 【社区版】
               └─ 直接返回 true（所有认证用户都有权限）
    ↓
3. 权限验证通过 → user_id 插入请求头
    ↓
4. Handler 执行（业务逻辑）
```

### 2.2 认证分流：oo_validator_internal 的四大分支路径

**核心文件：`src/handler/http/auth/validator.rs:880`**

这是整个认证系统的核心分流点，根据 `auth_info.auth` 的前缀进行四层分支判断：

```rust
async fn oo_validator_internal(
    req_data: &RequestData,
    auth_info: &AuthExtractor,
    path_prefix: &str,
) -> Result<AuthValidationResult, AuthError> {
    // 第一步：Session:: 前缀检测
    let (is_from_session, auth_str) = if let Some(rest) = auth_info.auth.strip_prefix("Session::") {
        // 格式: "Session::<session_id>::<actual_token>"
        if let Some((_session_id, token)) = rest.split_once("::") {
            (true, token.to_string())
        } else {
            (false, auth_info.auth.clone())
        }
    } else {
        (false, auth_info.auth.clone())
    };

    // 第二步：四大认证分支分流
    if let Some(info) = auth_str.strip_prefix("Basic ").map(str::trim) {
        // ════════════════════════════════════════════════
        // 分支 1: Basic 认证 (用户名:密码 Base64 编码)
        // ════════════════════════════════════════════════
        let decoded = match base64::decode(info) {
            Ok(val) => val,
            Err(_) => return Err(AuthError::Unauthorized("Unauthorized Access".to_string())),
        };
        let (username, password) = match get_user_details(&decoded) {
            Some(value) => value,
            None => return Err(AuthError::Unauthorized("Unauthorized Access".to_string())),
        };
        // Session 认证会设置 bypass_check = true，绕过后续权限检查
        let mut modified_auth_info = auth_info.clone();
        modified_auth_info.bypass_check = is_from_session || auth_info.bypass_check;
        validator(
            req_data,
            &username,
            &password,
            &modified_auth_info,
            path_prefix,
        )
        .await
    } else if auth_str.starts_with("Bearer") {
        // ════════════════════════════════════════════════
        // 分支 2: Bearer Token 认证 (JWT/OAuth)
        // ════════════════════════════════════════════════
        log::debug!("Bearer token found");
        super::token::token_validator(req_data, auth_info).await
    } else if let Ok(auth_tokens) = config::utils::json::from_str::<AuthTokensExt>(&auth_info.auth) {
        // ════════════════════════════════════════════════
        // 分支 3: Auth Ext Token 认证 (前端扩展 token)
        // ════════════════════════════════════════════════
        log::debug!("Auth ext token found");
        if auth_tokens.has_expired() {
            // 🔴 Token 过期直接返回未授权
            Err(AuthError::Unauthorized("Unauthorized Access".to_string()))
        } else {
            log::debug!("Auth ext token found: decoding");
            let decoded = match base64::decode(
                auth_tokens
                    .auth_ext
                    .strip_prefix("auth_ext")
                    .unwrap()
                    .trim(),
            ) {
                Ok(val) => val,
                Err(_) => return Err(AuthError::Unauthorized("Unauthorized Access".to_string())),
            };
            let (username, password) = match get_user_details(&decoded) {
                Some(value) => value,
                None => return Err(AuthError::Unauthorized("Unauthorized Access".to_string())),
            };
            log::info!("Auth ext token found: validating: {username}");
            validator(req_data, &username, &password, auth_info, path_prefix).await
        }
    } else {
        // ════════════════════════════════════════════════
        // 分支 4: 无法识别的认证方式
        // ════════════════════════════════════════════════
        Err(AuthError::Unauthorized("Unauthorized Access".to_string()))
    }
}
```

#### 2.2.1 四大认证分支详解

| 分支 | 触发条件 | 处理逻辑 | 适用场景 |
|------|---------|---------|---------|
| **Basic 认证** | `auth_str.starts_with("Basic ")` | Base64 解码 → 提取 username:password → 调用 `validator()` | API 调用、脚本集成 |
| **Bearer 认证** | `auth_str.starts_with("Bearer")` | 调用 `token_validator()` (企业版 JWT 验证，社区版直接返回 Not Supported) | SSO/OAuth 登录、Dex 集成 |
| **Auth Ext 认证** | JSON 可解析为 `AuthTokensExt` | 先检查 `has_expired()` → 再解码 auth_ext → 提取 username:password → 调用 `validator()` | 前端 Web 界面会话 |
| **Session 前缀** | `auth_str.starts_with("Session::")` | 解析 `Session::<session_id>::<token>` → 设置 `bypass_check = true` → 走 Basic 分支 | 登录态会话，绕过权限检查 |

#### 2.2.2 Session Token 绕过权限检查的触发条件

**触发条件（同时满足）：**
1. `auth_info.auth` 以 `Session::` 为前缀
2. 格式为 `Session::<session_id>::<actual_token>`，其中 `<actual_token>` 以 `Basic ` 开头
3. 解析成功后设置 `modified_auth_info.bypass_check = is_from_session || auth_info.bypass_check`

**效果：**
- 在 `validator()` 函数中，`if auth_info.bypass_check || check_permissions(...)` 判断短路
- `bypass_check = true` 时直接跳过 `check_permissions()` 调用
- 等同于 Session 认证的请求绕过了 OpenFGA 细粒度权限检查

#### 2.2.3 Auth Ext Token 过期判定

**核心文件：`src/common/meta/user.rs:473`**

```rust
pub struct AuthTokensExt {
    pub auth_ext: String,        // Base64 编码的 Basic 认证信息
    pub refresh_token: String,   // 刷新 token
    pub request_time: i64,       // token 获取时间（Unix 时间戳，秒）
    pub expires_in: i64,         // 有效期（秒）
}

impl AuthTokensExt {
    /// 检查 token 是否已过期
    pub fn has_expired(&self) -> bool {
        // 当前时间 - 请求时间 > 有效期？
        chrono::Utc::now().timestamp() - self.request_time > self.expires_in
    }
}
```

**过期判定逻辑：**
- `has_expired()` 在认证分流的**最开始**就被调用
- 一旦过期，直接返回 `AuthError::Unauthorized`，不进行后续验证
- 这是第一道防线，防止过期 token 消耗验证资源

#### 2.2.4 Bearer Token 验证：企业版 vs 社区版分岔

**核心文件：`src/handler/http/auth/token.rs:27/234`**

**企业版实现（JWT 验证）：**
```rust
#[cfg(feature = "enterprise")]
pub async fn token_validator(
    req_data: &RequestData,
    auth_info: &AuthExtractor,
) -> Result<AuthValidationResult, AuthError> {
    let user;
    let keys = get_dex_jwks().await;  // 获取 Dex JWKS 公钥
    // ... 解析路径 ...
    
    // 1. JWT 验证和解码
    match jwt::verify_decode_token(
        auth_info.auth.strip_prefix("Bearer").unwrap().trim(),
        &keys,
        &get_dex_config().client_id,
        false,
        login_flow,
    ) {
        Ok(res) => {
            let user_id = &res.0.user_email;
            if res.0.is_valid {
                // 2. 根据路径类型获取用户信息
                // - organizations/clusters 端点：从 _meta org 查找
                // - member_subscription/invites：特殊处理
                // - 普通端点：从 URL 中的 org_id 查找
                // ...
                match user {
                    Some(user) => {
                        // 3. 权限检查
                        if auth_info.bypass_check
                            || check_permissions(
                                &user_email,
                                auth_info.clone(),
                                user_role.clone(),
                                is_external,
                            )
                            .await
                        {
                            Ok(AuthValidationResult { /* ... */ })
                        } else {
                            Err(AuthError::Forbidden("Forbidden".to_string()))
                        }
                    }
                    // 特殊场景允许无 DB 用户
                    None if (is_list_invite_call || is_member_subscription || ...) => {
                        Ok(AuthValidationResult { user_email: res.0.user_email.clone(), ... })
                    }
                }
            }
        }
    }
}
```

**社区版实现（直接返回不支持）：**
```rust
#[cfg(not(feature = "enterprise"))]
pub async fn token_validator(
    _req_data: &RequestData,
    _token: &AuthExtractor,
) -> Result<AuthValidationResult, AuthError> {
    // 🟥 社区版直接返回 "Not Supported"，不支持 Bearer/JWT 认证
    Err(AuthError::Unauthorized("Not Supported".to_string()))
}
```

**⚠️ 关键注意：**
- 社区版的 Bearer 认证直接返回 `AuthError::Unauthorized("Not Supported")`
- 这意味着社区版只能使用 Basic 认证（用户名:密码）或 Auth Ext Token
- Bearer/JWT 是企业版专属功能，用于 Dex SSO 集成

### 2.3 认证中间件入口

**核心文件：`src/handler/http/router/mod.rs`**

```rust
pub async fn auth_middleware(request: Request, next: Next) -> Response {
    // 1. 提取请求数据（同步，确保 Future 是 Send）
    let req_data = RequestData {
        uri: request.uri().clone(),
        method: request.method().clone(),
        headers: request.headers().clone(),
    };

    // 2. 从请求中提取认证信息
    let (mut parts, body) = request.into_parts();
    let auth_info = match AuthExtractor::from_request_parts(&mut parts, &()).await {
        Ok(info) => info,
        Err(e) => return e.into_response(),
    };

    // 3. 验证认证信息
    match oo_validator(&req_data, &auth_info).await {
        Ok(result) => {
            // 将 user_id 插入请求头，供下游处理器使用
            parts.headers.insert(
                header::HeaderName::from_static("user_id"),
                header::HeaderValue::from_str(&result.user_email)
                    .unwrap_or_else(|_| header::HeaderValue::from_static("")),
            );
            next.run(Request::from_parts(parts, body)).await
        }
        Err(e) => e.into_response(),
    }
}
```

### 2.3 认证信息提取器

**核心文件：`src/common/utils/auth.rs`**

```rust
#[derive(Clone, Debug)]
pub struct AuthExtractor {
    pub auth: String,           // Authorization 头内容
    pub method: String,         // HTTP 方法: GET/POST/PUT/DELETE
    pub o2_type: String,        // 资源类型: streams:default/logs
    pub org_id: String,         // 组织 ID（从 URL 提取）
    pub bypass_check: bool,     // 是否绕过权限检查
    pub parent_id: String,      // 父资源 ID
}
```

### 2.4 认证验证器

**核心文件：`src/handler/http/auth/validator.rs`**

```rust
pub async fn oo_validator_internal(
    req_data: &RequestData,
    auth_info: &AuthExtractor,
    path_prefix: &str,
) -> Result<AuthValidationResult, AuthError> {
    // ... 提取 user_id 和 password ...

    match validate_credentials(user_id, password.trim(), path, auth_info.bypass_check).await {
        Ok(res) => {
            if res.is_valid {
                // 检查并创建组织（如果需要）
                check_and_create_org(user_id, &req_data.method, path).await?;

                #[cfg(feature = "enterprise")]
                { /* Viewer 角色自我更新特殊处理 */ }

                if auth_info.bypass_check
                    || check_permissions(
                        &res.user_email,
                        auth_info.clone(),
                        res.user_role.clone().unwrap_or(get_default_user_role()),
                        !res.is_internal_user,
                    )
                    .await
                {
                    Ok(AuthValidationResult {
                        user_email: res.user_email,
                        user_role: res.user_role,
                        is_internal_user: res.is_internal_user,
                    })
                } else {
                    Err(AuthError::Forbidden("Unauthorized Access".to_string()))
                }
            } else {
                Err(AuthError::Unauthorized("Unauthorized Access".to_string()))
            }
        }
        Err(err) => Err(err),
    }
}
```

### 2.5 凭证验证（validate_credentials）

**核心文件：`src/handler/http/auth/validator.rs:227`**

```rust
pub async fn validate_credentials(
    user_id: &str,
    user_password: &str,
    path: &str,
    from_session: bool,
) -> Result<TokenValidationResponse, AuthError> {
    // 1. 从 URL 路径解析 org_id
    let path_columns = path.split('/').collect::<Vec<&str>>();

    // 2. 根据 org_id 获取用户信息
    let user = if path_columns.last().unwrap_or(&"").eq(&"organizations") {
        // organizations 端点特殊处理：优先从 _meta org 查找
        db::user::get_db_user(user_id).await.ok().and_then(|db_user| {
            let all_users = db_user.get_all_users();
            all_users.iter()
                .find(|u| u.org == config::META_ORG_ID)
                .cloned()
                .or_else(|| all_users.first().cloned())
        })
    } else {
        match path.find('/') {
            Some(index) => {
                let org_id = if path_columns.len() > 1 && path_columns[0].eq(V2_API_PREFIX) {
                    path_columns[1]
                } else {
                    &path[0..index]
                };
                if is_root_user(user_id) {
                    users::get_user(Some(DEFAULT_ORG), user_id).await
                } else {
                    users::get_user(Some(org_id), user_id).await
                }
            }
            None => users::get_user(None, user_id).await,
        }
    };

    // 3. ServiceAccount token 验证
    if user.role.is_service_account() && user.token.eq(&user_password) {
        if !config.auth.service_account_enabled {
            return Ok(TokenValidationResponse { is_valid: false, .. });
        }
        // 检查 allow_static_token（非会话 token）
        if !from_session
            && let Ok(org_user) = db::org_users::get(&user.org, &user.email).await
            && !org_user.allow_static_token
        {
            return Ok(TokenValidationResponse { is_valid: false, .. });
        }
        return Ok(build_token_validation_response(&user));
    }

    // 4. 普通 token 验证
    if (path_columns.len() == 1 || INGESTION_EP.iter().any(|s| path_columns.contains(s)))
        && user.token.eq(&user_password)
    {
        return Ok(build_token_validation_response(&user));
    }

    // 5. 【企业版分岔点】原生登录限制
    #[cfg(feature = "enterprise")]
    {
        if !get_dex_config().native_login_enabled && !user.is_external {
            return Ok(TokenValidationResponse { is_valid: false, .. });
        }
        if get_dex_config().root_only_login && !is_root_user(user_id) {
            return Ok(TokenValidationResponse { is_valid: false, .. });
        }
    }

    // 6. 密码验证
    let in_pass = get_hash(user_password, &user.salt);
    if !user.password.eq(&in_pass)
        && !user.password_ext.unwrap_or("".to_string()).eq(&user_password)
    {
        return Ok(TokenValidationResponse { is_valid: false, .. });
    }

    // 7. 用户管理端点权限检查
    if !path.contains("/user")
        || (path.contains("/user")
            && (user.role.eq(&UserRole::Admin)
                || user.role.eq(&UserRole::Root)
                || user.email.eq(user_id)))
    {
        Ok(TokenValidationResponse { is_valid: true, .. })
    } else {
        Err(AuthError::Forbidden("Not allowed".to_string()))
    }
}
```

### 2.6 自动创建组织（check_and_create_org）

**核心文件：`src/handler/http/auth/validator.rs:579`**

```rust
async fn check_and_create_org(user_id: &str, method: &Method, path: &str) -> Result<(), AuthError> {
    let cfg = get_config();
    let path_columns = path.split('/').collect::<Vec<&str>>();

    // 跳过 node/profile 前缀
    if path_columns[0].eq("node") || path_columns[0].eq("profile") {
        return Ok(());
    }

    // 解析 org_id
    let org_id = if path_columns.len() > 2 && path_columns[0].eq("v2") {
        path_columns[1]
    } else {
        path_columns[0]
    };

    // 检查 org 是否已存在
    match get_org(org_id).await {
        Ok(_) => Ok(()),  // org 存在，直接通过
        Err(_) => {
            if !cfg.common.create_org_through_ingestion {
                Err(AuthError::NotFound("Organization not found".to_string()))
            } else if is_root_user(user_id)
                && method.eq(&Method::POST)
                && INGESTION_EP.contains(&path_columns[url_len - 1])
                && crate::service::organization::check_and_create_org(org_id).await.is_ok()
            {
                Ok(())  // Root 用户通过摄入端点自动创建 org
            } else {
                Err(AuthError::NotFound("Organization not found".to_string()))
            }
        }
    }
}
```

**组织创建服务层：`src/service/organization.rs:555`**

```rust
pub async fn check_and_create_org(org_id: &str) -> Result<Organization, anyhow::Error> {
    if let Some(org) = get_org(org_id).await {
        return Ok(org);
    }

    let org = &Organization {
        identifier: org_id.to_owned(),
        name: org_id.to_owned(),
        org_type: if org_id.eq(DEFAULT_ORG) { DEFAULT_ORG } else { CUSTOM }.to_owned(),
        service_account: None,
    };

    match db::organization::save_org(org).await {
        Ok(_) => {
            save_org_tuples(&org.identifier).await;  // 【企业版】同步到 OpenFGA
            #[cfg(feature = "cloud")]
            enqueue_cloud_event(CloudEvent { /* ... */ }).await;
            Ok(org.clone())
        }
        Err(e) => Err(anyhow::anyhow!("Error creating org: {}", e)),
    }
}
```

**社区版无 OFGA 版本：**

```rust
pub async fn check_and_create_org_without_ofga(org_id: &str) -> Result<Organization, anyhow::Error> {
    if let Some(org) = get_org(org_id).await {
        return Ok(org);
    }
    let org = &Organization { /* ... */ };
    match db::organization::save_org(org).await {
        Ok(_) => Ok(org.clone()),  // 不调用 save_org_tuples
        Err(e) => Err(anyhow::anyhow!("Error creating org: {}", e)),
    }
}
```

### 2.7 权限检查 - 企业版（OpenFGA 集成）

**核心文件：`src/handler/http/auth/validator.rs:1035`**

```rust
#[cfg(feature = "enterprise")]
pub(crate) async fn check_permissions(
    user_id: &str,
    auth_info: AuthExtractor,
    role: UserRole,
    _is_external: bool,
) -> bool {
    // 1. OpenFGA 未启用？直接通过
    if !get_openfga_config().enabled {
        return true;
    }

    // 2. 报表失败时临时绕过
    if block_feature_for_report_failure().await {
        return true;
    }

    // 3. 替换资源 ID 中的占位符
    let obj_str = if auth_info.o2_type.contains("##user_id##") {
        auth_info.o2_type.replace("##user_id##", user_id)
    } else {
        auth_info.o2_type
    };

    // 4. Root 用户绕过所有检查
    if role.eq(&UserRole::Root) {
        return true;
    }

    // 5. 创建组织场景：用 _meta org 检查
    let role_str = if auth_info.org_id.eq("organizations") && auth_info.method.eq("POST") {
        match ORG_USERS.get(&format!("{}/{user_id}", config::META_ORG_ID)) {
            Some(user) => format!("{}", user.role),
            None => "".to_string(),
        }
    } else {
        format!("{role}")
    };

    // 6. 确定检查用的 org_id
    let org_id = if auth_info.org_id.eq("organizations") {
        if auth_info.method.eq("POST") {
            config::META_ORG_ID  // 创建组织用 _meta 检查
        } else {
            user_id  // 其他组织操作用 user_id 检查
        }
    } else {
        &auth_info.org_id
    };

    // 7. 调用 OpenFGA 进行细粒度权限检查
    o2_openfga::authorizer::authz::is_allowed(
        org_id,
        user_id,
        &auth_info.method,
        &obj_str,
        &auth_info.parent_id,
        &role_str,
    )
    .await
}
```

### 2.8 权限检查 - 社区版

**核心文件：`src/handler/http/auth/validator.rs:1092`**

```rust
#[cfg(not(feature = "enterprise"))]
pub(crate) async fn check_permissions(
    _user_id: &str,
    _auth_info: AuthExtractor,
    _role: UserRole,
    _is_external: bool,
) -> bool {
    true  // 社区版简化：所有认证用户都有权限
}
```

### 2.9 资源列表权限过滤

**核心文件：`src/handler/http/auth/validator.rs:1115`**

```rust
#[cfg(feature = "enterprise")]
pub(crate) async fn list_objects_for_user(
    org_id: &str,
    user_id: &str,
    permission: &str,
    object_type: &str,
) -> Result<Option<Vec<String>>, AuthError> {
    let openfga_config = get_openfga_config();
    // 非 Root 用户 + OpenFGA 启用 + list_only_permitted 开启 → 过滤
    if !is_root_user(user_id) && openfga_config.enabled && openfga_config.list_only_permitted {
        let role = match users::get_user(Some(org_id), user_id).await {
            Some(user) => user.role.to_string(),
            None => "".to_string(),
        };
        match list_objects(user_id, permission, object_type, org_id, &role).await {
            Ok(resp) => Ok(Some(resp)),  // 返回用户有权限的对象 ID 列表
            Err(_) => Err(AuthError::Forbidden("Unauthorized Access".to_string())),
        }
    } else {
        Ok(None)  // 返回 None 表示不过滤（显示所有）
    }
}
```

### 2.10 细粒度权限检查工具函数

**核心文件：`src/common/utils/auth.rs`**

```rust
#[cfg(feature = "enterprise")]
pub async fn check_permissions(
    object_id: &str,
    org_id: &str,
    user_id: &str,
    object_type: &str,
    method: &str,
    parent_id: Option<&str>,
) -> bool {
    if !is_root_user(user_id) {
        // 从数据库获取用户信息（确保使用规范 email）
        let user: config::meta::user::User = match get_user(Some(org_id), user_id).await {
            Some(user) => user.clone(),
            None => return false,
        };

        // 构建 AuthExtractor 并委托给 validator::check_permissions
        return crate::handler::http::auth::validator::check_permissions(
            &user.email,
            AuthExtractor {
                auth: "".to_string(),
                method: method.to_string(),
                o2_type: format!("{}:{}",
                    OFGA_MODELS.get(object_type).map_or(object_type, |model| model.key),
                    object_id),
                org_id: org_id.to_string(),
                bypass_check: false,
                parent_id: parent_id.unwrap_or("").to_string(),
            },
            user.role,
            user.is_external,
        )
        .await;
    }
    true
}
```

---

## 三、资源 Org 级隔离实现

### 3.1 组织模型定义

**核心文件：`src/common/meta/organization.rs`**

```rust
pub const DEFAULT_ORG: &str = "default";

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct Organization {
    #[serde(default)]
    pub identifier: String,       // org_id，唯一标识
    #[serde(alias = "label")]
    pub name: String,             // 组织显示名称
    #[serde(default)]
    pub org_type: String,         // default / custom
    #[serde(default)]
    pub service_account: Option<String>,
}
```

### 3.2 组织数据库层

**核心文件：`src/infra/src/table/organizations.rs`**

```rust
#[derive(Debug, Clone)]
pub struct OrganizationRecord {
    pub identifier: String,       // org_id（主键）
    pub org_name: String,
    pub org_type: OrganizationType,
    pub created_at: i64,
    pub updated_at: i64,
    #[cfg(feature = "cloud")]
    pub trial_ends_at: i64,
}
```

#### 3.2.1 添加组织

```rust
pub async fn add(
    org_id: &str,
    org_name: &str,
    org_type: OrganizationType,
) -> Result<(), errors::Error> {
    let now = chrono::Utc::now().timestamp_micros();
    let record = ActiveModel {
        identifier: Set(org_id.to_string()),  // 主键：用户指定的 org_id
        org_name: Set(org_name.to_string()),
        org_type: Set(org_type.into()),
        created_at: Set(now),
        updated_at: Set(now),
        #[cfg(feature = "cloud")]
        trial_ends_at: Set(now + day_micros(15)),
    };

    let _lock = get_lock().await;
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;

    match Entity::insert(record).exec(client).await {
        Ok(_) => {
            let mut cache = CACHE.write().await;
            cache.insert(org_id.to_string(), org);
            Ok(())
        }
        Err(e) => match e.sql_err() {
            // 幂等性：唯一约束冲突时静默成功
            Some(SqlErr::UniqueConstraintViolation(_)) => Ok(()),
            _ => Err(Error::DbError(DbError::SeaORMError(e.to_string()))),
        },
    }
}
```

#### 3.2.2 获取组织

```rust
pub async fn get(org_id: &str) -> Result<OrganizationRecord, errors::Error> {
    // 先查缓存
    if let Some(v) = CACHE.read().await.get(org_id) {
        return Ok(v.clone());
    }
    // 缓存未命中查数据库
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;
    let model = Entity::find()
        .filter(Column::Identifier.eq(org_id))  // 按 org_id 过滤
        .one(client)
        .await
        .map_err(|e| Error::DbError(DbError::SeaORMError(e.to_string())))?
        .ok_or_else(|| Error::DbError(DbError::SeaORMError("Org not found".to_string())))?;

    let record = OrganizationRecord::from(model);
    // 更新缓存
    CACHE.write().await.insert(org_id.to_string(), record.clone());
    Ok(record)
}
```

### 3.3 组织设置隔离

**核心文件：`src/service/db/organization.rs`**

```rust
pub const ORG_SETTINGS_KEY_PREFIX: &str = "/organization/setting";

pub async fn set_org_setting(org_name: &str, setting: &OrganizationSetting) -> errors::Result<()> {
    let key = format!("{ORG_SETTINGS_KEY_PREFIX}/{org_name}");
    db::put(&key, json::to_vec(&setting).unwrap().into(), db::NEED_WATCH, None).await?;
    // 缓存组织设置
    ORGANIZATION_SETTING
        .clone()
        .write()
        .await
        .insert(key.to_string(), setting.clone());
    Ok(())
}

pub async fn get_org_setting(org_id: &str) -> Result<OrganizationSetting, Error> {
    let key = format!("{ORG_SETTINGS_KEY_PREFIX}/{org_id}");
    // 先查缓存
    if let Some(v) = ORGANIZATION_SETTING.read().await.get(&key) {
        let mut ret = v.clone();
        ret.free_trial_expiry = trial_period_expiry;
        return Ok(ret);
    }
    // 缓存未命中查数据库
    let mut settings: OrganizationSetting = match db::get(&key).await {
        Ok(settings) => json::from_slice(&settings)?,
        Err(Error::DbError(infra::errors::DbError::KeyNotExists(_))) => {
            OrganizationSetting::default()  // 使用默认设置
        }
        Err(e) => return Err(e),
    };
    // 更新缓存
    ORGANIZATION_SETTING.write().await.insert(key.to_string(), settings.clone());
    Ok(settings)
}
```

### 3.4 用户获取 - 组织上下文感知

**核心文件：`src/service/users.rs`**

```rust
pub async fn get_user(org_id: Option<&str>, name: &str) -> Option<User> {
    let org_id = match org_id {
        Some(local_org) => local_org,
        None => DEFAULT_ORG,
    };
    // 先查缓存（按 org_id 隔离）
    let user = get_cached_user_org(org_id, name);
    match user {
        Some(loc_user) => Some(loc_user),
        None => db::user::get(Some(org_id), name).await.ok().flatten(),
    }
}
```

### 3.5 数据库查询的组织过滤（通用模式）

所有数据库查询都必须带上 org_id 条件，以下是典型模式：

```rust
// 模式1：按 org_id 过滤
Entity::find()
    .filter(Column::OrgId.eq(org_id))
    .all(client)

// 模式2：按 org_id + email 过滤（大小写不敏感）
Entity::find()
    .filter(Column::OrgId.eq(org_id))
    .filter(Expr::expr(Func::lower(Expr::col(Column::Email))).eq(email.to_lowercase()))
    .one(client)

// 模式3：更新时按 org_id 过滤
Entity::update_many()
    .col_expr(Column::Role, Expr::value(role as i16))
    .filter(Column::OrgId.eq(org_id))
    .filter(Expr::expr(Func::lower(Expr::col(Column::Email))).eq(email.to_lowercase()))
    .exec(client)

// 模式4：删除时按 org_id 过滤
Entity::delete_many()
    .filter(Column::OrgId.eq(org_id))
    .filter(Expr::expr(Func::lower(Expr::col(Column::Email))).eq(email.to_lowercase()))
    .exec(client)

// 模式5：JOIN 查询时按 org_id 过滤
Entity::find()
    .filter(Column::OrgId.eq(org_id))
    .inner_join(other::Entity)
    .select_also(other::Entity)
    .all(client)
```

### 3.6 资源所有权与层级关系

**核心文件：`src/common/meta/authz.rs`**

```rust
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct Authz {
    pub obj_id: String,        // 资源 ID
    pub parent_type: String,   // 父资源类型（如 folders）
    pub parent: String,        // 父资源 ID
}
```

**企业版所有权设置（核心文件：`src/common/utils/auth.rs`）：**

```rust
#[cfg(feature = "enterprise")]
pub async fn set_ownership(org_id: &str, obj_type: &str, obj: Authz) {
    if get_openfga_config().enabled {
        let obj_str = format!("{}:{}",
            OFGA_MODELS.get(obj_type).unwrap().key, obj.obj_id);
        let parent_type = if obj.parent_type.is_empty() {
            ""
        } else {
            OFGA_MODELS.get(obj.parent_type.as_str()).unwrap().key
        };
        // 检查父文件夹是否存在（确保层级完整性）
        if obj_type.eq("folders")
            && authorizer::authz::check_folder_exists(org_id, &obj.obj_id).await
        {
            return;
        } else if obj.parent_type.eq("folders") {
            authorizer::authz::check_folder_exists(org_id, &obj.parent).await;
        }
        // 在 OpenFGA 中设置所有权关系
        authorizer::authz::set_ownership(
            org_id, &obj_str, &obj.parent, parent_type
        ).await;
    }
}
```

### 3.7 组织创建与删除时的元数据同步

**核心文件：`src/common/utils/auth.rs`**

```rust
#[cfg(feature = "enterprise")]
pub async fn save_org_tuples(org_id: &str) {
    if get_openfga_config().enabled {
        o2_openfga::authorizer::authz::save_org_tuples(org_id).await
    }
}

#[cfg(feature = "enterprise")]
pub async fn delete_org_tuples(org_id: &str) {
    if get_openfga_config().enabled {
        o2_openfga::authorizer::authz::delete_org_tuples(org_id).await
    }
}
```

---

## 四、企业版与社区版分岔点汇总

### 4.1 功能分岔点总览

| 功能模块 | 企业版 (`feature = "enterprise"`) | 社区版 |
|---------|---------------------------------|--------|
| **权限检查** | 集成 OpenFGA 细粒度权限控制 | 所有认证用户直接通过 |
| **资源列表过滤** | `list_only_permitted` 开启时只返回有权限的资源 | 不过滤，返回所有资源 |
| **组织元数据** | 创建/删除组织时同步到 OpenFGA | 不同步 |
| **原生登录限制** | 支持 `native_login_enabled`、`root_only_login` 配置 | 无此限制 |
| **资源所有权** | 通过 `Authz` 结构在 OpenFGA 中维护层级关系 | 不维护 |
| **扩展凭证验证** | `validate_credentials_ext` 完整实现 | 空实现，返回错误 |
| **组织重命名** | OpenFGA 启用时允许非 Root 用户重命名 | 仅 Root 用户可重命名 |
| **集群同步** | `super_cluster` 模块同步组织变更 | 不启用 |

### 4.2 分岔点代码位置详情

#### 分岔点 1：原生登录限制（validate_credentials）

**文件：`src/handler/http/auth/validator.rs:389`**

```rust
#[cfg(feature = "enterprise")]
{
    if !get_dex_config().native_login_enabled && !user.is_external {
        return Ok(TokenValidationResponse { is_valid: false, .. });
    }
    if get_dex_config().root_only_login && !is_root_user(user_id) {
        return Ok(TokenValidationResponse { is_valid: false, .. });
    }
}
```

#### 分岔点 2：组织创建同步 OpenFGA（check_and_create_org）

**文件：`src/service/organization.rs:573`**

```rust
match db::organization::save_org(org).await {
    Ok(_) => {
        save_org_tuples(&org.identifier).await;  // 仅企业版
        #[cfg(feature = "cloud")]
        enqueue_cloud_event(CloudEvent { /* ... */ }).await;
        Ok(org.clone())
    }
    // ...
}
```

#### 分岔点 3：Viewer 角色自我更新（oo_validator_internal）

**文件：`src/handler/http/auth/validator.rs:168`**

```rust
#[cfg(feature = "enterprise")]
if let Some(role) = &res.user_role
    && role.eq(&UserRole::Viewer)
    && req_data.method.eq(&Method::PUT)
    && path.ends_with(&format!("users/{}", res.user_email))
{
    // Viewer 可以更新自己的信息
    return Ok(AuthValidationResult { /* ... */ });
}
```

#### 分岔点 4：权限检查核心逻辑

**文件：`src/handler/http/auth/validator.rs:1035/1092`**

```rust
// 企业版
#[cfg(feature = "enterprise")]
pub(crate) async fn check_permissions(...) -> bool {
    if !get_openfga_config().enabled { return true; }
    if role.eq(&UserRole::Root) { return true; }
    // ... OpenFGA 检查逻辑 ...
    o2_openfga::authorizer::authz::is_allowed(...).await
}

// 社区版
#[cfg(not(feature = "enterprise"))]
pub(crate) async fn check_permissions(...) -> bool {
    true  // 直接通过
}
```

#### 分岔点 5：资源列表过滤

**文件：`src/handler/http/auth/validator.rs:1115`**

```rust
#[cfg(feature = "enterprise")]
pub(crate) async fn list_objects_for_user(...) -> Result<Option<Vec<String>>, AuthError> {
    let openfga_config = get_openfga_config();
    if !is_root_user(user_id) && openfga_config.enabled && openfga_config.list_only_permitted {
        let role = /* ... */;
        match list_objects(user_id, permission, object_type, org_id, &role).await {
            Ok(resp) => Ok(Some(resp)),  // 返回过滤后的列表
            Err(_) => Err(AuthError::Forbidden("Unauthorized Access".to_string())),
        }
    } else {
        Ok(None)  // 不过滤
    }
}
```

#### 分岔点 6：扩展凭证验证

**文件：`src/handler/http/auth/validator.rs:453/630`**

```rust
// 企业版完整实现
#[cfg(feature = "enterprise")]
pub async fn validate_credentials_ext(...) -> Result<TokenValidationResponse, AuthError> {
    // 完整的 SSO/OAuth 验证逻辑
}

// 社区版空实现
#[cfg(not(feature = "enterprise"))]
pub async fn validate_credentials_ext(...) -> Result<TokenValidationResponse, AuthError> {
    Err(AuthError::Unauthorized("Feature not available".to_string()))
}
```

#### 分岔点 7：组织重命名权限

**文件：`src/service/organization.rs:626`**

```rust
#[cfg(not(feature = "enterprise"))]
let is_allowed = false;
#[cfg(feature = "enterprise")]
let is_allowed = if get_openfga_config().enabled {
    true  // OpenFGA 已处理权限检查
} else {
    false
};
if !is_allowed && !is_root_user(user_email) {
    return Err(anyhow::anyhow!("Not allowed to rename org"));
}
```

#### 分岔点 8：集群同步

**文件：`src/service/db/organization.rs:298/316`**

```rust
// save_org 中
#[cfg(feature = "enterprise")]
super_cluster::organization_add(&key, entry).await?;

// rename_org 中
#[cfg(feature = "enterprise")]
super_cluster::organization_rename(&key, &org).await?;
```

---

## 五、完整调用链路（时序图风格）

### 5.1 认证与权限检查链路

```
HTTP Request
    │
    ▼
auth_middleware (router/mod.rs)
    ├─ RequestData { uri, method, headers }  ── 同步提取
    ├─ AuthExtractor::from_request_parts
    │   ├─ auth = Authorization header
    │   ├─ org_id = /api/{org_id}/...
    │   ├─ method = GET/POST/PUT/DELETE
    │   └─ o2_type = 资源类型 (streams:default/logs)
    │
    ▼
oo_validator (validator.rs:1011)
    └─ oo_validator_internal (validator.rs:120)
        ├─ 解析 user_id / password
        │
        ├─ validate_credentials (validator.rs:227)
        │   ├─ 从 URL 解析 org_id
        │   ├─ users::get_user(Some(org_id), user_id)
        │   │   ├─ get_cached_user_org(org_id, user_id)
        │   │   └─ db::user::get(Some(org_id), user_id)
        │   ├─ ServiceAccount token 验证
        │   │   └─ 检查 allow_static_token 标志
        │   ├─ Token 验证 (token == password)
        │   ├─ 【企业版】native_login_enabled / root_only_login
        │   └─ 密码验证 (get_hash + 比对)
        │
        ├─ check_and_create_org (validator.rs:579)
        │   ├─ org 已存在？→ ✅
        │   ├─ org 不存在？
        │   │   ├─ !create_org_through_ingestion → ❌ 404
        │   │   ├─ is_root + POST + 摄入端点
        │   │   │   └─ service::organization::check_and_create_org(org_id)
        │   │   │       ├─ db::organization::save_org(org)
        │   │   │       ├─ put_into_db_coordinator
        │   │   │       └─ 【企业版】save_org_tuples(org_id) → OpenFGA
        │   │   └─ 其他 → ❌ 404
        │
        ├─ 【企业版】Viewer 自我更新检查
        │
        └─ check_permissions (validator.rs:1035/1092)
            ├─ bypass_check？→ ✅
            ├─ 【企业版】
            │   ├─ !openfga.enabled → ✅
            │   ├─ block_feature_for_report_failure → ✅
            │   ├─ role == Root → ✅
            │   └─ o2_openfga::is_allowed(org_id, user_id, ...)
            └─ 【社区版】→ ✅ (直接返回 true)
    │
    ▼
user_id 插入请求头 → 下游 Handler
```

### 5.2 角色存储链路

```
创建/更新用户角色请求
    │
    ▼
UserRoleRequest → UserOrgRole (common/meta/user.rs:418)
    ├─ base_role = 解析标准角色 (Root/Admin/...)
    └─ custom_role = 企业版自定义角色
    │
    ▼
org_users::add_with_flags (infra/table/org_users.rs:222)
    ├─ id = ider::uuid()  ── 生成 KSUID 主键
    ├─ 构造 ActiveModel
    ├─ Entity::insert(record)
    │   └─ 唯一约束冲突？→ 静默成功
    ├─ put_into_db_coordinator (触发 watch 事件)
    └─ 【企业版】super_cluster 同步
    │
    ▼
watch() 监听到事件 (service/db/org_users.rs:314)
    ├─ ORG_USERS 缓存更新 (key: "{org_id}/{user_email_lower}")
    ├─ USERS_RUM_TOKEN 缓存更新
    └─ ROOT_USER 缓存更新 (如果是 Root)
```

### 5.3 资源访问隔离链路

```
访问资源请求 (GET /api/{org_id}/streams/{stream_name})
    │
    ▼
auth_middleware → 提取 org_id
    │
    ▼
Handler 调用 check_permissions(object_id, org_id, user_id, object_type, method, parent_id)
    ├─ is_root_user(user_id)？→ ✅ 绕过
    ├─ get_user(Some(org_id), user_id)  ── 获取该组织下的用户角色
    └─ 构造 OpenFGA 请求:
       is_allowed(org_id, user_email, method, "stream:{stream_name}", parent_id, role)
    │
    ▼
【企业版】列表查询 → list_objects_for_user(org_id, user_id, "read", "stream")
    └─ OpenFGA list_objects(...) → 返回用户有权限的 stream_id 列表
    │
    ▼
数据库查询强制过滤 org_id
    └─ .filter(Column::OrgId.eq(org_id))
```

### 5.4 真实资源列表入口：Folders 完整调用链分析

我们以 **Folders**（Dashboard/Alert/Report 文件夹）作为真实入口，完整分析 `list_objects_for_user` 到 org 过滤生效的全过程。

#### 5.4.1 调用入口：HTTP Handler → Service Layer

**API 路由定义：**
```
GET /api/{org_id}/folders/{folder_type}
```

**Handler 层调用 Service 层：**
```rust
// 伪代码：HTTP Handler 中调用
list_folders(org_id, Some(user_id), folder_type).await
```

#### 5.4.2 Service 层：list_folders 函数

**核心文件：`src/service/folders.rs:182`**

```rust
#[tracing::instrument()]
pub async fn list_folders(
    org_id: &str,
    user_id: Option<&str>,
    folder_type: FolderType,
) -> Result<Vec<Folder>, FolderError> {
    // 第一步：从 OpenFGA 获取用户有权限的文件夹列表
    let permitted_folders = permitted_folders(org_id, user_id, folder_type).await?;
    
    // 第二步：从数据库获取该 org 下所有文件夹
    let folders = table::folders::list_folders(org_id, folder_type).await?;
    
    // 第三步：根据 folder_type 确定 OpenFGA 模型 key
    #[cfg(feature = "enterprise")]
    let folder_ofga_model = match folder_type {
        FolderType::Dashboards => OFGA_MODELS.get("folders").unwrap().key,
        FolderType::Alerts => OFGA_MODELS.get("alert_folders").unwrap().key,
        FolderType::Reports => OFGA_MODELS.get("report_folders").unwrap().key,
    };
    #[cfg(not(feature = "enterprise"))]
    let folder_ofga_model = "";

    // 第四步：根据 permitted_folders 过滤结果
    let filtered = match permitted_folders {
        Some(permitted_folders) => {
            // 特殊权限：用户对该 org 下所有文件夹都有权限
            if permitted_folders.contains(&format!("{folder_ofga_model}:_all_{org_id}")) {
                folders  // 直接返回所有，不过滤
            } else {
                // 逐一遍历，只保留用户有权限的文件夹
                folders
                    .into_iter()
                    .filter(|folder_loc| {
                        permitted_folders
                            .contains(&format!("{folder_ofga_model}:{}", folder_loc.folder_id))
                    })
                    .collect::<Vec<_>>()
            }
        }
        // permitted_folders = None 表示不过滤（社区版或 OpenFGA 未启用）
        None => folders,
    };
    
    Ok(filtered)
}
```

#### 5.4.3 关键分支：permitted_folders 函数（企业版 vs 社区版）

**社区版实现（直接返回 None，不过滤）：**
**核心文件：`src/service/folders.rs:299`**
```rust
#[cfg(not(feature = "enterprise"))]
async fn permitted_folders(
    _org_id: &str,
    _user_id: Option<&str>,
    _folder_type: FolderType,
) -> Result<Option<Vec<String>>, FolderError> {
    Ok(None)  // 🟢 直接返回 None，表示不进行权限过滤
}
```

**企业版实现（两次调用 list_objects_for_user）：**
**核心文件：`src/service/folders.rs:308`**
```rust
#[cfg(feature = "enterprise")]
async fn permitted_folders(
    org_id: &str,
    user_id: Option<&str>,
    folder_type: FolderType,
) -> Result<Option<Vec<String>>, FolderError> {
    // 根据 folder_type 确定对应的 OpenFGA 模型 key
    let (folder_ofga_model, child_ofga_model) = match folder_type {
        FolderType::Dashboards => (
            OFGA_MODELS.get("folders").unwrap().key,        // "folder"
            OFGA_MODELS.get("dashboards").unwrap().key,     // "dashboard"
        ),
        FolderType::Alerts => (
            OFGA_MODELS.get("alert_folders").unwrap().key,  // "alert_folder"
            OFGA_MODELS.get("alerts").unwrap().key,         // "alert"
        ),
        FolderType::Reports => (
            OFGA_MODELS.get("report_folders").unwrap().key, // "report_folder"
            OFGA_MODELS.get("reports").unwrap().key,        // "report"
        ),
    };

    let Some(user_id) = user_id else {
        return Err(FolderError::PermittedFoldersMissingUser);
    };

    // ════════════════════════════════════════════════════════════════
    // 第一次调用 list_objects_for_user：获取用户有 GET 权限的文件夹
    // ════════════════════════════════════════════════════════════════
    let mut folder_list = crate::handler::http::auth::validator::list_objects_for_user(
        org_id,
        user_id,
        "GET",                    // 权限动作
        folder_ofga_model,        // 对象类型：folder / alert_folder / report_folder
    )
    .await
    .map_err(|err| FolderError::PermittedFoldersValidator(err.to_string()))?;

    // ════════════════════════════════════════════════════════════════
    // 第二次调用 list_objects_for_user：通过子资源反推文件夹权限
    // ════════════════════════════════════════════════════════════════
    // 场景：用户可能没有直接对 Folder 的 GET 权限，但对 Folder 下的某个 Dashboard 有权限
    // 这种情况下也应该能看到该 Folder
    let permitted_dashboards = crate::handler::http::auth::validator::list_objects_for_user(
        org_id,
        user_id,
        "GET_INDIVIDUAL_FROM_ROLE",  // 特殊权限动作
        child_ofga_model,            // 子对象类型：dashboard / alert / report
    )
    .await
    .map_err(|err| FolderError::PermittedFoldersValidator(err.to_string()))?;

    // 从 dashboard ID 中提取 folder ID
    // dashboard ID 格式："dashboard:{folder_id}/{dashboard_id}"
    if let Some(permitted_dashboards) = permitted_dashboards {
        let mut folder_list_with_roles = vec![];
        for dashboard in permitted_dashboards {
            let Some((_, folder_id)) = dashboard.split_once(":") else {
                continue;
            };
            // 从 "folder_id/dashboard_id" 中提取 folder_id
            let Some((folder_id, _)) = folder_id.split_once("/") else {
                continue;
            };
            folder_list_with_roles.push(format!("{folder_ofga_model}:{folder_id}"));
        }
        // 合并两次查询结果
        if let Some(folder_list) = folder_list.as_mut() {
            folder_list.extend(folder_list_with_roles);
        } else {
            folder_list = Some(folder_list_with_roles);
        }
    }

    Ok(folder_list)
}
```

#### 5.4.4 深入：list_objects_for_user 函数内部逻辑

**核心文件：`src/handler/http/auth/validator.rs:1115`**

```rust
#[cfg(feature = "enterprise")]
pub(crate) async fn list_objects_for_user(
    org_id: &str,
    user_id: &str,
    permission: &str,      // e.g. "GET", "GET_INDIVIDUAL_FROM_ROLE"
    object_type: &str,     // e.g. "folder", "dashboard"
) -> Result<Option<Vec<String>>, AuthError> {
    let openfga_config = get_openfga_config();
    
    // ════════════════════════════════════════════════════════════════
    // 三大触发条件（同时满足才进行过滤）：
    // 1. 用户不是 Root
    // 2. OpenFGA 已启用
    // 3. list_only_permitted 配置为 true
    // ════════════════════════════════════════════════════════════════
    if !is_root_user(user_id) && openfga_config.enabled && openfga_config.list_only_permitted {
        // 获取用户在该 org 下的角色
        let role = match users::get_user(Some(org_id), user_id).await {
            Some(user) => user.role.to_string(),
            None => "".to_string(),
        };
        
        // 调用 OpenFGA list_objects API
        match list_objects(user_id, permission, object_type, org_id, &role).await {
            Ok(resp) => Ok(Some(resp)),  // 返回用户有权限的对象 ID 列表
            Err(_) => Err(AuthError::Forbidden("Unauthorized Access".to_string())),
        }
    } else {
        // 不满足过滤条件 → 返回 None，表示不进行权限过滤
        Ok(None)
    }
}

// 社区版空实现（永远返回 None）
#[cfg(not(feature = "enterprise"))]
pub(crate) async fn list_objects_for_user(
    _org_id: &str,
    _user_id: &str,
    _permission: &str,
    _object_type: &str,
) -> Result<Option<Vec<String>>, AuthError> {
    Ok(None)
}
```

#### 5.4.5 数据库层：org_id 过滤生效

**核心文件：`src/infra/src/table/folders.rs:235`**

```rust
async fn list_models(
    db: &DatabaseConnection,
    org_id: &str,
    folder_type: FolderType,
) -> Result<Vec<Model>, sea_orm::DbErr> {
    Entity::find()
        // 🟢 org 级隔离第一道防线：WHERE org = ?
        .filter(Column::Org.eq(org_id))
        // 🟢 类型过滤：WHERE type = ?
        .filter(Column::Type.eq(folder_type_into_i16(folder_type)))
        .order_by(Column::Id, sea_orm::Order::Asc)
        .all(db)
        .await
}
```

#### 5.4.6 完整调用时序图

```
HTTP Request: GET /api/{org_id}/folders/dashboards
    │
    ├─ auth_middleware 验证通过
    │   └─ user_id 插入请求头
    │
    ▼
service::folders::list_folders(org_id, Some(user_id), Dashboards)
    │
    ├─ permitted_folders(org_id, user_id, Dashboards)
    │   │
    │   ├─ 【社区版】→ 返回 Ok(None)  ←───────┐
    │   │                                       │
    │   └─ 【企业版】                           │
    │       ├─ 第 1 次 list_objects_for_user(  │
    │       │    org_id, user_id,              │
    │       │    "GET", "folder")              │
    │       │   ├─ 三条件检查？                 │
    │       │   │   ├─ !is_root_user(user_id)? │
    │       │   │   ├─ openfga.enabled?        │
    │       │   │   └─ list_only_permitted?    │
    │       │   ├─ 全部满足？                   │
    │       │   │   ├─ 是 → OpenFGA list_objects → 返回 Vec<String>
    │       │   │   └─ 否 → 返回 Ok(None)  ────┘
    │       │
    │       └─ 第 2 次 list_objects_for_user(
    │            org_id, user_id,
    │            "GET_INDIVIDUAL_FROM_ROLE", "dashboard")
    │            └─ 同上逻辑
    │
    ├─ table::folders::list_folders(org_id, Dashboards)
    │   └─ list_models(db, org_id, folder_type)
    │       └─ Entity::find()
    │           ├─ .filter(Column::Org.eq(org_id))  ← org 级过滤
    │           └─ .filter(Column::Type.eq(...))
    │
    └─ 结果过滤
        ├─ permitted_folders = Some(list)？
        │   ├─ 包含 "_all_{org_id}"？→ 全部返回
        │   └─ 否则 → filter 只保留匹配的 folder_id
        └─ permitted_folders = None？→ 全部返回
```

#### 5.4.7 过滤生效的四大关键节点

| 节点 | 位置 | 过滤逻辑 | 说明 |
|------|------|---------|------|
| **节点 1** | `list_objects_for_user` 入口 | 三条件检查 | `!is_root && openfga.enabled && list_only_permitted` 必须同时为 true |
| **节点 2** | OpenFGA `list_objects` | 返回用户有权限的对象 ID 列表 | 只有通过检查才会调用 OpenFGA |
| **节点 3** | 数据库 `list_models` | `WHERE org = ?` | 无论权限如何，数据库层始终按 org_id 过滤 |
| **节点 4** | Service 层 `filter()` | 匹配 folder_id | 将数据库结果与 OpenFGA 结果做交集 |

---

## 六、关键设计要点

### 6.1 角色独立性设计

- **多组织角色分离**：一个用户在不同组织可以有不同角色，通过 `Vec<UserOrg>` 实现
- **缓存隔离**：`ORG_USERS` 缓存使用 `"{org_id}/{user_email_lower}"` 作为键，确保组织隔离
- **数据库级隔离**：
  - 主键：`id char(27)` (KSUID)
  - 唯一索引：`(email, org_id)` 保证一个用户在一个组织中只有一条记录
  - 注意：唯一索引顺序是 `(email, org_id)`，不是 `(org_id, email)`！
- **幂等性处理**：`add` 操作遇到唯一约束冲突时静默成功，不返回错误

### 6.2 权限上下文切换

- **URL 驱动**：org_id 从 URL 路径中提取，无需额外参数
- **中间件透传**：user_id 通过请求头传递给下游 handler
- **企业/社区双模式**：通过 `#[cfg(feature = "enterprise")]` 实现两种权限模型
- **OpenFGA 集成**：企业版使用 OpenFGA 实现细粒度 ABAC（基于属性的访问控制）
- **Root 用户豁免**：Root 角色绕过所有权限检查

### 6.3 资源组织级隔离

- **查询强制过滤**：所有数据库查询必须带上 org_id 条件
- **所有权层级**：通过 `Authz` 结构体定义资源父子关系，支持文件夹级权限继承
- **组织设置隔离**：每个组织有独立的 `OrganizationSetting`，互不影响
- **列表过滤**：企业版支持 `list_only_permitted`，只返回用户有权限的资源
- **email 大小写不敏感**：所有查询使用 `lower(email)` 进行匹配

---

## 七、核心文件索引

| 模块 | 文件路径 | 主要职责 |
|------|---------|---------|
| 角色定义 | `src/config/src/meta/user.rs` | `UserRole` 枚举、`DBUser`、`UserOrg` 定义 |
| API 角色模型 | `src/common/meta/user.rs` | `UserOrgRole`、`UserRoleRequest`、角色转换 |
| ID 生成 | `src/config/src/ider.rs` | `uuid()` 生成 KSUID |
| 认证中间件 | `src/handler/http/router/mod.rs` | `auth_middleware` 入口 |
| 认证验证器 | `src/handler/http/auth/validator.rs` | `oo_validator`、`validate_credentials`、`check_permissions`、`check_and_create_org`、`list_objects_for_user` |
| 权限工具 | `src/common/utils/auth.rs` | `AuthExtractor`、`check_permissions`（对外 API）、`set_ownership`、`save_org_tuples` |
| 组织定义 | `src/common/meta/organization.rs` | `Organization`、`OrganizationSetting` |
| 组织数据库表 | `src/infra/src/table/entity/organizations.rs` | 组织表实体定义 |
| 组织数据库操作 | `src/infra/src/table/organizations.rs` | 组织表 ORM 操作 |
| 组织服务层 | `src/service/db/organization.rs` | 组织设置 CRUD、缓存管理 |
| org_users 表实体 | `src/infra/src/table/entity/org_users.rs` | `org_users` 表实体定义 |
| org_users 表迁移 | `src/infra/src/table/migration/m20241227_000300_create_org_users_table.rs` | 建表语句、唯一索引定义 |
| org_users 表操作 | `src/infra/src/table/org_users.rs` | `OrgUserRecord`、ORM CRUD 查询 |
| org_users 服务层 | `src/service/db/org_users.rs` | CRUD、缓存 watch、cache 初始化 |
| 用户服务 | `src/service/users.rs` | `get_user`、`get_user_by_token` |
| 组织服务 | `src/service/organization.rs` | `check_and_create_org`、`rename_org` |

---

## 八、常见误区修正

### 8.1 org_users 主键不是复合主键

❌ 错误：`org_users` 表使用 `(org_id, email)` 作为复合主键

✅ 正确：
- 主键是单独的 `id char(27)` 字段，值为 KSUID（由 `ider::uuid()` 生成）
- 唯一性是通过 `(email, org_id)` 上的**唯一索引**保证的
- 注意唯一索引的列顺序是 `(email, org_id)`，不是 `(org_id, email)`

### 8.2 add 操作的幂等性

`add_with_flags` 遇到唯一约束冲突时不会返回错误，而是静默成功（`Ok(())`），这意味着：
- 重复添加同一个用户到同一个组织不会报错
- 如果需要检测"用户已存在"，需要先调用 `get` 进行检查

### 8.3 email 匹配是大小写不敏感的

所有查询都使用 `lower(email) = lower(input)` 进行匹配，这意味着：
- `User@Example.com` 和 `user@example.com` 被视为同一个用户
- 但数据库存储的是原始大小写的 email

### 8.4 Root 用户的特殊处理

- Root 用户查询时使用 `DEFAULT_ORG` 作为 org_id，而不是 URL 中的 org_id
- Root 用户绕过所有 OpenFGA 权限检查
- 只有 Root 用户可以通过摄入端点自动创建新组织
