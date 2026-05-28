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

**角色转换逻辑：**
```rust
impl From<&UserRoleRequest> for UserOrgRole {
    fn from(role: &UserRoleRequest) -> Self {
        let standard_role = get_roles()
            .into_iter()
            .find(|user_role| user_role.to_string().eq_ignore_ascii_case(&role.role));

        let custom_role = if let Some(role) = role.custom.as_ref() {
            Some(role.clone())
        } else if standard_role.is_none() {
            Some(vec![role.role.clone()])
        } else {
            None
        };

        let base_role = standard_role.unwrap_or_else(get_default_user_role);

        UserOrgRole { base_role, custom_role }
    }
}
```

### 1.4 数据库持久层 - org_users 表

**核心文件：`src/infra/src/table/org_users.rs`**

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

**数据库操作（核心文件：`src/service/db/org_users.rs`）：**

```rust
// 添加用户到组织
pub async fn add(org_id: &str, user_email: &str, role: UserRole, token: &str, rum_token: Option<String>) -> Result<(), anyhow::Error>

// 更新用户在组织中的角色
pub async fn update(org_id: &str, user_email: &str, role: UserRole, token: &str, rum_token: Option<String>) -> Result<(), anyhow::Error>

// 从组织中移除用户
pub async fn remove(org_id: &str, user_email: &str) -> Result<(), anyhow::Error>

// 获取用户在组织中的记录
pub async fn get(org_id: &str, user_email: &str) -> Result<OrgUserRecord, anyhow::Error>

// 列出用户所属的所有组织
pub async fn list_orgs_by_user(user_email: &str) -> Result<Vec<UserOrgExpandedRecord>, anyhow::Error>

// 列出组织中的所有用户
pub async fn list_users_by_org(org_id: &str) -> Result<Vec<OrgUserRecord>, anyhow::Error>
```

### 1.5 缓存层设计

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

### 2.1 认证中间件入口

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

### 2.2 认证信息提取器

**核心文件：`src/common/utils/auth.rs`**

```rust
#[derive(Clone, Debug)]
pub struct AuthExtractor {
    pub auth: String,
    pub method: String,
    pub o2_type: String,
    pub org_id: String,
    pub bypass_check: bool,
    pub parent_id: String,
}
```

**提取逻辑：**
- 从 HTTP 头 `Authorization`、`X-Forwarded-User` 等提取认证信息
- 从 URL 路径中提取 `org_id`（如 `/api/{org_id}/...`）
- 从 URL 路径中提取资源类型（如 `streams`、`dashboards` 等）
- 解析 HTTP 方法（GET/POST/PUT/DELETE）

### 2.3 认证验证器

**核心文件：`src/handler/http/auth/validator.rs`**

```rust
pub async fn validator(
    req_data: &RequestData,
    user_id: &str,
    password: &str,
    auth_info: &AuthExtractor,
    path_prefix: &str,
) -> Result<AuthValidationResult, AuthError> {
    // 1. 验证凭证
    let res = validate_credentials(user_id, password.trim(), path, auth_info.bypass_check).await?;

    if res.is_valid {
        // 2. 检查并创建组织（如果需要）
        check_and_create_org(user_id, &req_data.method, path).await?;

        // 3. 权限检查
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
        Err(AuthError::Unauthorized("Invalid Credentials".to_string()))
    }
}
```

### 2.4 权限检查 - 企业版（OpenFGA 集成）

**核心文件：`src/handler/http/auth/validator.rs`**

```rust
#[cfg(feature = "enterprise")]
pub(crate) async fn check_permissions(
    user_id: &str,
    auth_info: AuthExtractor,
    role: UserRole,
    _is_external: bool,
) -> bool {
    // 如果 OpenFGA 未启用，直接通过
    if !get_openfga_config().enabled {
        return true;
    }

    // Root 用户绕过检查
    if role.eq(&UserRole::Root) {
        return true;
    }

    // 处理特殊场景：创建组织时使用 META_ORG 进行检查
    let org_id = if auth_info.org_id.eq("organizations") {
        if auth_info.method.eq("POST") {
            config::META_ORG_ID
        } else {
            user_id
        }
    } else {
        &auth_info.org_id
    };

    // 替换资源 ID 中的占位符
    let obj_str = if auth_info.o2_type.contains("##user_id##") {
        auth_info.o2_type.replace("##user_id##", user_id)
    } else {
        auth_info.o2_type
    };

    // 调用 OpenFGA 进行细粒度权限检查
    o2_openfga::authorizer::authz::is_allowed(
        org_id,
        user_id,
        &auth_info.method,
        &obj_str,
        &auth_info.parent_id,
        &role.to_string(),
    )
    .await
}
```

### 2.5 权限检查 - 社区版

**核心文件：`src/handler/http/auth/validator.rs`**

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

### 2.6 细粒度权限检查工具函数

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
                o2_type: format!("{}:{}", OFGA_MODELS.get(object_type).map_or(object_type, |model| model.key), object_id),
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

### 2.7 资源列表权限过滤

**核心文件：`src/handler/http/auth/validator.rs`**

```rust
#[cfg(feature = "enterprise")]
pub(crate) async fn list_objects_for_user(
    org_id: &str,
    user_id: &str,
    permission: &str,
    object_type: &str,
) -> Result<Option<Vec<String>>, AuthError> {
    let openfga_config = get_openfga_config();
    if !is_root_user(user_id) && openfga_config.enabled && openfga_config.list_only_permitted {
        let role = match users::get_user(Some(org_id), user_id).await {
            Some(user) => user.role.to_string(),
            None => "".to_string(),
        };
        // 从 OpenFGA 获取用户有权限访问的对象列表
        match list_objects(user_id, permission, object_type, org_id, &role).await {
            Ok(resp) => Ok(Some(resp)),
            Err(_) => Err(AuthError::Forbidden("Unauthorized Access".to_string())),
        }
    } else {
        Ok(None)  // 返回 None 表示不过滤（显示所有）
    }
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
    pub org_type: String,
    #[serde(default)]
    pub service_account: Option<String>,
}
```

### 3.2 组织数据库层

**核心文件：`src/infra/src/table/organizations.rs`**

```rust
#[derive(Debug, Clone)]
pub struct OrganizationRecord {
    pub identifier: String,       // org_id
    pub org_name: String,
    pub org_type: OrganizationType,
    pub created_at: i64,
    pub updated_at: i64,
    #[cfg(feature = "cloud")]
    pub trial_ends_at: i64,
}

// 数据库操作
pub async fn add(org_id: &str, org_name: &str, org_type: OrganizationType) -> Result<(), errors::Error>
pub async fn get(org_id: &str) -> Result<OrganizationRecord, errors::Error>
pub async fn list(filter: ListFilter) -> Result<Vec<OrganizationRecord>, errors::Error>
pub async fn delete(org_id: &str) -> Result<(), errors::Error>
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

### 3.5 数据库查询的组织过滤

**核心文件：`src/infra/src/table/org_users.rs`**

```rust
// 按组织和用户查询（双条件过滤）
pub async fn get(org_id: &str, user_email: &str) -> Result<OrgUserRecord, Error> {
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;
    let model = Entity::find()
        .filter(Column::OrgId.eq(org_id))       // org_id 过滤
        .filter(Column::Email.eq(user_email))   // user_email 过滤
        .one(client)
        .await
        .map_err(|e| Error::DbError(DbError::SeaORMError(e.to_string())))?;
    // ...
}

// 列出用户所属的所有组织
pub async fn list_orgs_by_user(user_email: &str) -> Result<Vec<UserOrgExpandedRecord>, Error> {
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;
    // JOIN org_users 和 organizations 表
    let records: Vec<UserOrgExpandedRecord> = Entity::find()
        .filter(Column::Email.eq(user_email))  // 按用户过滤
        .join(JoinType::InnerJoin, super::organizations::Entity::belongs_to(Entity).into())
        .select_also(super::organizations::Entity)
        .into_model::<UserOrgExpandedRecord>()
        .all(client)
        .await
        .map_err(|e| Error::DbError(DbError::SeaORMError(e.to_string())))?;
    Ok(records)
}

// 列出组织中的所有用户
pub async fn list_users_by_org(org_id: &str) -> Result<Vec<OrgUserRecord>, Error> {
    let client = ORM_CLIENT.get_or_init(connect_to_orm).await;
    let models = Entity::find()
        .filter(Column::OrgId.eq(org_id))       // 按组织过滤
        .order_by_asc(Column::Email)
        .all(client)
        .await
        .map_err(|e| Error::DbError(DbError::SeaORMError(e.to_string())))?;
    // ...
}
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
        let obj_str = format!("{}:{}", OFGA_MODELS.get(obj_type).unwrap().key, obj.obj_id);
        let parent_type = if obj.parent_type.is_empty() {
            ""
        } else {
            OFGA_MODELS.get(obj.parent_type.as_str()).unwrap().key
        };
        // 检查父文件夹是否存在（确保层级完整性）
        if obj_type.eq("folders") && authorizer::authz::check_folder_exists(org_id, &obj.obj_id).await {
            return;
        } else if obj.parent_type.eq("folders") {
            authorizer::authz::check_folder_exists(org_id, &obj.parent).await;
        }
        // 在 OpenFGA 中设置所有权关系
        authorizer::authz::set_ownership(org_id, &obj_str, &obj.parent, parent_type).await;
    }
}
```

### 3.7 组织创建时的初始化

**核心文件：`src/common/utils/auth.rs`**

```rust
#[cfg(feature = "enterprise")]
pub async fn save_org_tuples(org_id: &str) {
    if get_openfga_config().enabled {
        o2_openfga::authorizer::authz::save_org_tuples(org_id).await
    }
}
```

**组织删除时的清理：**

```rust
#[cfg(feature = "enterprise")]
pub async fn delete_org_tuples(org_id: &str) {
    if get_openfga_config().enabled {
        o2_openfga::authorizer::authz::delete_org_tuples(org_id).await
    }
}
```

---

## 四、完整调用链路

### 4.1 用户认证与权限检查链路

```
HTTP 请求
    ↓
auth_middleware (router/mod.rs:160)
    ├─ 提取 RequestData（同步）
    ├─ AuthExtractor::from_request_parts（提取 auth/org_id/资源类型）
    │   └─ 从 URL 路径解析 org_id
    │   └─ 从 Authorization 头提取凭证
    └─ oo_validator (auth/validator.rs)
        ├─ validate_credentials（验证用户名密码/token）
        │   └─ get_user(org_id, user_id)（按组织上下文获取用户）
        │       ├─ get_cached_user_org(org_id, user_email)
        │       └─ db::user::get(Some(org_id), name)
        ├─ check_and_create_org（自动创建组织）
        └─ check_permissions（权限检查）
            ├─ 企业版：调用 OpenFGA is_allowed(org_id, user_id, method, obj_str, parent_id, role)
            └─ 社区版：直接返回 true
    ↓
处理器 handler（user_id 已在请求头中）
```

### 4.2 角色存储链路

```
创建/更新用户角色请求
    ↓
UserRoleRequest → UserOrgRole（common/meta/user.rs:418）
    ├─ 解析 base_role（标准角色）
    └─ 解析 custom_role（企业版自定义角色）
    ↓
org_users::add/update (service/db/org_users.rs)
    ├─ 写入数据库（org_users 表，包含 org_id, email, role, token）
    ├─ put_into_db_coordinator（触发 watch 事件）
    └─ 企业版：super_cluster 同步
    ↓
watch() 监听到事件（service/db/org_users.rs:314）
    ├─ 更新 ORG_USERS 缓存（key: "{org_id}/{user_email}"）
    ├─ 更新 USERS_RUM_TOKEN 缓存
    └─ 更新 ROOT_USER 缓存（如果是 Root 用户）
```

### 4.3 资源访问隔离链路

```
访问资源请求（如 GET /api/{org_id}/streams/{stream_name}）
    ↓
auth_middleware 提取 org_id
    ↓
handler 中调用 check_permissions(object_id, org_id, user_id, object_type, method, parent_id)
    ├─ is_root_user(user_id)？Root 用户绕过
    ├─ get_user(Some(org_id), user_id)（获取用户在该组织的角色）
    └─ 构造 OpenFGA 请求：is_allowed(org_id, user_email, method, "stream:{stream_name}", parent_id, role)
    ↓
企业版 list_objects_for_user（列表查询时）
    └─ OpenFGA list_objects(user_id, "read", "stream", org_id, role)
    └─ 只返回用户有权限的 stream_id 列表
    ↓
数据库查询时过滤 org_id
    └─ 所有查询都带上 org_id 条件
```

---

## 五、关键设计要点

### 5.1 角色独立性设计
- **多组织角色分离**：一个用户在不同组织可以有不同角色，通过 `Vec<UserOrg>` 实现
- **缓存隔离**：`ORG_USERS` 缓存使用 `"{org_id}/{user_email}"` 作为键，确保组织隔离
- **数据库级隔离**：`org_users` 表使用 `(org_id, email)` 作为复合主键

### 5.2 权限上下文切换
- **URL 驱动**：org_id 从 URL 路径中提取，无需额外参数
- **中间件透传**：user_id 通过请求头传递给下游 handler
- **企业/社区双模式**：通过 `#[cfg(feature = "enterprise")]` 实现两种权限模型
- **OpenFGA 集成**：企业版使用 OpenFGA 实现细粒度 ABAC（基于属性的访问控制）

### 5.3 资源组织级隔离
- **查询强制过滤**：所有数据库查询必须带上 org_id 条件
- **所有权层级**：通过 `Authz` 结构体定义资源父子关系，支持文件夹级权限继承
- **组织设置隔离**：每个组织有独立的 `OrganizationSetting`，互不影响
- **列表过滤**：企业版支持 `list_only_permitted`，只返回用户有权限的资源

---

## 六、核心文件索引

| 模块 | 文件路径 | 主要职责 |
|------|---------|---------|
| 角色定义 | `src/config/src/meta/user.rs` | `UserRole` 枚举、`DBUser`、`UserOrg` 定义 |
| API 角色模型 | `src/common/meta/user.rs` | `UserOrgRole`、`UserRoleRequest`、角色转换 |
| 认证中间件 | `src/handler/http/router/mod.rs` | `auth_middleware` 入口 |
| 认证验证器 | `src/handler/http/auth/validator.rs` | `oo_validator`、`check_permissions`、`list_objects_for_user` |
| 权限工具 | `src/common/utils/auth.rs` | `AuthExtractor`、`check_permissions`（对外 API）、`set_ownership` |
| 组织定义 | `src/common/meta/organization.rs` | `Organization`、`OrganizationSetting` |
| 组织数据库 | `src/infra/src/table/organizations.rs` | 组织表 ORM 操作 |
| 组织服务 | `src/service/db/organization.rs` | 组织设置 CRUD、缓存管理 |
| 用户-组织表 | `src/infra/src/table/org_users.rs` | `OrgUserRecord`、ORM 查询 |
| 用户-组织服务 | `src/service/db/org_users.rs` | CRUD、缓存 watch、cache 初始化 |
| 用户服务 | `src/service/users.rs` | `get_user`、`get_user_by_token` |
