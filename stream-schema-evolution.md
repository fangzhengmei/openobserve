# OpenObserve Stream Schema 演进与字段推断实现分析

## 概述

OpenObserve 采用了一套完整的 schema 演进机制，支持在数据写入过程中自动推断字段类型、处理类型冲突、以及记录 schema 版本历史。整个系统围绕 Arrow Schema 构建，确保类型安全的同时提供灵活的演进能力。

---

## 一、核心模块结构

### 1.1 模块分层

```
src/
├── config/src/utils/schema.rs       # Schema 推断与类型转换核心逻辑
├── service/schema.rs                # Schema 检查与演进服务层
├── service/db/schema.rs             # Schema 数据库操作包装层
├── infra/src/schema/
│   ├── mod.rs                       # Schema 缓存与合并核心逻辑
│   └── history/                     # Schema 历史记录存储
│       ├── mod.rs                   # History trait 定义
│       ├── sqlite.rs                # SQLite 实现
│       └── postgres.rs              # PostgreSQL 实现
└── common/meta/stream.rs            # SchemaEvolution 数据结构定义
```

### 1.2 关键数据结构

**SchemaEvolution** (`common/meta/stream.rs:88-91`)
```rust
pub struct SchemaEvolution {
    pub is_schema_changed: bool,      // schema 是否发生变化
    pub types_delta: Option<Vec<Field>>,  // 类型变更字段列表
}
```

**SchemaCache** (`infra/src/schema/mod.rs:740-805`)
- 包装 Arrow Schema，缓存字段索引映射
- 预计算 schema hash 用于快速比较
- 提供字段快速查找能力

---

## 二、写入链路边界：实时写入 vs 历史归档

### 2.1 时间窗口边界定义

**可写入时间范围**：
```
[ now - ingest_allowed_upto , now + ingest_allowed_in_future ]
```

配置参数（`service/schema.rs:57-69`）：
- `ZO_INGEST_ALLOWED_UPTO`: 可写入的最早历史数据（小时）
- `ZO_INGEST_ALLOWED_IN_FUTURE`: 可写入的最远未来数据（小时）

**边界检查点**：
1. **Logs 写入** (`service/logs/ingest.rs:98-99`)
   ```rust
   let min_ts = now - cfg.limit.ingest_allowed_upto_micro;
   let max_ts = now + cfg.limit.ingest_allowed_in_future_micro;
   ```

2. **Traces 写入** (`service/traces/mod.rs:245-246`)
   - 相同时间窗口检查逻辑

3. **Metrics 写入**
   - 同样遵循时间窗口限制

### 2.2 边界处理行为

| 数据时间戳 | 处理方式 | 错误信息 |
|-----------|----------|----------|
| < min_ts | 丢弃 | "Too old data, only last N hours data can be ingested" |
| > max_ts | 丢弃 | "Too far data, only future N hours data can be ingested" |
| 范围内 | 正常写入 | - |

### 2.3 实时写入链路

**入口函数**：`check_for_schema` (`service/schema.rs:81-209`)

```
HTTP/gRPC 请求
    ↓
ingest() [logs/metrics/traces]
    ├─ 时间窗口检查（min_ts / max_ts）
    ├─ 数据解析与展平
    ├─ check_for_schema()  ← Schema 检查边界
    │   ├─ 快速路径：schema_eq() 比较
    │   └─ 慢速路径：handle_diff_schema()
    └─ write_file() → WAL → MemTable
```

**关键设计**：
- Schema 检查发生在**写入 WAL 之前**，确保只有符合 schema 的数据进入系统
- 时间窗口检查在最外层，过早/过晚数据直接拒绝
- Schema 演进仅在**可写入时间窗口内**的数据触发

### 2.4 历史归档链路

历史数据（已持久化到 Parquet 文件）不触发 schema 演进：

```
查询请求
    ↓
search()
    ├─ 按时间范围选择 Parquet 文件
    ├─ 按时间范围选择对应 schema 版本
    ├─ 多版本 schema 合并查询
    └─ 结果返回
```

**边界结论**：
- **实时写入链路**：触发 schema 推断、演进、版本创建
- **历史归档链路**：只读 schema 版本，不触发演进

---

## 三、首次写入字段推断流程

### 3.1 推断入口

`check_for_schema` 函数 (`service/schema.rs:81-209`) 是 schema 检查的主入口：

1. **获取缓存 schema**：先从 `STREAM_SCHEMAS_LATEST` 缓存获取
2. **推断新 schema**：调用 `infer_json_schema_from_map` 从记录中推断
3. **快速路径比较**：使用 `schema_eq` 比较 schema 是否相同
4. **慢路径处理**：如 schema 不同，进入 `handle_diff_schema`

### 3.2 字段类型推断算法

**infer_json_schema_from_map** (`config/src/utils/schema.rs:55-80`)

核心逻辑在 `infer_json_schema_from_object` (`config/src/utils/schema.rs:118-156`)：

| JSON 类型 | 推断 Arrow 类型 |
|-----------|----------------|
| String    | Utf8           |
| Number(i64) | Int64        |
| Number(u64) | UInt64       |
| Number(f64) | Float64      |
| Boolean   | Boolean        |
| Null      | 忽略           |
| 其他      | 报错           |

### 3.3 类型提升规则

**convert_data_type** (`config/src/utils/schema.rs:158-208`) 处理同一字段在不同记录中的类型冲突：

```
Int64  → UInt64 | Float64 | Utf8 | LargeUtf8
UInt64 → Float64 | Utf8 | LargeUtf8
Float32 → Float64
Float64 → Utf8 | LargeUtf8
Int8 → Int16 → Int32 → Int64
UInt8 → UInt16 → UInt32 → UInt64
Boolean → 任意类型
Utf8 → LargeUtf8
```

> **设计意图**：遵循" widening conversion"（拓宽转换）原则，确保数据不会因类型变更而丢失。

### 3.4 Schema 标准化

**fix_schema** (`config/src/utils/schema.rs:213-266`) 对推断出的 schema 进行标准化：

1. 移除 `Null` 类型字段
2. Traces 流强制添加 `start_time`、`end_time`（UInt64）
3. `_timestamp` 字段强制为 Int64 且非空
4. Metrics 流的 `__hash__` 字段强制为 UInt64
5. 按字段名称排序（确保一致性）

---

## 四、Schema 演进与冲突处理

### 4.1 Schema 变更检测

**get_schema_changes** (`service/schema.rs:574-615`)

```rust
pub fn get_schema_changes(schema: &SchemaCache, inferred_schema: &Schema) 
    -> (bool, Vec<Field>)
```

检测逻辑：
1. **新字段检测**：inferred_schema 中有而 schema 中没有 → is_schema_changed = true
2. **类型变更检测**：
   - 拓宽转换（如 Int32→Int64）：更新 schema，记录 delta
   - 收窄转换（如 Int64→Int32）：不更新 schema，添加 `zo_cast` 标记到 delta

### 4.2 Schema 合并核心逻辑

**get_merge_schema_changes** (`infra/src/schema/mod.rs:688-738`)

```rust
pub fn get_merge_schema_changes(schema: &Schema, inferred_schema: &Schema) 
    -> (bool, Vec<Field>, Vec<Field>)
```

返回值：
- `bool`: schema 是否实际变更
- `Vec<Field>`: 类型变更 delta（含 zo_cast 标记）
- `Vec<Field>`: 合并后的完整字段列表

### 4.3 类型拓宽判定

**is_widening_conversion** (`infra/src/schema/mod.rs:816-891`)

定义了完整的类型拓宽矩阵，决定哪些类型转换被允许并触发 schema 更新：

| 源类型 | 允许的目标类型 |
|--------|----------------|
| Boolean | Int8/16/32/64, Float16/32/64, Utf8, LargeUtf8 |
| Int8 | Int16/32/64, Float16/32/64, Utf8, LargeUtf8 |
| Int32 | Int64, UInt32/64, Float64, Utf8, LargeUtf8 |
| Int64 | UInt64, Float64, Utf8, LargeUtf8 |
| UInt32 | UInt64, Utf8, LargeUtf8 |
| Float32 | Float64, Utf8, LargeUtf8 |
| Utf8 | LargeUtf8 |

### 4.4 Schema 版本演进机制

**merge** (`infra/src/schema/mod.rs:419-538`) 是 schema 更新的核心：

#### 场景 1：新流首次写入
- 直接创建 schema，设置 `created_at` 和 `start_dt` 元数据
- 不创建新版本（只有初始版本）

#### 场景 2：字段添加 / 类型拓宽
- 判断是否需要新版本：过滤 delta 中带 `zo_cast` 标记的字段
- 如需新版本：
  1. 更新旧版本 schema：添加 `end_dt = record_ts`
  2. 创建新版本 schema：设置 `start_dt = record_ts`
- 如不需新版本：直接更新最新 schema

#### 场景 3：类型收窄（Int64 → Int32）
- 不更新 schema
- 在 delta 字段中添加 `zo_cast: true` 元数据标记
- 写入时按原 schema 进行类型转换

### 4.5 乱序时间触发版本补齐

**判定逻辑** (`service/schema.rs:164-173, 193-206`)

```rust
// 触发条件
if !field_datatype_delta.is_empty() {
    if let Some(start_dt) = schema_metadata.get("start_dt") {
        let created_at = start_dt.parse().unwrap_or_default();
        if record_ts <= created_at {  // 关键判定
            need_insert_new_latest = true;
        }
    }
}

// 执行补齐
if need_insert_new_latest {
    _ = handle_diff_schema(
        ...,
        now_micros(),  // 使用当前时间作为新版本 start_dt
        ...
    ).await?;
}
```

**触发条件**：
1. 存在类型变更 delta（`field_datatype_delta` 非空）
2. 记录时间戳 ≤ 当前 schema 版本的 `start_dt`

**执行结果**：
- 创建一个**新的 schema 版本**，`start_dt = now_micros()`（当前系统时间）
- 原 schema 版本保持不变（`end_dt` 不修改）
- 新记录使用新 schema 版本写入

**设计意图**：
- 防止乱序旧数据的类型变更污染已经存在的 schema 版本
- 确保时间戳较早的数据不会导致已有的 schema 版本"提前"开始
- 新版本从当前时间开始，保证时序一致性

### 4.6 并发控制

**handle_diff_schema** (`service/schema.rs:242-479`) 中的并发保护：

1. **本地锁**：`infra::local_lock::lock(&cache_key)` 确保单节点内串行更新
2. **双重检查**：获取锁后再次检查缓存，避免重复更新
3. **重试机制**：数据库事务失败时重试（`meta_transaction_retries` 次）
4. **缓存更新**：数据库更新成功后同步更新内存缓存

---

## 五、Schema 历史记录系统

### 5.1 存储设计

**表结构** (`infra/src/schema/history/sqlite.rs:90-110`)

```sql
CREATE TABLE schema_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    org          VARCHAR NOT NULL,          -- 组织ID
    stream_type  VARCHAR NOT NULL,          -- 流类型 (logs/metrics/traces)
    stream_name  VARCHAR NOT NULL,          -- 流名称
    start_dt     INTEGER NOT NULL,          -- 版本生效时间戳（微秒）
    value        TEXT NOT NULL              -- Schema JSON 序列化
);
```

**索引设计**：
- `schema_history_org_idx`: org 字段普通索引
- `schema_history_stream_idx`: (org, stream_type, stream_name) 联合索引
- `schema_history_stream_version_idx`: (org, stream_type, stream_name, start_dt) 唯一索引

### 5.2 版本管理机制

**Schema 元数据字段**：
- `created_at`: 流创建时间
- `start_dt`: 当前版本生效起始时间
- `end_dt`: 当前版本生效结束时间（新版本创建时设置）

**版本查询** (`infra/src/schema/mod.rs:165-248`) - `get_versions` 函数：
1. 优先从内存缓存 `STREAM_SCHEMAS` 获取版本列表
2. 缓存未命中时从数据库加载
3. 支持按时间范围过滤版本

### 5.3 历史记录触发点

Schema 历史记录在以下情况创建：
1. **新字段添加**：导致字段数量增加
2. **类型拓宽**：如 Int32 → Int64
3. **字段删除**：用户手动删除字段时
4. **乱序补齐**：旧数据触发新版本创建时

> **注意**：类型收窄（带 zo_cast 标记）不会创建历史记录。

### 5.4 旧数据回溯支持

**filter_schema_version_id** (`service/db/schema.rs:922-940`)

根据查询时间范围选择合适的 schema 版本：
```rust
pub fn filter_schema_version_id(schemas: &[Schema], _start_dt: i64, end_dt: i64) 
    -> Option<usize>
```

逻辑：找到第一个 `end_dt < schema.end_dt` 的版本，确保查询历史数据时使用当时的 schema。

---

## 六、用户定义 Schema (UDS) 三类流行为对比

### 6.1 UDS 支持范围

**support_uds()** (`config/src/meta/stream.rs:146-151`)

```rust
pub fn support_uds(&self) -> bool {
    matches!(
        *self,
        StreamType::Logs | StreamType::Metrics | StreamType::Traces
    )
}
```

| 流类型 | 支持 UDS |
|--------|----------|
| Logs | ✅ |
| Metrics | ✅ |
| Traces | ✅ |
| 其他类型 | ❌ |

### 6.2 自动启用条件

**handle_diff_schema** (`service/schema.rs:353-358`)

三类流自动启用 UDS 的条件**完全相同**：

```rust
if cfg.common.allow_user_defined_schemas
    && cfg.limit.schema_max_fields_to_enable_uds > 0
    && stream_type.support_uds()        // 三类流都支持
    && defined_schema_fields.is_empty()
    && final_schema.fields().len() > cfg.limit.schema_max_fields_to_enable_uds
{
    // 自动启用 UDS
}
```

### 6.3 强制保留字段差异

**check_schema_for_defined_schema_fields** (`service/schema.rs:525-572`)

UDS 启用时，三类流有不同的**强制保留字段**：

#### Logs 流
- **无额外强制字段**
- 仅保留通用系统字段：`_timestamp`, `_all`, `_id`, `_original`, `_all_values`

#### Metrics 流
强制保留以下字段（`service/schema.rs:533-542`）：
```rust
fields.insert("__name__".to_string());
fields.insert("__hash__".to_string());
fields.insert("__bucket__".to_string());
fields.insert("__quantile__".to_string());
fields.insert("__exemplar__".to_string());
fields.insert("__value__".to_string());
fields.insert("trace_id".to_string());
fields.insert("span_id".to_string());
```

#### Traces 流
强制保留以下字段（`service/schema.rs:543-567`）：
```rust
// 核心 tracing 字段
fields.insert("service_name".to_string());
fields.insert("operation_name".to_string());
fields.insert("trace_id".to_string());
fields.insert("span_id".to_string());
fields.insert("span_kind".to_string());
fields.insert("span_status".to_string());
fields.insert("reference_parent_span_id".to_string());
fields.insert("reference_parent_trace_id".to_string());
fields.insert("reference_ref_type".to_string());
fields.insert("start_time".to_string());
fields.insert("end_time".to_string());
fields.insert("duration".to_string());
fields.insert("events".to_string());

// Gen-AI 字段自动包含
for field in schema.fields() {
    let name = field.name();
    if name.starts_with("gen_ai_")
        || name.starts_with("llm_")
        || name == "user_id"
        || name == "session_id"
    {
        fields.insert(name.to_string());
    }
}
```

### 6.4 UDS 字段选择策略对比

| 维度 | Logs | Metrics | Traces |
|------|------|---------|--------|
| 强制保留字段 | 仅系统字段 | 8 个 metrics 关键字段 | 12 个 tracing 字段 + Gen-AI 自动发现 |
| Gen-AI 自动包含 | ❌ | ❌ | ✅ |
| 字段排序 | 按名称 | 按名称 | 按名称 |
| 触发阈值 | 相同配置 | 相同配置 | 相同配置 |

### 6.5 Schema 过滤逻辑

**generate_schema_for_defined_schema_fields** (`service/schema.rs:484-523`)

当 UDS 启用且字段数超过阈值时，只保留定义的字段：
- 按名称排序确保一致性
- 特殊字段自动保留（时间戳、系统字段）
- 不同流类型有各自的强制保留字段

---

## 七、缓存系统设计

### 7.1 多级缓存结构

```
STREAM_SCHEMAS_LATEST (RwAHashMap)
  └── key: "{org}/{stream_type}/{stream_name}"
  └── value: SchemaCache (最新版本 schema)

STREAM_SCHEMAS (RwAHashMap)
  └── key: "{org}/{stream_type}/{stream_name}"
  └── value: Vec<(start_dt, Schema)> (所有版本)

STREAM_SETTINGS (RwAHashMap)
  └── key: "{org}/{stream_type}/{stream_name}"
  └── value: StreamSettings (流配置)
```

### 7.2 缓存加载流程

**cache()** (`service/db/schema.rs:600-678`) 启动时预加载：
1. 从数据库读取所有 schema 记录
2. 按流分组，提取每个流的最新版本
3. 填充 `STREAM_SCHEMAS_LATEST`、`STREAM_SETTINGS`
4. 构建完整版本列表填充 `STREAM_SCHEMAS`

### 7.3 变更监听

**watch()** (`service/db/schema.rs:373-598`) 实时同步：
1. 监听 `/schema/` 前缀的数据库变更事件
2. Put 事件：更新对应流的缓存
3. Delete 事件：清理缓存及相关资源

---

## 八、关键代码路径

### 8.1 数据写入时 Schema 检查流程

```
ingest()
  ↓
时间窗口检查 [min_ts, max_ts]
  ↓ （数据在范围内才继续）
check_for_schema()  [service/schema.rs:81]
  ├─ 从缓存获取 schema
  ├─ infer_json_schema_from_map()  [config/utils/schema.rs:55]
  │   └─ infer_json_schema_from_object()
  │       └─ convert_data_type()
  ├─ schema_eq() 快速比较
  ├─ get_schema_changes() 检测变更
  ├─ 乱序检测：record_ts <= start_dt ?
  │   └─ 是：need_insert_new_latest = true
  └─ handle_diff_schema()  [service/schema.rs:242]
      ├─ local_lock 并发控制
      └─ db::schema::merge()  [service/db/schema.rs:54]
          └─ infra::schema::merge()  [infra/schema/mod.rs:419]
              ├─ get_merge_schema_changes()
              ├─ 决定是否创建新版本
              ├─ history::create() 记录历史
              └─ 更新 STREAM_SCHEMAS_LATEST 缓存
                  ↓ （乱序时二次调用）
                  handle_diff_schema(now_micros())
```

### 8.2 核心文件速查表

| 功能 | 文件 | 关键函数 |
|------|------|----------|
| 字段推断 | `config/src/utils/schema.rs` | `infer_json_schema_from_map`, `convert_data_type` |
| Schema 检查 | `service/schema.rs` | `check_for_schema`, `handle_diff_schema` |
| Schema 合并 | `infra/src/schema/mod.rs` | `merge`, `get_merge_schema_changes` |
| 类型拓宽 | `infra/src/schema/mod.rs` | `is_widening_conversion` |
| 乱序补齐 | `service/schema.rs` | `check_for_schema` (行 164-206) |
| 历史记录 | `infra/src/schema/history/` | `create`, `create_table` |
| 缓存管理 | `service/db/schema.rs` | `cache`, `watch`, `list` |
| UDS 差异 | `service/schema.rs` | `check_schema_for_defined_schema_fields` |

---

## 九、设计特点总结

1. **宽表兼容**：支持动态字段添加，无需预先定义 schema
2. **类型安全**：通过拓宽转换保证数据完整性，收窄转换用 cast 标记
3. **版本化演进**：每个 schema 变更带时间戳，支持历史数据回溯查询
4. **乱序友好**：旧数据触发新版本补齐，保证时序一致性
5. **高并发设计**：本地锁 + 双重检查 + 数据库事务重试
6. **性能优化**：内存缓存 + 哈希快速比较 + 快/慢路径分离
7. **持久化历史**：独立的 schema_history 表记录每次演进
8. **灵活配置**：支持用户定义 schema（UDS）限制字段爆炸
9. **类型感知**：三类流（Logs/Metrics/Traces）UDS 行为差异化设计
10. **边界清晰**：实时写入触发演进，历史归档只读查询

