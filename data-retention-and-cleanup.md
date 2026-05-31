# OpenObserve 数据保留与后台清理协同机制——源码深度分析（校正版）

> **文档版本**：v2.0（基于源码逐行核对校正）
> **核对范围**：`src/service/compact/`、`src/service/db/compact/`、`src/infra/src/file_list/`（postgres.rs、sqlite.rs、mod.rs）、`src/config/src/meta/stream.rs`
> **关键校正点**：batch_process 真实落库行为、file_list_deleted 无唯一约束、Retry 幂等性边界

---

## 1. file_list 删除的真实落库行为（源码核校正）

### 1.1 核心校正：deleted=true 不是逻辑标记，是物理删除

#### 1.1.1 代码执行流程

`retention.rs:522-568 delete_from_file_list()`：

```rust
// retention.rs:535-558
let files = file_list::query(
    &fake_trace_id, org_id, stream_type, stream_name,
    PartitionTimeLevel::Hour, time_range, None,
).await?;

for file in files.into_iter() {
    file.deleted = true;  // 内存标记，区分 add_items 和 del_items
    // ... 按小时分组
}
// 调用 write_file_list 写入
write_file_list(org_id, hours_files).await?;
```

> **源码定位**：`src/service/compact/retention.rs:557` — `file.deleted = true;` 是内存标记

#### 1.1.2 batch_process 的真实 SQL 行为

`inner_batch_process`（`postgres.rs:1878-2022`、`sqlite.rs:1525-1645`）分两步处理：

**第一步：add_items（`!v.deleted`）**
```sql
-- postgres.rs:1906-1924
INSERT INTO file_list (..., deleted, ...)
VALUES (..., false, ...)
ON CONFLICT DO NOTHING;

-- sqlite.rs:1540-1570
INSERT INTO file_list (id, ..., deleted, ...)
VALUES (?, ..., false, ...)
ON CONFLICT(id) DO NOTHING;
```

**第二步：del_items（`v.deleted`）**
```sql
-- postgres.rs:1967-1998
SELECT id FROM file_list WHERE stream = $1 AND date = $2 AND file = $3;
DELETE FROM file_list WHERE id IN(...);

-- sqlite.rs:1599-1623
SELECT id FROM file_list WHERE stream = $1 AND date = $2 AND file = $3;
DELETE FROM file_list WHERE id IN(...);
```

> **源码定位**：`postgres.rs:1998` — `DELETE FROM file_list WHERE id IN({});`
> **源码定位**：`sqlite.rs:1623` — `DELETE FROM file_list WHERE id IN({});`

#### 1.1.3 关键结论（校正 v1.0）

| 项目 | v1.0 描述（错误） | v2.0 校正（正确） |
|------|------------------|------------------|
| `file.deleted = true` 作用 | 逻辑删除标记，写入 file_list 表 | 内存标记，仅用于区分 add_items 和 del_items |
| batch_process 对 del_items 的操作 | `UPDATE file_list SET deleted = true` | `SELECT id` → `DELETE FROM file_list WHERE id IN(...)` |
| file_list 表记录状态 | 记录保留，`deleted=true` | 记录被物理删除 |
| 查询引擎是否需要过滤 deleted | 是，查询时加 `WHERE deleted = false` | 否，记录已不存在于 file_list 表 |

> **注**：`file_list` 表确实有 `deleted BOOLEAN default false not null` 列（`postgres.rs:3269`、`sqlite.rs:1661`），但该列仅用于 **Merge 操作中新文件插入时的标识**（插入时硬编码为 `false`），在删除路径中不使用。

### 1.2 三种 file_list_deleted_mode 的精确行为

#### 1.2.1 模式定义

`FileListBookKeepMode`（`config/src/meta/stream.rs:1325-1330`）：
```rust
pub enum FileListBookKeepMode {
    History,     // "history"
    #[default]
    Deleted,     // "deleted"
    None,        // "none"
}
```

#### 1.2.2 Retention 删除路径（`retention.rs:571-637`）

`write_file_list` 按模式分支执行：

| 步骤 | `Deleted`（默认） | `History` | `None` |
|------|-------------------|-----------|--------|
| ① `batch_add_history` | ❌ 跳过 | ✅ 插入 `file_list_history` | ❌ 跳过 |
| ② `batch_process` | ✅ 从 `file_list` 物理删除 | ✅ 从 `file_list` 物理删除 | ✅ 从 `file_list` 物理删除 |
| ③ `batch_add_deleted` | ✅ 插入 `file_list_deleted` | ❌ 跳过 | ❌ 跳过 |

> **源码定位**：`retention.rs:582-597` — History 模式先调用 batch_add_history，再调用 batch_process
> **源码定位**：`retention.rs:599-604` — 无论什么模式，都必须执行 batch_process
> **源码定位**：`retention.rs:606-628` — 只有 Deleted 模式才调用 batch_add_deleted

#### 1.2.3 batch_add_history 的实现（校正 v1.0）

`batch_add_history`（`postgres.rs:132-134`、`sqlite.rs:129-131`）：
```rust
async fn batch_add_history(&self, files: &[FileKey]) -> Result<()> {
    self.inner_batch_process("file_list_history", files).await
}
```

调用 `inner_batch_process("file_list_history", files)` 时：
- `add_items` 被过滤为 `!v.deleted` 的文件
- 插入时 `deleted` 硬编码为 `false`（`postgres.rs:1924`、`sqlite.rs:1560`）
- 所以 `retention.rs:589-592` 必须先 `deleted: false` clone 一份：
  ```rust
  let events = events.iter().map(|v| FileKey {
      deleted: false,  // 必须设为 false 才能被 add_items 过滤到
      ..v.clone()
  }).collect::<Vec<_>>();
  infra_file_list::batch_add_history(&events).await?;
  ```

> **源码定位**：`retention.rs:589-592` — clone 时将 deleted 设为 false

#### 1.2.4 Merge/Dump 路径（不受模式影响）

Merge 的 `write_file_list`（`merge.rs:1035-1106`）和 Dump 的 `delete_by_time_range`（`dump.rs:476-494`）**不检查** `file_list_deleted_mode`，始终执行：
1. `batch_process(events)` — 插入新文件、删除旧文件
2. `batch_add_deleted(org_id, created_at, &del_items)` — 旧文件写入 `file_list_deleted`

**合理性**：Merge 是替换操作（旧文件 → 新文件），旧文件的存储空间必须回收，不受保留模式影响。

#### 1.2.5 孤儿文件风险矩阵

| 模式 | Retention 删除的文件 | Merge 替换的旧文件 | 孤儿文件风险 |
|------|---------------------|-------------------|------------|
| `Deleted` | 延迟物理删除 ✅ | 延迟物理删除 ✅ | 无 |
| `History` | 保留在存储 ✅（归档到 history 表） | 延迟物理删除 ✅ | 无 |
| `None` | **成为孤儿文件** ⚠️（file_list 记录被删但未入 deleted 表） | 延迟物理删除 ✅ | **Retention 产生的孤儿** |

---

## 2. file_list_deleted 去重依据与唯一约束核查

### 2.1 表结构定义

**Postgres（主库）** `postgres.rs:2778-2786`、`3310-3320`：
```sql
CREATE TABLE IF NOT EXISTS file_list_deleted (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account VARCHAR(128) not null,
    org VARCHAR(100) not null,
    stream VARCHAR(256) not null,
    date VARCHAR(16) not null,
    file VARCHAR(1024) not null,
    index_file BOOLEAN default false not null,
    flattened BOOLEAN default false not null,
    created_at BIGINT not null
)
```

**SQLite** `sqlite.rs:1686-1704`：
```sql
CREATE TABLE IF NOT EXISTS file_list_deleted (
    id INTEGER not null primary key autoincrement,
    account VARCHAR not null,
    org VARCHAR not null,
    stream VARCHAR not null,
    date VARCHAR not null,
    file VARCHAR not null,
    index_file BOOLEAN default false not null,
    flattened BOOLEAN default false not null,
    created_at BIGINT not null
)
```

### 2.2 唯一约束核查

| 约束类型 | Postgres | SQLite |
|---------|----------|--------|
| `id` 主键 | ✅ 有 | ✅ 有 |
| `(stream, date, file)` 唯一约束 | ❌ 无 | ❌ 无 |
| `ON CONFLICT` 去重 | ❌ 无（`batch_add_deleted` 直接 INSERT） | ❌ 无（`batch_add_deleted` 直接 INSERT） |

> **源码定位**：`postgres.rs:237-239` — INSERT 语句无 `ON CONFLICT`
> **源码定位**：`sqlite.rs:187-189` — INSERT 语句无 `ON CONFLICT`

### 2.3 重复记录的产生机制

在 Retention 的 `write_file_list` 重试循环中：
```rust
// retention.rs:580-631
for _ in 0..5 {
    // batch_process（幂等）
    if let Err(e) = infra_file_list::batch_process(&events).await { continue; }
    // batch_add_deleted（非幂等！）
    if let Err(e) = infra_file_list::batch_add_deleted(org_id, created_at, &del_items).await {
        continue;  // 重试时 batch_process 已成功，但 batch_add_deleted 会再次 INSERT
    }
}
```

**风险场景**：
1. 第一次循环：`batch_process` 成功，`batch_add_deleted` 因网络波动失败
2. 第二次循环：`batch_process` 幂等成功，`batch_add_deleted` 再次 INSERT 相同文件
3. 结果：`file_list_deleted` 表中出现两条 (stream, date, file) 相同但 id 不同的记录

### 2.4 重复记录的处理机制

#### 2.4.1 延迟删除读取

`query_deleted`（`postgres.rs:645-663`、`sqlite.rs:645-662`）：
```sql
SELECT id, account, stream, date, file, index_file, flattened 
FROM file_list_deleted 
WHERE org = $1 AND created_at < $2 
ORDER BY created_at ASC LIMIT $3;
```

**返回所有匹配记录**，包括重复的。重复记录会被多次送到 `deleted::delete()` 执行。

#### 2.4.2 物理删除去重

`storage::del()` 支持批量删除，底层存储 API 对不存在的文件返回 "not found" 错误，代码中做了容错（`deleted.rs:47-51`）：
```rust
if !e.to_string().to_lowercase().contains("not found") {
    log::error!("[COMPACTOR] delete files from storage failed: {e}");
    return Err(e.into());
}
```

**存储删除的重复调用无副作用**。

#### 2.4.3 表记录删除

`batch_remove_deleted`（`postgres.rs:269-329`、`sqlite.rs:216-258`）通过 **(stream, date, file) 查询 id**：
```sql
-- postgres.rs:291、sqlite.rs:239
SELECT id FROM file_list_deleted WHERE stream = $1 AND date = $2 AND file = $3;
DELETE FROM file_list_deleted WHERE id IN(...);
```

**关键**：使用 `fetch_one()` 只返回**第一个匹配的 id**。

| 调用位置 | 方法 | 重复记录处理 |
|---------|------|-------------|
| `postgres.rs:296` | `fetch_one(&pool).await` | 每次只取第一个匹配的 id |
| `sqlite.rs:244` | `fetch_one(&*pool).await` | 每次只取第一个匹配的 id |

**重复记录的完全清理**：如果同一份文件有 N 条重复记录，需要 N 次 `run_delay_deletion` 循环才能全部删除。每次删除一条，下一轮 `query_deleted` 会返回剩下的。

### 2.5 去重依据总结

| 层面 | 去重机制 | 有效性 |
|------|---------|--------|
| 数据库约束 | 无唯一约束，id 自增主键 | ❌ 无法防重复插入 |
| Retention 重试 | 无 `mark_deleted_done` 标志，重试时重复调用 batch_add_deleted | ❌ 可能产生重复 |
| Merge 重试 | 有 `mark_deleted_done` 标志，重试时不重复 batch_add_deleted | ✅ Merge 路径不产生重复 |
| 物理删除 | 存储 API "not found" 容错 | ✅ 重复删除无副作用 |
| 表记录删除 | `fetch_one()` 每次只删一条，多轮循环清理 | ✅ 最终全部删除，只是需要多轮 |

---

## 3. 并发重试结论的完整证据链

### 3.1 Retention write_file_list 重试（`retention.rs:580-637`）

```rust
// retention.rs:580-631
let mut success = false;
let created_at = Utc::now().timestamp_micros();
for _ in 0..5 {
    // History 模式：batch_add_history
    if mode == History {
        let events = events.iter().map(|v| FileKey {
            deleted: false, ..v.clone()
        }).collect::<Vec<_>>();
        infra_file_list::batch_add_history(&events).await?;  // 失败不重试，直接 return
    }
    // batch_process（必须成功）
    if let Err(e) = infra_file_list::batch_process(&events).await {
        log::error!("retrying: {e}");
        tokio::time::sleep(Duration::from_secs(1)).await;
        continue;
    }
    // Deleted 模式：batch_add_deleted
    if mode == Deleted {
        let del_items = ...;
        if let Err(e) = infra_file_list::batch_add_deleted(org_id, created_at, &del_items).await {
            log::error!("retrying: {e}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;  // 重试时 batch_process 已成功，但会重新执行
        }
    }
    success = true;
    break;
}
```

**幂等性分析**：

| 步骤 | 幂等性 | 依据 |
|------|--------|------|
| `batch_add_history` | ✅ 幂等 | INSERT 有 `ON CONFLICT DO NOTHING`（`postgres.rs:1845`） |
| `batch_process` - INSERT | ✅ 幂等 | INSERT 有 `ON CONFLICT DO NOTHING`（`postgres.rs:1845`、`sqlite.rs:1570`） |
| `batch_process` - DELETE | ✅ 幂等 | `DELETE WHERE id IN(...)` 对不存在的 id 无影响 |
| `batch_add_deleted` | ❌ 非幂等 | 无唯一约束，无 `ON CONFLICT`，重复 INSERT 产生重复记录 |

**风险**：`batch_add_deleted` 失败重试会产生重复记录（已在第 2 章分析）。

### 3.2 Merge write_file_list 重试（`merge.rs:1062-1078`）

```rust
// merge.rs:1062-1078
let mut success = false;
let mut mark_deleted_done = false;  // ✅ 有标志位
let created_at = now_micros();
for _ in 0..5 {
    if !mark_deleted_done && let Err(e) = infra::file_list::batch_process(events).await {
        continue;
    }
    mark_deleted_done = true;  // 标记 batch_process 已成功，重试时跳过
    if !del_items.is_empty() && let Err(e) = infra_file_list::batch_add_deleted(...).await {
        continue;  // 重试时只重试 batch_add_deleted，不重复 batch_process
    }
    success = true;
    break;
}
```

**对比 Retention 路径**：
- Merge 使用 `mark_deleted_done` 标志，确保 `batch_process` 只执行一次
- 即使 `batch_add_deleted` 重试，也不会重复执行 `batch_process`
- 但 `batch_add_deleted` 本身仍是非幂等的，重试仍可能产生重复记录

> **源码定位**：`merge.rs:1060` — `let mut mark_deleted_done = false;`
> **源码定位**：`merge.rs:1068` — `mark_deleted_done = true;`

### 3.3 Dump delete_by_time_range 重试（`dump.rs:476-494`）

```rust
// dump.rs:477-494
let mut success = false;
let mut mark_deleted_done = false;  // ✅ 有标志位
for _ in 0..5 {
    if !mark_deleted_done && let Err(e) = infra::file_list::batch_process(&items).await {
        continue;
    }
    mark_deleted_done = true;
    if let Err(e) = infra_file_list::batch_add_deleted(...).await {
        continue;
    }
    break;
}
```

与 Merge 相同的模式。

### 3.4 并发执行的事务隔离保障

`inner_batch_process` 在单个数据库事务内完成所有操作：

```rust
// postgres.rs:1899
let mut tx = pool.begin().await?;
// ... INSERT add_items ...
// ... SELECT id + DELETE del_items ...
tx.commit().await?;

// sqlite.rs:1532
let mut tx = client.begin().await?;
// ... INSERT add_items ...
// ... SELECT id + DELETE del_items ...
tx.commit().await?;
```

**并发安全**：
1. del_items 按 `id` 排序（`postgres.rs:1946-1949`、`sqlite.rs:1581-1584`），减少锁范围
2. 事务原子性：要么全部成功，要么全部回滚
3. 同一份文件的并发 Retention 和 Merge 操作：
   - 先成功者从 file_list 表删除记录
   - 后成功者查询 `id` 时返回 `RowNotFound` → `continue`（`postgres.rs:1982`、`sqlite.rs:1610`）
   - 不会报错，事务正常提交

### 3.5 作业级重试机制

| 作业类型 | 失败处理 | 依据 |
|---------|---------|------|
| Retention | job 不调用 `delete_stream_done` → CACHE 不清除 → 下轮 `run_retention` 重新拾取 | `retention.rs:468-477` 失败不调用 `handle_delete_by_date_done` |
| Merge | 不标记 job 为 done → `compactor_check_running_jobs` 超时后重置为 pending | `mod.rs:388` 失败不调用 `infra_file_list::update_job_status` |
| Dump | 类似 Merge，超时后重置 | `dump.rs:141` |

---

## 4. Retention 与 Merge 的互斥边界条件（校正版）

### 4.1 is_deleting_stream 机制详解

`is_deleting_stream`（`db/compact/retention.rs:107-114`）：
```rust
pub fn is_deleting_stream(
    org_id: &str, stream_type: StreamType, stream_name: &str,
    date_range: Option<(&str, &str)>,
) -> bool {
    CACHE.contains_key(&mk_key(org_id, stream_type, stream_name, date_range))
}
```

`mk_key`（同文件 :29-39）：
| date_range | 生成 key | 场景 |
|-----------|---------|------|
| `None` | `{org}/{type}/{stream}/all` | `delete_all` 整流删除 |
| `Some(("2023-01-01","2023-01-02"))` | `{org}/{type}/{stream}/2023-01-01,2023-01-02` | 按天 retention 删除 |

### 4.2 关键边界：所有调用点都传 `None`

| 调用位置 | 调用表达式 | 匹配 key |
|---------|-----------|---------|
| `mod.rs:142` run_generate_job | `is_deleting_stream(&org_id, stream_type, &stream_name, None)` | `.../all` |
| `mod.rs:312` run_merge | `is_deleting_stream(&org_id, stream_type, &stream_name, None)` | `.../all` |
| `dump.rs:107` | `is_deleting_stream(&org_id, stream_type, &stream_name, None)` | `.../all` |
| `stream.rs:371` 流设置更新 | `is_deleting_stream(org_id, stream_type, stream_name, None)` | `.../all` |
| `flight.rs:132` 查询入口 | `is_deleting_stream(&org_id, stream_type, &stream_name, None)` | `.../all` |
| `ingestion/mod.rs:472` 写入 | `is_deleting_stream(org_id, stream_type, stream_name, None)` | `.../all` |

**结论**：**日常按天 Retention（date_range = Some）不会触发任何互斥保护**。只有整流删除（delete_all）才会阻断 Merge、写入、查询。

### 4.3 保留期偏移量检查（Merge 的第二道防线）

`run_merge`（`mod.rs:302-309`）：
```rust
let stream_data_retention_end = if stream_settings.data_retention > 0 {
    now - Duration::try_days(stream_settings.data_retention).unwrap()
} else {
    data_lifecycle_end  // 全局保留截止时间
};
if job.offsets <= stream_data_retention_end.timestamp_micros() {
    need_done_ids.push(job.id);  // 数据在保留期外，直接标记 done
    continue;
}
```

**边界漏洞**：此检查只判断 Merge 作业的 **offset** 是否在保留期外，不判断作业内文件的 **时间范围**。如果 Merge 作业跨越保留期边界（部分文件在期外、部分在期内），期外文件仍会被处理。

### 4.4 互斥矩阵（校正版）

| 场景 | is_deleting_stream 块？ | offset 检查块？ | 实际行为 |
|------|------------------------|-----------------|---------|
| delete_all + Merge | ✅ 块（key = `.../all`） | N/A | Merge 生成和执行均被跳过 |
| delete_all + 写入/查询 | ✅ 块（key = `.../all`） | N/A | 阻断 |
| 按天 Retention + Merge（期外 offset） | ❌ 不块（key 不匹配） | ✅ 块（offset ≤ 保留截止） | Merge 作业标记为 done |
| 按天 Retention + Merge（期内 offset） | ❌ 不块 | ❌ 不块 | **并发执行** |
| 按天 Retention + 写入/查询 | ❌ 不块 | N/A | 正常写入/查询 |

### 4.5 并发执行的安全性分析

按天 Retention 与保留期内 Merge 并发执行是**安全的**，证据链：

1. **操作范围不重叠**：Retention 按天删除，Merge 按小时合并。同一天内 Retention 删除全天文件，Merge 合并某小时文件，不会操作相同时间范围的文件。

2. **事务原子性**：`inner_batch_process` 在事务内完成（`postgres.rs:1899`、`sqlite.rs:1532`）。如果并发操作相同文件：
   - 先成功者 DELETE 记录
   - 后成功者 SELECT id 时收到 `RowNotFound` → `continue`（`postgres.rs:1982`、`sqlite.rs:1610`）
   - 事务仍成功提交，无报错

3. **Merge 新文件逃脱 Retention 的窗口**：
   - Retention 在 `delete_from_file_list` 中 `file_list::query()` 查询待删文件列表（`retention.rs:535`）
   - Merge 在 `write_file_list` 中 `batch_process()` 写入新文件（`merge.rs:1063`）
   - 如果 Merge 写入发生在 Retention 查询之后、`batch_process` 之前，新文件不在待删列表中，会逃脱当轮 Retention
   - **但**：下一轮 Retention 会重新查询并删除这些文件（`retention.rs:318` 的 `while start < time_range.end` 确保每天只处理一次，但每天会执行一次）
   - **最大延迟**：24 小时（下一轮 daily retention 执行时）

---

## 5. Hasher 键节点分配差异（精确源码定位）

### 5.1 一致性哈希实现

`get_node_from_consistent_hash`（`infra/src/cluster/mod.rs:99-126`）：
```rust
pub async fn get_node_from_consistent_hash(
    key: &str, role: &Role, group: Option<RoleGroup>,
) -> Option<String> {
    let hash = config::utils::hash::gxhash::new().sum64(key);
    let nodes = match role {
        Role::Compactor => COMPACTOR_CONSISTENT_HASH.read().await,
        Role::FlattenCompactor => FLATTEN_COMPACTOR_CONSISTENT_HASH.read().await,
        _ => return None,
    };
    let mut iter = nodes.lower_bound(Bound::Included(&hash));
    if let Some((_, name)) = iter.next() { return Some(name.clone()); }
    if let Some((_, name)) = nodes.first_key_value() { return Some(name.clone()); }
    None
}
```

> **源码定位**：`infra/src/cluster/mod.rs:100` — `gxhash::new().sum64(key)`

### 5.2 各操作路径的 Hash 键汇总

| 操作 | 阶段 | 调用位置 | Hash 键 | 角色 |
|------|------|---------|---------|------|
| Retention 生成 | `run_retention` 生成 job | `retention.rs:92-96` | `stream_name`（`stream.as_ref()`） | Compactor |
| Retention 执行 | `run_retention` 执行 job | `mod.rs:61-65` | `job`（完整 key: `{org}/{type}/{stream}/{date_range}`） | Compactor |
| Merge 生成 | `run_generate_job` | `mod.rs:146-151` | `stream_name`（`stream.as_ref()`） | Compactor |
| Merge 执行（Daily） | `run_merge` | `mod.rs:296-299` | `stream_name`（`stream.as_ref()`） | Compactor |
| Merge 执行（Hourly） | `run_merge` | N/A | 无节点检查（任何节点可拾取） | — |
| Dump 执行（Daily） | `dump::run` | `dump.rs:112-116` | `stream_name`（`stream.as_ref()`） | Compactor |
| Flatten 生成 | `run_generate_flatten_job` | `mod.rs:218-221` | `stream_name`（`stream.as_ref()`） | FlattenCompactor |
| Downsampling 生成 | `run_generate_downsampling_job` | `mod.rs:252-256` | `stream_name`（`stream.as_ref()`） | Compactor |

### 5.3 Retention 生成 ≠ 执行的设计意图

源码注释明确说明（`mod.rs:61-62`）：
```rust
// here we use job to get the compactor node, so that we can use different compactor 
// for different job of same stream
```

**负载均衡设计**：同一 stream 的不同日期 Retention job 分散到不同节点执行，避免单节点过载。

**节点分配示例**（3 节点集群）：
```
stream_name = "app_logs"
  → sum64("app_logs") = 0x7F3A → 节点 B 生成 job

Job 1: "default/logs/app_logs/2023-01-01,2023-01-02"
  → sum64(...) = 0x2B1C → 节点 A 执行

Job 2: "default/logs/app_logs/2023-01-02,2023-01-03"
  → sum64(...) = 0xE9D4 → 节点 C 执行
```

### 5.4 节点分配差异对作业冲突的影响

| 风险场景 | 现有防护 | 残留风险 |
|---------|---------|---------|
| Retention 生成与执行在不同节点 | `process_stream` 节点级锁（`retention.rs:436-443`） | 无 |
| Retention 与 Merge 在不同节点并发 | 事务原子性 + RowNotFound 容错 | Merge 新文件可能逃脱当轮 Retention（24h 内下轮覆盖） |
| 两个节点执行同一 Retention job | `process_stream` 检查节点 uuid（`retention.rs:436-443`） | 无 |
| 节点变更 | 非归属节点主动释放 offset（`mod.rs:120-137`） | CACHE watch 传播延迟（最坏多等一轮调度） |

### 5.5 Daily partition 的内存级互斥

`db/compact/stream.rs:20-32`：
```rust
static COMPACT_STREAM_RUNNING: Lazy<RwHashSet<String>> = Lazy::new(RwHashSet::default);

pub fn is_running(key: &str) -> bool {
    COMPACT_STREAM_RUNNING.read().contains(key)
}
```

**仅对 Daily partition 生效**（`mod.rs:295-298`），且是**进程内 HashSet**，跨节点不生效。但 Daily partition 的 Merge 生成和执行使用相同 hash key（`stream_name`），保证同一节点处理，所以此互斥有效。

---

## 6. 查询一致性保障（校正版）

### 6.1 四层一致性保障

| 层级 | 机制 | 源码定位 | 说明 |
|------|------|---------|------|
| L1 | 物理删除隔离 | `postgres.rs:1998`、`sqlite.rs:1623` | `batch_process` 从 file_list 表物理 DELETE 记录，不是逻辑标记 |
| L2 | 事务原子性 | `postgres.rs:1899`、`sqlite.rs:1532` | 所有增删操作在单个事务内完成，不会出现部分写入 |
| L3 | 延迟物理删除 | `deleted.rs:26-51` | `delete_files_delay_hours`（默认 2h）后才删除存储文件，在途查询有足够时间完成 |
| L4 | 整流删除阻断 | `flight.rs:132`、`stream.rs:371` | `delete_all` 时通过 `is_deleting_stream(_, None)` 阻断写入和查询 |

### 6.2 查询一致性证据链

1. **查询路径不读取 deleted 字段**：
   `query`（`postgres.rs:454-457`）：
   ```sql
   SELECT id, account, stream, date, file, min_ts, max_ts, records, 
          original_size, compressed_size, index_size, flattened
   FROM file_list
   WHERE stream = $1 AND max_ts >= $2 AND max_ts <= $3 AND min_ts <= $4 
         AND date >= $5 AND date < $6;
   ```
   SELECT 列表中**无 `deleted` 列**，WHERE 子句中也**无 `AND deleted = false`**。因为记录已被物理 DELETE，不存在了。

2. **延迟删除窗口**：
   `run_delay_deletion`（`mod.rs:400-416`）只处理 `created_at < now - delete_files_delay_hours` 的记录。逻辑删除完成后，存储文件至少保留 2 小时，在途查询不会遇到 "文件不存在" 错误。

3. **并发查询与删除的一致性**：
   - T1: 查询开始 → `file_list::query()` → 获得文件列表
   - T2: Retention 执行 → `batch_process` → 物理 DELETE 记录 + 写入 `file_list_deleted`
   - T3: 查询继续 → 读取存储上的文件（此时文件仍存在，未到延迟删除时间）
   - T4: 查询结束 → 正常返回结果
   - T5: 2 小时后 → `run_delay_deletion` → 物理删除存储文件

   只要查询在 2 小时内完成，不会有一致性问题。

### 6.3 唯一的一致性风险场景

**极端时序**：
1. T1: 长时查询开始 → 读取文件列表
2. T2: 2 小时后 → `run_delay_deletion` 物理删除存储文件
3. T3: 查询仍在进行 → 尝试读取已删除的文件 → 报错

**缓解**：长时查询罕见，且 `delete_files_delay_hours` 可配置（默认 2h）。

---

## 7. 完整源码定位索引表

| 机制 | 源码文件 | 行号 |
|------|---------|------|
| FileKey 结构体定义 | `config/src/meta/stream.rs` | L276-282 |
| FileRecord.deleted 字段 | `infra/src/file_list/mod.rs` | L661-662 |
| Retention write_file_list | `service/compact/retention.rs` | L571-637 |
| Retention delete_from_file_list | `service/compact/retention.rs` | L522-568 |
| Retention file.deleted = true 标记 | `service/compact/retention.rs` | L557 |
| Merge write_file_list | `service/compact/merge.rs` | L1035-1106 |
| Merge mark_deleted_done 标志 | `service/compact/merge.rs` | L1060, L1068 |
| Dump delete_by_time_range | `service/compact/dump.rs` | L476-494 |
| postgres inner_batch_process | `infra/src/file_list/postgres.rs` | L1878-2022 |
| postgres INSERT ON CONFLICT | `infra/src/file_list/postgres.rs` | L1845 |
| postgres DELETE FROM file_list | `infra/src/file_list/postgres.rs` | L1998 |
| postgres batch_add_deleted | `infra/src/file_list/postgres.rs` | L223-267 |
| postgres batch_remove_deleted | `infra/src/file_list/postgres.rs` | L269-329 |
| postgres batch_add_history | `infra/src/file_list/postgres.rs` | L132-134 |
| postgres query_deleted | `infra/src/file_list/postgres.rs` | L638-663 |
| postgres file_list_deleted schema | `infra/src/file_list/postgres.rs` | L2778-2786, L3310-3320 |
| postgres file_list schema | `infra/src/file_list/postgres.rs` | L3262-3278 |
| sqlite inner_batch_process | `infra/src/file_list/sqlite.rs` | L1525-1645 |
| sqlite INSERT ON CONFLICT | `infra/src/file_list/sqlite.rs` | L1570 |
| sqlite DELETE FROM file_list | `infra/src/file_list/sqlite.rs` | L1623 |
| sqlite batch_add_deleted | `infra/src/file_list/sqlite.rs` | L172-214 |
| sqlite file_list_deleted schema | `infra/src/file_list/sqlite.rs` | L1686-1704 |
| sqlite file_list schema | `infra/src/file_list/sqlite.rs` | L1653-1670 |
| is_deleting_stream 定义 | `service/db/compact/retention.rs` | L107-114 |
| mk_key 定义 | `service/db/compact/retention.rs` | L29-39 |
| is_deleting_stream 调用点 | `service/compact/mod.rs` | L142, L312 |
| is_deleting_stream 调用点 | `service/compact/dump.rs` | L107 |
| is_deleting_stream 调用点 | `service/db/compact/stream.rs` | L371 |
| is_deleting_stream 调用点 | `service/flight.rs` | L132 |
| is_deleting_stream 调用点 | `service/ingestion/mod.rs` | L472 |
| Retention job 生成 hash key | `service/compact/retention.rs` | L92-96 |
| Retention job 执行 hash key | `service/compact/mod.rs` | L61-65 |
| Merge job 生成 hash key | `service/compact/mod.rs` | L146-151 |
| Merge Daily 执行 hash key | `service/compact/mod.rs` | L296-299 |
| get_node_from_consistent_hash | `infra/src/cluster/mod.rs` | L99-126 |
| 延迟删除 deleted::delete | `service/compact/deleted.rs` | L25-139 |
| run_delay_deletion | `service/compact/mod.rs` | L399-446 |
| storage del "not found" 容错 | `service/compact/deleted.rs` | L47-51 |
| Daily partition 内存互斥 | `service/db/compact/stream.rs` | L20-32 |
| 非归属节点释放 offset | `service/compact/mod.rs` | L120-137 |
| FileListBookKeepMode 定义 | `config/src/meta/stream.rs` | L1325-1330 |
| query 不包含 deleted 列 | `infra/src/file_list/postgres.rs` | L454-457 |

---

## 8. 关键配置速查

| 环境变量 | 默认值 | 源码定位 |
|----------|--------|---------|
| `ZO_COMPACT_FILE_LIST_DELETED_MODE` | `"deleted"` | `config/src/config.rs:1916` |
| `ZO_COMPACT_DELETE_FILES_DELAY_HOURS` | `2` | `config/src/config.rs:1908` |
| `ZO_COMPACT_FILE_LIST_DELETED_BATCH_SIZE` | `1000` | `config/src/config.rs:1920` |
| `ZO_COMPACT_DATA_RETENTION_DAYS` | `3650` | `config/src/config.rs:1904` |
| `ZO_COMPACT_EXTENDED_DATA_RETENTION_DAYS` | `3650` | `config/src/config.rs:1900` |
| `ZO_COMPACT_JOB_RUN_TIMEOUT` | `600` | `config/src/config.rs:1936` |

---

## 9. 校正总结（v1.0 → v2.0）

| 校正项 | v1.0 错误 | v2.0 正确 |
|--------|-----------|-----------|
| batch_process 删除方式 | UPDATE deleted=true 逻辑标记 | DELETE FROM file_list 物理删除 |
| file_list_deleted 唯一约束 | 隐含假设存在 | 无唯一约束，重试可能产生重复记录 |
| batch_add_deleted 幂等性 | 隐含假设幂等 | 非幂等，无 ON CONFLICT |
| Retention retry 标志位 | 未区分 Retention/Merge | Merge 有 mark_deleted_done，Retention 无 |
| 查询一致性过滤 | 需要 WHERE deleted=false | 无需过滤，记录已被物理 DELETE |
| batch_add_history 实现 | 描述模糊 | 调用 inner_batch_process("file_list_history", files)，需先将 deleted 设为 false |
