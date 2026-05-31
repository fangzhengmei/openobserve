# OpenObserve 数据保留与后台清理协同机制——源码深度分析（v3.0 最终校正版）

> **文档版本**：v3.0（基于源码逐行核对，纠正查询粒度、路径定位、并发控制等关键错误）
> **核对范围**：`src/service/compact/`、`src/service/file_list.rs`、`src/infra/src/file_list/`、`src/config/src/utils/parquet.rs`
> **关键校正点**：查询粒度参数、函数调用路径、Postgres 咨询锁机制、重复记录处理流程

---

## 1. 查询粒度与路径定位校正（v2.0 → v3.0）

### 1.1 查询函数的真实调用链

#### 1.1.1 错误描述 vs 真实实现

| 项目 | v2.0 错误描述 | v3.0 校正（正确） |
|------|-------------|-----------------|
| 查询调用函数 | `infra_file_list::query(..., time_range, None)` | `service::file_list::query(&fake_trace_id, org_id, stream_type, stream_name, PartitionTimeLevel::Unset, time_range.0, time_range.1)` |
| 时间粒度参数 | `PartitionTimeLevel::Hour` | `PartitionTimeLevel::Unset` |
| 时间范围参数 | `(time_min, time_max)` 元组 + `flattened: Option<bool>` | 分开的 `time_min` 和 `time_max`，无 `flattened` 参数 |
| 额外行为 | 仅查询 file_list 表 | 查询 file_list 表 + file_list_dump 表，合并后去重 |

> **源码定位**：`src/service/compact/retention.rs:535-543`
> ```rust
> let files = file_list::query(
>     &fake_trace_id,
>     org_id,
>     stream_type,
>     stream_name,
>     PartitionTimeLevel::Unset,  // ← 不是 Hour
>     time_range.0,  // ← 分开的参数
>     time_range.1,
> )
> .await?;
> ```

#### 1.1.2 `service::file_list::query` 的完整实现

`src/service/file_list.rs:40-72`：
```rust
pub async fn query(
    trace_id: &str,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    time_level: PartitionTimeLevel,
    time_min: i64,
    time_max: i64,
) -> Result<Vec<FileKey>> {
    // ① 查询 file_list 主表
    let mut files = infra_file_list::query(
        org_id, stream_type, stream_name, time_level,
        (time_min, time_max), None,
    ).await?;
    
    // ② 查询 file_list_dump 归档表
    let dumped_files = file_list_dump::query(
        trace_id, org_id, stream_type, stream_name,
        (time_min, time_max), &[],
    ).await?;
    
    // ③ 合并结果并按键去重
    files.extend(dumped_files.iter().map(|f| f.into()));
    files.par_sort_unstable_by(|a, b| a.key.cmp(&b.key));
    files.dedup_by(|a, b| a.key == b.key);
    
    Ok(files)
}
```

> **源码定位**：`src/service/file_list.rs:40-72`

#### 1.1.3 `infra_file_list::query` 的真实签名

`src/infra/src/file_list/mod.rs:295-314`：
```rust
pub async fn query(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    time_level: PartitionTimeLevel,
    time_range: (i64, i64),
    flattened: Option<bool>,
) -> Result<Vec<FileKey>> {
    validate_time_range(time_range)?;
    CLIENT.query(org_id, stream_type, stream_name, time_level, time_range, flattened).await
}
```

**实际查询 SQL**（Postgres）`src/infra/src/file_list/postgres.rs:454-457`：
```sql
SELECT id, account, stream, date, file, min_ts, max_ts, records, 
       original_size, compressed_size, index_size, flattened
FROM file_list
WHERE stream = $1 AND max_ts >= $2 AND max_ts <= $3 AND min_ts <= $4 
      AND date >= $5 AND date < $6;
```

**关键观察**：SELECT 列表中**没有 `deleted` 列**，WHERE 子句中也**没有 `AND deleted = false`**。这进一步证实了删除是**物理 DELETE**，不是逻辑标记。

---

### 1.2 文件路径解析的真实格式

#### 1.2.2 FileKey.key 的组成

`src/infra/src/file_list/mod.rs:676-686`：
```rust
impl From<&FileRecord> for FileKey {
    fn from(r: &FileRecord) -> Self {
        Self {
            id: r.id,
            account: r.account.to_string(),
            key: "files/".to_string() + &r.stream + "/" + &r.date + "/" + &r.file,
            //         ^^^^^^   ^^^^^^^^^^^^^   ^^^^^^^^^^^^^^   ^^^^^^^^^^^^^
            //         prefix    org/type/stream   YYYY/MM/DD/HH   filename.parquet
            meta: r.into(),
            deleted: r.deleted,
            segment_ids: None,
        }
    }
}
```

**完整路径格式**（示例）：
```
files/default/logs/olympics/2022/10/03/10/6982652937134804993_1.parquet
└─┬──┘ └──┬──┘ └┬─┘ └───┬────┘ └───────────┬──────────────┘ └───────┬───────┘
  │       │     │        │                      │                        │
  0       1     2        3                    4-7                      8
```

#### 1.2.3 `parse_file_key_columns` 的真实实现

**位置**：`src/config/src/utils/parquet.rs:125-139`（**不是** `file_list/mod.rs`）

```rust
pub fn parse_file_key_columns(key: &str) -> Result<(String, String, String), anyhow::Error> {
    // eg: files/default/logs/olympics/2022/10/03/10/6982652937134804993_1.parquet
    let columns = key.splitn(9, '/').collect::<Vec<&str>>();
    if columns.len() < 9 {
        return Err(anyhow::anyhow!("[file_list] Invalid file path: {key}"));
    }
    let stream_key = format!("{}/{}/{}", columns[1], columns[2], columns[3]);
    let date_key = format!("{}/{}/{}/{}", columns[4], columns[5], columns[6], columns[7]);
    let file_name = columns[8].to_string();
    Ok((stream_key, date_key, file_name))
}
```

#### 1.2.4 Retention 中的路径分组（校正版）

`src/service/compact/retention.rs:551-558`：
```rust
for mut file in files {
    let columns: Vec<_> = file.key.split('/').collect();
    let hour_key = format!(
        "{}/{}/{}/{}",
        columns[4], columns[5], columns[6], columns[7]  // ← 实际是 年/月/日/时
    );
    let entry = hours_files.entry(hour_key).or_default();
    file.deleted = true;  // ← 内存标记，区分 add_items/del_items
    entry.push(file);
}
```

**校正**：之前描述 hour_key 是 `{columns[4]}/{columns[5]}/{columns[6]}/{columns[7]}` 是正确的，但需要明确这对应 `年/月/日/时`。

---

## 2. file_list_deleted 重复记录的完整处理流程

### 2.1 重复记录的产生机制（证据链完整）

| 步骤 | 代码位置 | 行为 | 风险 |
|------|---------|------|------|
| 1 | `retention.rs:580-631` | Retention 重试循环，无 `mark_deleted_done` | 每次重试都重新执行 batch_process + batch_add_deleted |
| 2 | `retention.rs:599-604` | `batch_process` 执行 DELETE（幂等：已删的查不到 id，跳过） | 无副作用 |
| 3 | `retention.rs:621-627` | `batch_add_deleted` 执行 INSERT（非幂等：无唯一约束） | **产生重复记录** |

**关键证据** `src/service/compact/retention.rs:580-631`：
```rust
for _ in 0..5 {
    // History 模式处理（略）
    
    // 无论什么模式，都执行 batch_process
    if let Err(e) = infra_file_list::batch_process(&events).await {
        tokio::time::sleep(Duration::from_secs(1)).await;
        continue;  // 重试
    }
    
    // Deleted 模式：写入 file_list_deleted
    if mode == Deleted {
        if let Err(e) = infra_file_list::batch_add_deleted(org_id, created_at, &del_items).await {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;  // 重试：batch_process 已成功，但 batch_add_deleted 重跑
        }
    }
    success = true;
    break;
}
```

**产生重复的精确时序**：
```
循环 1: batch_process 成功 → batch_add_deleted 失败（网络波动）
循环 2: batch_process 幂等成功 → batch_add_deleted 再次 INSERT 相同文件
结果: file_list_deleted 表中出现两条 (stream, date, file) 相同但 id 不同的记录
```

---

### 2.2 Postgres 的并发控制机制（之前遗漏的关键实现）

#### 2.2.1 咨询锁（Advisory Lock）

`src/infra/src/file_list/postgres.rs:777-803`：
```rust
async fn query_deleted(&self, org_id: &str, time_max: i64, limit: i64) -> Result<Vec<FileListDeleted>> {
    let pool = CLIENT.clone();
    let mut tx = pool.begin().await?;
    
    // 使用咨询锁，确保同一时间只有一个事务能查询
    let lock_key = "file_list_deleted:query_deleted";
    let lock_id = config::utils::hash::gxhash::new().sum64(lock_key);
    let lock_sql = format!("SELECT pg_advisory_xact_lock({lock_id})");
    if let Err(e) = sqlx::query(&lock_sql).execute(&mut *tx).await {
        tx.rollback().await?;
        return Err(e.into());
    }
    
    // ... 查询和更新逻辑 ...
}
```

**关键**：`pg_advisory_xact_lock` 是**事务级**咨询锁，事务结束自动释放。锁 key 是固定字符串的 hash，**全局互斥**（所有 org 共用同一把锁）。

#### 2.2.2 "认领"机制：UPDATE created_at

`src/infra/src/file_list/postgres.rs:836-871`：
```rust
// 查询记录
let items: Vec<FileListDeleted> = sqlx::query_as::<_, super::FileDeletedRecord>(
    "SELECT id, account, stream, date, file, index_file, flattened 
     FROM file_list_deleted 
     WHERE org = $1 AND created_at < $2 
     ORDER BY created_at ASC LIMIT $3;"
)
.fetch_all(&mut *tx).await?;

// 关键：将这些记录的 created_at 更新为 NOW，避免被其他节点再次查询到
let ids = items.iter().map(|r| r.id.to_string()).collect::<Vec<_>>();
let sql = format!(
    "UPDATE file_list_deleted SET created_at = $1 WHERE id IN ({});",
    ids.join(",")
);
let now = now_micros();
let ret = sqlx::query(&sql).bind(now).execute(&mut *tx).await?;

if ret.rows_affected() != ids.len() as u64 {
    tx.rollback().await?;  // 更新行数不匹配，回滚
    return Ok(Vec::new());
}

tx.commit().await?;  // 提交事务，释放咨询锁
Ok(items)
```

**"认领"机制的作用**：
1. 查询到 N 条待删除记录
2. 立即将它们的 `created_at` 更新为当前时间（+ `delete_files_delay_hours`）
3. 这样下一轮查询不会再返回这些记录（因为 `created_at < time_max` 不再满足）
4. 如果物理删除失败，这些记录会在 `delete_files_delay_hours` 小时后重新可见

---

### 2.3 SQLite 的差异（无并发控制）

`src/infra/src/file_list/sqlite.rs:635-663`：
```rust
async fn query_deleted(&self, org_id: &str, time_max: i64, limit: i64) -> Result<Vec<FileListDeleted>> {
    // 无咨询锁
    // 无 UPDATE created_at 认领
    // 直接 SELECT 返回结果
    
    let ret = sqlx::query_as::<_, super::FileDeletedRecord>(
        "SELECT id, account, stream, date, file, index_file, flattened 
         FROM file_list_deleted 
         WHERE org = $1 AND created_at < $2 
         ORDER BY created_at ASC LIMIT $3;"
    )
    .fetch_all(&pool).await?;
    
    Ok(ret.iter().map(|r| FileListDeleted {
        id: r.id,
        account: r.account.to_string(),
        file: format!("files/{}/{}/{}", r.stream, r.date, r.file),
        index_file: r.index_file,
        flattened: r.flattened,
    }).collect())
}
```

**SQLite vs Postgres 对比**：

| 特性 | Postgres | SQLite |
|------|----------|--------|
| 咨询锁 | ✅ `pg_advisory_xact_lock` | ❌ 无 |
| UPDATE created_at 认领 | ✅ 有 | ❌ 无 |
| 多节点并发安全 | ✅ 全局互斥 | ❌ 仅单节点部署安全 |
| 失败重试机制 | ✅ 更新失败自动回滚 | ❌ 依赖调用方重试 |

---

### 2.4 重复记录的完整处理链路

```
┌─────────────────────────────────────────────────────────────────────┐
│                     重复记录产生（Retention 重试）                    │
│  retention.rs:580-631 无 mark_deleted_done，重试时重复 batch_add_deleted │
│  结果：两条 (stream, date, file) 相同但 id 不同的记录                  │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     查询阶段（query_deleted）                        │
│  Postgres: pg_advisory_xact_lock + UPDATE created_at 认领              │
│  SQLite: 直接 SELECT，无锁无认领                                      │
│  结果：返回所有符合条件的记录，包括重复的                              │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     物理删除（storage::del）                         │
│  deleted.rs:33-51 对 "not found" 错误容错，重复删除无副作用             │
│  结果：文件只需要删除一次，后续重复删除静默成功                         │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     表记录删除（batch_remove_deleted）               │
│  deleted.rs:118-136 → postgres.rs:269-329                             │
│  如果 file.id > 0：直接使用 id 删除（query_deleted 返回了 id）          │
│  如果 file.id = 0：SELECT id → DELETE WHERE id IN(...)                │
│  fetch_one() 每次只返回第一个匹配的 id，N 条重复需要 N 轮删除          │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 2.5 batch_remove_deleted 的精确流程

`src/infra/src/file_list/postgres.rs:269-329`：
```rust
async fn batch_remove_deleted(&self, files: &[FileKey]) -> Result<()> {
    let chunks = files.chunks(100);
    for files in chunks {
        let mut ids = Vec::with_capacity(files.len());
        for file in files {
            // 关键：query_deleted 返回的记录已经包含 id，所以走这里
            if file.id > 0 {
                ids.push(file.id.to_string());
                continue;
            }
            // 只有 id 缺失时才查询
            let (stream_key, date_key, file_name) = parse_file_key_columns(&file.key)?;
            let ret: Option<i64> = sqlx::query_scalar(
                "SELECT id FROM file_list_deleted WHERE stream = $1 AND date = $2 AND file = $3;"
            )
            .bind(stream_key).bind(date_key).bind(file_name)
            .fetch_one(&pool).await;  // ← fetch_one() 只取第一条
            
            match ret {
                Ok(Some(v)) => ids.push(v.to_string()),
                Ok(None) => continue,
                Err(sqlx::Error::RowNotFound) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        
        if !ids.is_empty() {
            let sql = format!("DELETE FROM file_list_deleted WHERE id IN({});", ids.join(","));
            pool.execute(sql.as_str()).await?;
        }
    }
    Ok(())
}
```

**重复记录的清理循环**（Postgres 场景）：
```
假设同一份文件有 3 条重复记录：id=100, id=200, id=300

第 1 轮 run_delay_deletion:
  query_deleted → 返回 [id=100, id=200, id=300]（ORDER BY created_at ASC）
  UPDATE created_at = NOW() （3 条都更新）
  storage::del → 文件被删除
  batch_remove_deleted → DELETE WHERE id IN(100,200,300)
  结果：3 条记录一次被删除 ✅

特殊场景（部分失败）：
第 1 轮 query_deleted → 返回 [id=100, id=200]（LIMIT=2）
  UPDATE created_at = NOW()
  storage::del → 成功
  batch_remove_deleted → DELETE WHERE id IN(100,200)
  结果：id=100,200 被删，id=300 仍在表中

第 2 轮 run_delay_deletion（2 小时后）：
  query_deleted → id=300 的 created_at 已过 2 小时，返回 [id=300]
  ... 重复流程 ...
  结果：id=300 被删除

最坏情况：LIMIT 条重复记录需要 N/LIMIT + 1 轮才能全部删除
```

---

## 3. 重复记录的边界影响分析

### 3.1 存储层影响

| 影响类型 | 分析 | 风险等级 |
|---------|------|---------|
| 存储文件 | `storage::del()` 对 "not found" 错误容错，重复删除无副作用 | ✅ 无风险 |
| 倒排索引文件 | 同上，`deleted.rs:55-81` 有同样的容错逻辑 | ✅ 无风险 |
| Flattened 文件 | 同上，`deleted.rs:83-115` 有同样的容错逻辑 | ✅ 无风险 |

**证据** `src/service/compact/deleted.rs:47-51`：
```rust
if !e.to_string().to_lowercase().contains("not found") {
    log::error!("[COMPACTOR] delete files from storage failed: {e}");
    return Err(e.into());
}
// "not found" 错误被忽略，返回 Ok
```

---

### 3.2 数据库层影响

| 影响类型 | 分析 | 风险等级 |
|---------|------|---------|
| 表膨胀 | 重复记录占用额外存储空间，每条记录约 100-200 字节 | ⚠️ 低风险（需重试才产生，通常重试次数少） |
| 查询性能 | `query_deleted` 扫描更多记录，但有 LIMIT 约束（默认 10000） | ⚠️ 低风险（LIMIT 控制单次扫描量） |
| 删除性能 | N 条重复记录需要 N/LIMIT + 1 轮 `run_delay_deletion` | ⚠️ 中风险（延迟清理时间，但不阻塞流程） |

**证据** `src/service/compact/deleted.rs:23`：
```rust
const BATCH_SIZE: i64 = 10000;
```

`src/infra/src/file_list/postgres.rs:808-814`：
```sql
SELECT ... FROM file_list_deleted 
WHERE org = $1 AND created_at < $2 
ORDER BY created_at ASC LIMIT $3;  -- $3 = BATCH_SIZE = 10000
```

---

### 3.3 一致性影响

| 影响类型 | 分析 | 风险等级 |
|---------|------|---------|
| 查询一致性 | 逻辑删除（从 file_list 表 DELETE）已完成，物理删除延迟不影响查询结果 | ✅ 无风险 |
| 数据完整性 | 文件最终会被删除（多轮循环），不会永久残留 | ✅ 无风险 |
| 并发安全 | Postgres 有咨询锁全局互斥，同一时间只有一个节点处理 | ✅ 无风险（Postgres）；⚠️ SQLite 仅单节点安全 |

---

### 3.4 可观测性影响

| 指标 | 重复记录的影响 |
|------|--------------|
| `zo_storage_original_bytes` | 不受影响（逻辑删除后文件不计入统计） |
| `zo_storage_files` | 不受影响（逻辑删除后文件不计入统计） |
| `zo_compact_used_time` | 可能增加（多轮循环执行） |
| 日志 | `deleted.rs:425` 打印 "deleted from file_list_deleted {affected} files"，重复记录会导致计数偏大 |

---

## 4. 可核实的证据清单

| 编号 | 结论 | 源码文件 | 行号 | 核实方法 |
|------|------|---------|------|---------|
| **E1** | Retention 查询使用 `PartitionTimeLevel::Unset`，不是 `Hour` | `service/compact/retention.rs` | L540 | 搜索 `PartitionTimeLevel::Unset` |
| **E2** | Retention 调用的是 `service::file_list::query`，不是 `infra_file_list::query` | `service/compact/retention.rs` | L535-543 | 检查 import 段 + 函数签名 |
| **E3** | `service::file_list::query` 合并 file_list 和 file_list_dump 结果并去重 | `service/file_list.rs` | L40-72 | 阅读完整函数 |
| **E4** | `batch_process` 执行物理 DELETE，不是 UPDATE deleted 标记 | `infra/file_list/postgres.rs` | L1967-1998 | 阅读 `inner_batch_process` 的 DELETE 分支 |
| **E5** | 查询 SQL 不包含 `deleted` 列，也不过滤 `deleted=false` | `infra/file_list/postgres.rs` | L454-457 | 检查 SELECT 列表和 WHERE 子句 |
| **E6** | `parse_file_key_columns` 在 `parquet.rs`，不是 `file_list/mod.rs` | `config/src/utils/parquet.rs` | L125-139 | 搜索函数定义 |
| **E7** | FileKey.key 格式为 `files/ + stream + / + date + / + file` | `infra/file_list/mod.rs` | L681 | 检查 `From<&FileRecord> for FileKey` |
| **E8** | `file_list_deleted` 表无 `(stream, date, file)` 唯一约束 | `infra/file_list/postgres.rs` | L2778-2786 | 检查 CREATE TABLE 语句 |
| **E9** | `batch_add_deleted` 无 `ON CONFLICT`，直接 INSERT | `infra/file_list/postgres.rs` | L237-239 | 检查 INSERT 语句 |
| **E10** | Postgres `query_deleted` 使用 `pg_advisory_xact_lock` 全局互斥 | `infra/file_list/postgres.rs` | L789-803 | 搜索 `pg_advisory_xact_lock` |
| **E11** | Postgres `query_deleted` 用 UPDATE created_at "认领" 记录 | `infra/file_list/postgres.rs` | L836-871 | 阅读 UPDATE 逻辑 |
| **E12** | SQLite `query_deleted` 无锁无认领 | `infra/file_list/sqlite.rs` | L635-663 | 对比 Postgres 实现 |
| **E13** | Retention `write_file_list` 无 `mark_deleted_done` | `service/compact/retention.rs` | L580-631 | 检查循环内变量 |
| **E14** | Merge `write_file_list` 有 `mark_deleted_done` | `service/compact/merge.rs` | L1060, L1068 | 对比 Retention 实现 |
| **E15** | `batch_remove_deleted` 用 `fetch_one()` 每次只取第一条 | `infra/file_list/postgres.rs` | L296 | 检查 `fetch_one` 调用 |
| **E16** | 延迟删除 BATCH_SIZE = 10000 | `service/compact/deleted.rs` | L23 | 搜索 `BATCH_SIZE` |
| **E17** | 物理删除对 "not found" 错误容错 | `service/compact/deleted.rs` | L47-51 | 检查错误处理逻辑 |
| **E18** | 所有调用 `is_deleting_stream` 都传 `None` | `service/compact/mod.rs` | L142, L312 | 搜索所有调用点 |
| **E19** | Retention 执行用完整 job key 做一致性哈希 | `service/compact/mod.rs` | L61-65 | 检查 hash 参数 |
| **E20** | Retention 生成用 stream_name 做一致性哈希 | `service/compact/retention.rs` | L92-96 | 对比执行阶段的 hash 参数 |

---

## 5. 校正总结（v2.0 → v3.0）

| 校正项 | v2.0 错误 | v3.0 正确 | 证据编号 |
|--------|----------|----------|---------|
| Retention 查询粒度 | `PartitionTimeLevel::Hour` | `PartitionTimeLevel::Unset` | E1 |
| 查询函数调用路径 | `infra_file_list::query` | `service::file_list::query`（合并 dump 结果） | E2, E3 |
| 删除方式 | 物理 DELETE（正确） | 物理 DELETE（保留，补充 SQL 证据） | E4, E5 |
| 唯一约束 | 无唯一约束（正确） | 无唯一约束（补充表结构证据） | E8, E9 |
| Postgres 并发控制 | 未提及咨询锁和认领机制 | `pg_advisory_xact_lock` + UPDATE created_at 认领 | E10, E11 |
| SQLite 并发控制 | 未提及 | 无锁无认领，仅单节点安全 | E12 |
| parse_file_key_columns 位置 | `file_list/mod.rs` | `config/src/utils/parquet.rs` | E6 |
| FileKey.key 格式 | 描述模糊 | `files/ + stream + / + date + / + file` | E7 |
| 重复记录清理效率 | 需 N 轮 | N 条重复在单轮中用 IN 批量删除（如果都被 query_deleted 返回） | E15 |
| 重试机制对比 | 描述正确 | 保留，补充证据链 | E13, E14 |
| "not found" 容错 | 描述正确 | 保留，补充证据链 | E17 |

---

## 6. 完整源码定位索引表（更新版）

| 机制 | 源码文件 | 行号 |
|------|---------|------|
| Retention delete_from_file_list | `service/compact/retention.rs` | L522-568 |
| Retention 调用 service::file_list::query | `service/compact/retention.rs` | L535-543 |
| Retention file.deleted = true 内存标记 | `service/compact/retention.rs` | L557 |
| Retention 路径分组（年/月/日/时） | `service/compact/retention.rs` | L551-558 |
| Retention write_file_list 重试 | `service/compact/retention.rs` | L571-637 |
| service::file_list::query 合并 dump | `service/file_list.rs` | L40-72 |
| parse_file_key_columns 函数 | `config/src/utils/parquet.rs` | L125-139 |
| FileKey From<FileRecord> 实现 | `infra/file_list/mod.rs` | L676-686 |
| Merge write_file_list（有 mark_deleted_done） | `service/compact/merge.rs` | L1035-1106 |
| Merge mark_deleted_done 标志 | `service/compact/merge.rs` | L1060, L1068 |
| Dump delete_by_time_range | `service/compact/dump.rs` | L476-494 |
| postgres inner_batch_process | `infra/file_list/postgres.rs` | L1878-2022 |
| postgres DELETE FROM file_list | `infra/file_list/postgres.rs` | L1998 |
| postgres batch_add_deleted | `infra/file_list/postgres.rs` | L223-267 |
| postgres batch_remove_deleted | `infra/file_list/postgres.rs` | L269-329 |
| postgres query_deleted（咨询锁 + 认领） | `infra/file_list/postgres.rs` | L777-877 |
| postgres pg_advisory_xact_lock | `infra/file_list/postgres.rs` | L789-803 |
| postgres UPDATE created_at 认领 | `infra/file_list/postgres.rs` | L836-871 |
| postgres file_list_deleted schema | `infra/file_list/postgres.rs` | L2778-2786, L3310-3320 |
| postgres file_list schema | `infra/file_list/postgres.rs` | L3262-3278 |
| postgres query SQL 无 deleted 列 | `infra/file_list/postgres.rs` | L454-457 |
| sqlite inner_batch_process | `infra/file_list/sqlite.rs` | L1525-1645 |
| sqlite batch_add_deleted | `infra/file_list/sqlite.rs` | L172-214 |
| sqlite query_deleted（无锁无认领） | `infra/file_list/sqlite.rs` | L635-663 |
| sqlite file_list_deleted schema | `infra/file_list/sqlite.rs` | L1686-1704 |
| is_deleting_stream 定义 | `service/db/compact/retention.rs` | L107-114 |
| is_deleting_stream 调用点（都传 None） | `service/compact/mod.rs` | L142, L312 |
| Retention job 生成 hash key | `service/compact/retention.rs` | L92-96 |
| Retention job 执行 hash key | `service/compact/mod.rs` | L61-65 |
| 延迟删除 deleted::delete | `service/compact/deleted.rs` | L25-139 |
| 延迟删除 BATCH_SIZE | `service/compact/deleted.rs` | L23 |
| 物理删除 "not found" 容错 | `service/compact/deleted.rs` | L47-51 |
| run_delay_deletion 调度 | `service/compact/mod.rs` | L399-446 |
| FileListBookKeepMode 定义 | `config/src/meta/stream.rs` | L1325-1330 |
| FileListBookKeepMode 配置 | `config/src/config.rs` | L1916 |
| delete_files_delay_hours 配置 | `config/src/config.rs` | L1908 |
| file_list_deleted_batch_size 配置 | `config/src/config.rs` | L1918-1923 |

---

## 7. 关键配置速查（更新版）

| 环境变量 | 默认值 | 影响 | 源码定位 |
|----------|--------|------|---------|
| `ZO_COMPACT_FILE_LIST_DELETED_MODE` | `"deleted"` | 控制删除簿记模式 | `config/src/config.rs:1916` |
| `ZO_COMPACT_DELETE_FILES_DELAY_HOURS` | `2` | 物理删除延迟小时数 | `config/src/config.rs:1908` |
| `ZO_COMPACT_FILE_LIST_DELETED_BATCH_SIZE` | `1000` | 删除操作批大小（inner_batch_process 用） | `config/src/config.rs:1918-1923` |
| `ZO_COMPACT_DATA_RETENTION_DAYS` | `3650` | 全局保留天数 | `config/src/config.rs:1904` |
| `ZO_COMPACT_JOB_RUN_TIMEOUT` | `600` | 作业超时秒数 | `config/src/config.rs:1936` |

> **注意**：`deleted.rs:23` 中的 `BATCH_SIZE = 10000` 是 `query_deleted` 的 LIMIT 参数，与 `ZO_COMPACT_FILE_LIST_DELETED_BATCH_SIZE = 1000`（`inner_batch_process` 的删除批大小）是两个不同的配置。
