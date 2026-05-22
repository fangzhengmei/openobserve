# WAL 归档、压实调度与崩溃重放协作关系分析

## 1. 核心模块概览

### 1.1 模块职责划分

| 模块 | 主要职责 | 关键文件 |
|------|---------|---------|
| **WAL 核心层** | 底层 WAL 文件的读写、CRC 校验、Snappy 压缩 | `src/wal/src/` |
| **Ingester 层** | WAL 写入、MemTable 管理、Immutable 持久化 | `src/ingester/src/wal.rs`, `src/ingester/src/writer.rs`, `src/ingester/src/immutable.rs` |
| **文件上传层** | 本地 Parquet 文件合并、上传到对象存储 | `src/job/files/parquet.rs` |
| **压实调度层** | 压实任务生成、调度、执行 | `src/job/compactor.rs`, `src/service/compact/` |

---

## 2. WAL 写入与归档流程

### 2.1 数据写入路径

```
数据输入
    ↓
[Writer.write_batch] 预处理器
    ├─ 序列化 Entry 为字节（用于 WAL）
    ├─ 转换为 Arrow RecordBatch（用于 MemTable）
    ↓
[Writer.consume_processed] IO 线程
    ├─ 检查是否需要旋转 WAL/MemTable
    ├─ 写入 WAL 文件（带 CRC 校验 + Snappy 压缩）
    ├─ 写入 MemTable（内存表）
    └─ 按需 fsync
```

**关键代码位置**：
- `src/ingester/src/writer.rs:484` - `consume_processed` 函数

### 2.2 WAL 文件格式

```
+-------------------+
|  文件标识符 (13B) |  "OPENOBSERVEV3"
+-------------------+
|  头部长度 (4B)    |
+-------------------+
|  头部数据 (可空)  |
+-------------------+
|  条目 1:          |
|  CRC32 (4B)       |
|  压缩长度 (4B)    |
|  压缩数据         |
+-------------------+
|  条目 2:          |
|  ...              |
+-------------------+
```

**关键代码位置**：
- `src/wal/src/writer.rs:116` - `write` 函数
- `src/wal/src/reader.rs:157` - `_read_entry` 函数

### 2.3 WAL 旋转（Rotation）

当满足以下任一条件时触发旋转：
1. WAL 文件大小超过 `max_file_size_on_disk`
2. MemTable 大小超过 `max_file_size_in_memory`
3. 文件存在时间超过 `max_file_retention_time`

**旋转流程**：
```rust
// src/ingester/src/writer.rs:551
async fn rotate(&self, entry_bytes_size: usize, entry_batch_size: usize) -> Result<()> {
    // 1. 创建新的 WAL Writer
    let (new_wal, _header_size) = WalWriter::new(...)?;
    
    // 2. 同步旧 WAL 并替换
    wal.sync().context(WalSnafu)?;
    let old_wal = std::mem::replace(&mut *wal, new_wal);
    
    // 3. 创建新的 MemTable 并替换
    let new_mem = MemTable::new();
    let old_mem = std::mem::replace(&mut *mem, new_mem);
    
    // 4. 将旧 MemTable 加入 IMMUTABLES 队列等待持久化
    let table = Arc::new(Immutable::new(self.idx, self.key.clone(), old_mem));
    IMMUTABLES.write().await.insert(path, table);
}
```

### 2.4 Immutable 持久化（WAL 归档）

Immutable 持久化采用 **5 步原子操作** 确保崩溃安全：

```rust
// src/ingester/src/immutable.rs:92
pub(crate) async fn persist(&self, wal_path: &PathBuf) -> Result<PersistStat> {
    // 1. 写入内存表到磁盘，使用 .par 临时扩展名
    let (schema_size, paths) = self.memtable.persist(...).await?;
    
    // 2. 创建锁文件，记录所有 .par 文件名
    let done_path = wal_path.with_extension("lock");
    fs::write(&done_path, lock_data.as_bytes()).await?;
    
    // 3. 删除 WAL 文件（此时数据已安全落地）
    fs::remove_file(wal_path).await?;
    
    // 4. 将 .par 文件重命名为 .parquet
    for (path, stat) in paths {
        fs::rename(&path, &path.with_extension("parquet")).await?;
    }
    
    // 5. 删除锁文件
    fs::remove_file(&done_path).await?;
}
```

**关键设计意图**：
- 每一步都是幂等或可恢复的
- 通过 `.lock` 文件标记持久化进度
- 只有删除 WAL 文件后才认为数据已安全归档

---

## 3. 本地文件上传流程

### 3.1 文件扫描与上传

`src/job/files/parquet.rs` 定期扫描本地 `data_wal_dir/files/` 目录下的 parquet 文件：

```
扫描周期: file_push_interval (默认 10 秒)
    ↓
[scan_wal_files] 扫描 .parquet 文件
    ↓
[prepare_files] 按分区分组，跳过处理中文件
    ↓
[move_files] 合并小文件并上传
    ├─ 按大小/时间合并本地小文件
    ├─ 上传到对象存储（S3/OSS 等）
    ├─ 写入 file_list 元数据到 DB
    └─ 删除本地文件（或加入待删除队列）
```

### 3.2 上传阈值控制

文件上传需满足以下任一条件：
1. 总大小达到 `max_file_size_on_disk`（默认 256MB）
2. 文件存在时间超过 `max_file_retention_time`（默认 300 秒）
3. 字段数量超过 `file_move_fields_limit`（用于控制合并开销）

**关键代码位置**：
- `src/job/files/parquet.rs:469` - 阈值检查逻辑

---

## 4. 压实（Compaction）调度与执行

### 4.1 压实调度架构

```
                              ┌─────────────────────┐
                              │  job/compactor.rs   │
                              │  定时任务发生器      │
                              └─────────┬───────────┘
                                        │
                ┌───────────────────────┼───────────────────────┐
                ▼                       ▼                       ▼
    ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
    │ run_generate_job   │  │ run_merge          │  │ run_retention      │
    │ 生成压实任务       │  │ 执行压实任务       │  │ 数据保留删除       │
    └─────────┬──────────┘  └─────────┬──────────┘  └────────────────────┘
              │                       │
              ▼                       ▼
    ┌────────────────────┐  ┌────────────────────┐
    │ service/compact/   │  │ JobScheduler       │
    │ merge.rs           │  │ 任务分发器         │
    └─────────┬──────────┘  └─────────┬──────────┘
              │                       │
              ▼                       ▼
    ┌────────────────────┐  ┌────────────────────┐
    │ generate_job_by_   │  │ MergeWorker        │
    │ stream             │  │ 文件合并执行器     │
    └────────────────────┘  └────────────────────┘
```

### 4.2 压实任务生成

**时间窗口控制**（`src/service/compact/merge.rs:138`）：
```rust
// 必须等待至少 3 * max_file_retention_time 才能压实
// - 第1个周期：最后1小时的本地文件上传到存储，写入 file_list
// - 第2个周期：最后1小时的 file_list 上传到存储
// - 第3个周期：可以开始压实
if offset >= time_now_hour
    || time_now.timestamp_micros() - offset
        <= Duration::try_seconds(cfg.limit.max_file_retention_time as i64)
            .unwrap()
            .num_microseconds()
            .unwrap()
            * 3
{
    return Ok(()); // 时间未到，等待
}
```

### 4.3 压实偏移量管理

每个流的压实进度通过 `compact_files` 表记录：
- `offset`：最后压实到的时间点（微秒级时间戳，按小时对齐）
- `node`：负责该流压实的节点 UUID

**关键代码位置**：
- `src/service/compact/merge.rs:78` - `get_offset` 获取压实进度
- `src/service/compact/merge.rs:162` - `set_offset` 更新压实进度

### 4.4 压实执行流程

```
[run_merge] 获取待处理任务
    ↓
[merge_by_stream] 按流处理
    ├─ 按分区键分组文件
    ├─ 按合并策略排序（FileSize/FileTime/TimeRange）
    ├─ 分组：将小文件组合成批次（不超过 max_file_size）
    │
    ├─ [MergeWorker.merge_files] 实际合并
    │   ├─ 从对象存储下载文件
    │   ├─ DataFusion 执行合并
    │   ├─ 生成倒排索引（如需要）
    │   ├─ 上传合并后的大文件
    │   └─ 返回新文件元数据
    │
    └─ [write_file_list] 原子更新
        ├─ 写入新文件记录
        ├─ 标记旧文件为已删除
        └─ 事务性批量处理
```

---

## 5. 崩溃恢复机制

### 5.1 启动时恢复流程

**关键入口**：`src/ingester/src/lib.rs:93` - `init()` 函数

```rust
pub async fn init() -> errors::Result<()> {
    // 步骤1: 检查未完成的 parquet 文件
    wal::check_uncompleted_parquet_files().await?;
    
    // 步骤2: 扫描并重放 WAL 文件
    let wal_dir = PathBuf::from(&config::get_config().common.data_wal_dir).join("logs");
    let wal_files = wal::wal_scan_files(&wal_dir, "wal")
        .await
        .unwrap_or_default();
    
    // 异步重放 WAL（不阻塞启动）
    tokio::task::spawn(async move {
        if let Err(e) = wal::replay_wal_files(wal_dir, wal_files).await {
            log::error!("replay wal files error: {e}");
        }
    });
    
    // ... 启动其他后台任务
}
```

### 5.2 未完成 Parquet 文件检查

`src/ingester/src/wal.rs:50` - `check_uncompleted_parquet_files()`

根据 5 步持久化流程，处理各种中断场景：

| 中断时机 | 发现的文件 | 恢复动作 |
|---------|-----------|---------|
| 步骤1后（.par 已写，无 .lock） | .par 文件，无 .lock，无 .wal | 删除 .par 文件 |
| 步骤2后（.lock 已写） | .lock + .par + .wal | 删除 .wal，重命名 .par->.parquet，删除 .lock |
| 步骤3后（.wal 已删） | .lock + .par | 重命名 .par->.parquet，删除 .lock |
| 步骤4后（.parquet 已写） | .lock + .parquet | 删除 .lock |

**关键代码位置**：
- `src/ingester/src/wal.rs:50` - `check_uncompleted_parquet_files` 函数

### 5.3 WAL 重放机制

`src/ingester/src/wal.rs:108` - `replay_wal_files()`

```rust
pub(crate) async fn replay_wal_files(wal_dir: PathBuf, wal_files: Vec<PathBuf>) -> Result<()> {
    for wal_file in wal_files.iter() {
        // 1. 从 WAL 文件名解析 org_id, stream_type, thread_idx
        let file_str = wal_file.strip_prefix(&wal_dir)...;
        let key = WriterKey::new_replay(org_id, stream_type);
        
        // 2. 创建临时 MemTable
        let mut memtable = memtable::MemTable::new();
        
        // 3. 读取并验证 WAL 条目
        let mut reader = wal::Reader::from_path(wal_file)?;
        loop {
            let entry = match reader.read_entry() {
                Ok(entry) => entry,
                Err(wal::Error::UnableToReadData { .. }) => continue, // 跳过坏条目
                Err(wal::Error::LengthMismatch { .. }) => continue,
                Err(wal::Error::ChecksumMismatch { .. }) => continue,
                Err(e) => return Err(Error::WalError { source: e }),
            };
            let Some(entry_bytes) = entry else { break };
            
            // 4. 反序列化 Entry
            let mut entry = super::Entry::from_bytes(&entry_bytes)?;
            
            // 5. 推断 Schema 并写入 MemTable
            let infer_schema = infer_json_schema_from_values(...)?;
            let batch = entry.into_batch(key.stream_type.clone(), infer_schema.clone())?;
            memtable.write(infer_schema, entry, batch)?;
        }
        
        // 6. 直接持久化到磁盘（走正常的 5 步流程）
        let immutable = immutable::Immutable::new(idx, key, memtable);
        let stat = immutable.persist(&wal_path).await?;
    }
}
```

**重放容错设计**：
- 单个条目损坏不影响整个文件（跳过损坏条目）
- CRC 校验确保数据完整性
- 长度校验防止解析错误

---

## 6. 三者协作关系与衔接点

### 6.1 完整数据流图

```
 数据写入
    │
    ▼
┌─────────────────────────────────────────────────┐
│  WAL 写入层 (ingester/writer.rs)                │
│  ├─ 写入 WAL 文件 (带 CRC + Snappy)              │
│  └─ 写入 MemTable                               │
└───────────────────┬─────────────────────────────┘
                    │ 达到阈值触发旋转
                    ▼
┌─────────────────────────────────────────────────┐
│  Immutable 持久化层 (ingester/immutable.rs)     │
│  5 步原子操作:                                   │
│  1. 写 .par 文件                                │
│  2. 写 .lock 文件                                │
│  3. 删 .wal 文件                                 │ ◄─── 崩溃恢复边界
│  4. 重命名 .par -> .parquet                     │
│  5. 删 .lock 文件                                │
└───────────────────┬─────────────────────────────┘
                    │ 本地 parquet 文件
                    ▼
┌─────────────────────────────────────────────────┐
│  文件上传层 (job/files/parquet.rs)              │
│  ├─ 扫描本地文件                                 │
│  ├─ 合并小文件                                   │
│  ├─ 上传到对象存储                               │
│  └─ 写入 file_list 元数据                        │
└───────────────────┬─────────────────────────────┘
                    │ 对象存储中的小文件
                    ▼
┌─────────────────────────────────────────────────┐
│ 压实调度层 (job/compactor.rs, service/compact/) │
│  ├─ 按小时/天生成压实任务                        │
│  ├─ 合并对象存储中的小文件                        │
│  ├─ 原子更新 file_list (添加新文件+标记删除旧文件)│
│  └─ 维护压实偏移量 (offset)                      │
└─────────────────────────────────────────────────┘
```

### 6.2 关键衔接点分析

#### 衔接点 1: WAL 删除与持久化完成

**位置**：`src/ingester/src/immutable.rs:116`
```rust
// 步骤3: 删除 WAL 文件 - 这是关键的提交点
fs::remove_file(wal_path).await?;
```

- **之前**：WAL 文件存在，崩溃时需要重放
- **之后**：WAL 文件已删除，数据已安全在 .par 文件中
- **崩溃恢复**：通过 .lock 文件判断是否需要继续完成后续步骤

#### 衔接点 2: 本地上传与压实时间窗口

**位置**：`src/service/compact/merge.rs:138`
```rust
// 必须等待 3 * max_file_retention_time 才能压实
```

- **目的**：确保所有本地文件都已上传到对象存储
- **计算**：`3 * max_file_retention_time`
  - 周期1：本地文件上传
  - 周期2：file_list 元数据同步
  - 周期3：安全窗口

#### 衔接点 3: 压实偏移量与数据保留

**位置**：`src/service/compact/mod.rs:307`
```rust
if job.offsets <= stream_data_retention_end.timestamp_micros() {
    need_done_ids.push(job.id); // 数据将被保留策略删除，直接跳过
    continue;
}
```

- 压实前检查数据是否已超过保留期
- 避免压实即将被删除的数据，浪费资源

#### 衔接点 4: 崩溃恢复与正常启动

**位置**：`src/main.rs:286`
```rust
// ingester init 内部包含崩溃恢复
if let Err(e) = ingester::init().await {
    job_init_tx.send(false).ok();
    panic!("ingester init failed: {e}");
}
```

- 崩溃恢复是 ingester 初始化的一部分
- 先检查未完成的 parquet 文件，再重放 WAL
- 完成恢复后才启动正常的写入和压实任务

### 6.3 故障场景与恢复路径

| 故障场景 | 恢复机制 | 涉及模块 |
|---------|---------|---------|
| **WAL 写入中途崩溃** | 重启时 WAL 重放，跳过 CRC 不匹配的条目 | `ingester/wal.rs:replay_wal_files` |
| **Immutable 持久化中途崩溃** | 通过 .lock 文件检测，从断点继续 | `ingester/wal.rs:check_uncompleted_parquet_files` |
| **文件上传中途崩溃** | 重启后重新扫描本地文件，重复上传（幂等） | `job/files/parquet.rs:scan_wal_files` |
| **压实执行中途崩溃** | 任务超时后重新调度，通过 offset 避免重复处理 | `job/compactor.rs:check_running_jobs` |
| **节点宕机（压实中）** | 其他节点通过一致性哈希接管，从 offset 继续 | `service/compact/merge.rs:get_offset` |

---

## 7. 关键设计决策与权衡

### 7.1 WAL 不直接删除，等待持久化完成

**设计**：WAL 文件只在 Immutable 持久化的第3步才删除

**原因**：
- 确保 MemTable 数据已安全写入磁盘
- 允许崩溃时通过重放 WAL 恢复未持久化的数据
- 避免了 WAL 与磁盘数据之间的不一致窗口

### 7.2 5 步持久化而非 2 步提交

**设计**：采用 5 步文件操作而非数据库事务

**权衡**：
- ✅ 不依赖外部事务系统
- ✅ 每步都是幂等或可恢复的
- ✅ 崩溃恢复逻辑简单直接
- ❌ 需要多次文件系统操作
- ❌ .lock 文件可能残留（但启动时会清理）

### 7.3 压实延迟 3 个周期

**设计**：必须等待 `3 * max_file_retention_time` 才能压实

**权衡**：
- ✅ 确保所有数据都已上传到对象存储
- ✅ 避免压实正在上传的文件
- ❌ 增加了小文件存在的时间
- ❌ 短期内查询可能需要扫描更多小文件

### 7.4 WAL 重放异步执行

**设计**：WAL 重放在后台线程执行，不阻塞服务启动

**权衡**：
- ✅ 服务快速启动，快速恢复写入
- ❌ 重放完成前，这部分数据不可查询
- ❌ 如果重放过程中再次崩溃，需要重新开始

---

## 8. 代码索引速查表

| 功能 | 文件位置 | 行号 |
|------|---------|------|
| WAL 写入 | `src/ingester/src/writer.rs` | 484 |
| WAL 旋转 | `src/ingester/src/writer.rs` | 551 |
| Immutable 持久化 5 步 | `src/ingester/src/immutable.rs` | 92 |
| 未完成文件检查 | `src/ingester/src/wal.rs` | 50 |
| WAL 重放 | `src/ingester/src/wal.rs` | 108 |
| 文件上传合并 | `src/job/files/parquet.rs` | 351 |
| 压实任务生成 | `src/service/compact/merge.rs` | 72 |
| 压实时间窗口检查 | `src/service/compact/merge.rs` | 138 |
| 压实执行 | `src/service/compact/merge.rs` | 395 |
| 文件合并核心 | `src/service/compact/merge.rs` | 655 |
| 压实调度启动 | `src/job/compactor.rs` | 29 |
| 启动恢复入口 | `src/ingester/src/lib.rs` | 93 |

---

## 9. 总结

WAL 归档、压实调度与崩溃恢复三者通过以下机制紧密协作：

1. **WAL 作为事实来源**：所有写入先确认到 WAL，确保数据不丢失
2. **5 步持久化作为衔接桥梁**：通过原子操作序列在 WAL 和磁盘文件之间建立安全的状态转移
3. **时间窗口作为安全边界**：3 倍保留时间确保本地上传完成后才开始压实
4. **偏移量作为进度标记**：每个流的压实进度通过 offset 追踪，支持节点故障转移
5. **多阶段崩溃恢复**：先清理未完成的持久化，再重放 WAL，确保任何中断点都能正确恢复

整个设计的核心哲学是：**通过可预测的文件系统操作和明确的状态标记，在不依赖复杂分布式事务的前提下，实现数据的最终一致性和故障可恢复性。**
