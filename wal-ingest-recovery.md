# OpenObserve WAL 写入与崩溃恢复分析

## 一、WAL 文件格式与核心实现

### 1.1 WAL 文件结构

WAL 库位于 `src/wal/`，底层实现采用带校验和的 Snappy 压缩格式。

**文件头结构** (`src/wal/src/lib.rs:28-38`)：
```
+-------------------------+-------------------+-------------------+
| 13 bytes File Identifier | 4 bytes Header Len | N bytes Header    |
|  "OPENOBSERVEV3"         |  (Big Endian)      | (Key-Value pairs) |
+-------------------------+-------------------+-------------------+
```

**条目结构** (`src/wal/src/writer.rs:116-171`)：
```
+-------------------+------------------------+--------------------------+
| 4 bytes CRC32     | 4 bytes Compressed Len | Snappy Compressed Data   |
| (Checksum)        | (Big Endian)           | (with embedded CRC calc) |
+-------------------+------------------------+--------------------------+
```

### 1.2 核心数据结构

**Writer** (`src/wal/src/writer.rs:29-36`)：
- `path`: WAL 文件路径
- `f`: `BufWriter<File>` 带缓冲的文件写入器
- `bytes_written`: 已写入的压缩字节数
- `uncompressed_bytes_written`: 已写入的未压缩字节数
- `buffer`: 临时缓冲区，最大 128KB

**Reader** (`src/wal/src/reader.rs:29-33`)：
- 支持从指定位置读取（Checkpoint 恢复）
- 读取时自动校验 CRC32 与长度
- 解压 Snappy 压缩数据

### 1.3 关键配置参数 (`config`)
- `data_wal_dir`: WAL 文件根目录
- `max_file_size_on_disk`: 单个 WAL 文件最大大小（触发 rotate）
- `max_file_retention_time`: WAL 文件最大保留时间（触发 rotate）
- `wal_write_buffer_size`: WAL 写入缓冲区大小
- `wal_write_queue_size`: WAL 写入队列大小
- `mem_persist_interval`: Immutable 持久化间隔
- `wal_fsync_disabled`: 是否禁用 fsync

---

## 二、Ingest 流程：先写 WAL 后写 Memtable

### 2.1 整体写入流程

入口在 `src/ingester/src/writer.rs:388-548`，核心调用链：

```
write() / write_batch()
    ↓
preprocess_batch()  # CPU 密集：JSON → Bytes, JSON → Arrow
    ↓
[可选：写入队列]  # 异步化 IO
    ↓
consume_processed()
    ├─→ rotate()        # 检查是否需要轮转
    ├─→ WAL.write()     # 1. 先写 WAL (纯 IO)
    ├─→ Memtable.write()# 2. 再写内存表 (纯 IO)
    └─→ [可选：fsync]   # 强制刷盘
```

### 2.2 关键代码分析

**Writer 结构体** (`src/ingester/src/writer.rs:57-65`)：
```rust
pub struct Writer {
    idx: usize,                          // 桶索引
    key: WriterKey,                      // org_id + stream_type
    wal: Arc<RwLock<WalWriter>>,         // WAL 写入器
    memtable: Arc<RwLock<MemTable>>,     // 内存表
    next_seq: AtomicU64,                 // 下一个 WAL ID 序列号
    created_at: AtomicI64,               // 创建时间（用于 TTL 检查）
    write_queue: Arc<mpsc::Sender<...>>, // 写入队列
}
```

**WAL 写入核心** (`src/ingester/src/writer.rs:493-508`)：
```rust
// Write into WAL - pure IO, no CPU-intensive processing
let mut wal = self.wal.write().await;
for entry in batch.bytes_entries {
    if entry.is_empty() {
        continue;
    }
    wal.write(&entry).context(WalSnafu)?;
    tokio::task::coop::consume_budget().await;
}
drop(wal);
```

**Memtable 写入** (`src/ingester/src/writer.rs:514-529`)：
```rust
// Write into Memtable - pure IO, no CPU-intensive processing
let mut mem = self.memtable.write().await;
for (entry, batch_entry) in batch.entries.into_iter().zip(batch.batch_entries) {
    if entry.data_size == 0 {
        continue;
    }
    mem.write(entry.schema.clone().unwrap(), entry, batch_entry)?;
    tokio::task::coop::consume_budget().await;
}
drop(mem);
```

### 2.3 设计要点

1. **CPU/IO 分离**：`preprocess_batch` 在调用线程完成 JSON→Arrow 转换，`consume_processed` 专注于纯 IO 操作
2. **队列异步化**：通过 `wal_write_queue_enabled` 配置可选择将写入请求放入队列，由后台任务处理
3. **WAL 先于 Memtable**：严格保证数据先持久化到磁盘，再写入内存，确保崩溃时数据不丢失
4. **协作式调度**：每次写入后调用 `consume_budget()` 避免长时间阻塞

---

## 三、Rotation 与 Immutable 持久化（WAL → Parquet）

### 3.1 Rotation 触发条件

**WAL 轮转阈值检查** (`src/ingester/src/writer.rs:662-674`)：
```rust
fn check_wal_threshold(&self, written_size: (usize, usize), data_size: usize) -> bool {
    compressed_size > FILE_TYPE_IDENTIFIER_LEN
        && (compressed_size + data_size > max_file_size_on_disk
            || uncompressed_size + data_size > max_file_size_on_disk
            || created_at + max_file_retention_time <= now())
}
```

**Memtable 轮转阈值** (`src/ingester/src/writer.rs:677-683`)：
```rust
fn check_mem_threshold(&self, written_size: (usize, usize), data_size: usize) -> bool {
    json_size > 0
        && (json_size + data_size > max_file_size_in_memory
            || arrow_size + data_size > max_file_size_in_memory)
}
```

### 3.2 Rotation 执行流程

**rotate()** (`src/ingester/src/writer.rs:551-619`)：
```rust
async fn rotate(&self, ...) -> Result<()> {
    // 1. 轮转 WAL：创建新 WAL，sync 旧 WAL
    let mut wal = self.wal.write().await;
    let (new_wal, _) = WalWriter::new(...)?;
    wal.sync()?;
    let old_wal = std::mem::replace(&mut *wal, new_wal);
    drop(wal);

    // 2. 轮转 Memtable：创建新 Memtable
    let new_mem = MemTable::new();
    let mut mem = self.memtable.write().await;
    let old_mem = std::mem::replace(&mut *mem, new_mem);
    drop(mem);

    // 3. 将旧 WAL + 旧 Memtable 放入 IMMUTABLES 等待持久化
    let table = Arc::new(Immutable::new(self.idx, self.key.clone(), old_mem));
    IMMUTABLES.write().await.insert(path, table);
}
```

### 3.3 Immutable 持久化（5 步原子协议）

**persist()** (`src/ingester/src/immutable.rs:92-131`)：

此阶段仅将 Memtable 落盘为本地 `.parquet` 文件，**不涉及对象存储上传**。

```
Step 1: 将 Memtable 写入 .par 临时文件
    ↓  memtable.persist() → 生成多个 .par 文件
    ↓  同时写入 WAL_PARQUET_METADATA 内存缓存（key 为 .parquet 路径）
Step 2: 创建 .lock 文件，记录所有 .par 文件路径
    ↓  fs::write(done_path, lock_data)
Step 3: 删除原始 .wal 文件
    ↓  fs::remove_file(wal_path)
Step 4: 将 .par 文件重命名为 .parquet
    ↓  fs::rename(path, path.with_extension("parquet"))
Step 5: 删除 .lock 文件
    ↓  fs::remove_file(done_path)
```

**关键代码**：
```rust
pub(crate) async fn persist(&self, wal_path: &PathBuf) -> Result<PersistStat> {
    // 1. dump memtable to disk
    let (schema_size, paths) = self
        .memtable
        .persist(
            self.memtable.id(),
            self.idx,
            &self.key.org_id,
            &self.key.stream_type,
        )
        .await?;
    
    // 2. create a lock file
    let done_path = wal_path.with_extension("lock");
    let lock_data = paths
        .iter()
        .map(|(p, ..)| p.to_string_lossy())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&done_path, lock_data.as_bytes()).await?;
    
    // 3. delete wal file
    fs::remove_file(wal_path).await?;
    
    // 4. rename the tmp files to parquet files
    for (path, stat) in paths {
        fs::rename(&path, &path.with_extension("parquet")).await?;
    }
    
    // 5. delete the lock file
    fs::remove_file(&done_path).await?;
}
```

**WAL_PARQUET_METADATA 缓存**：`memtable.persist()` 在写入 `.par` 之前，会将 `FileMeta` 以 `.parquet` 路径为 key 写入全局缓存 `WAL_PARQUET_METADATA` (`src/ingester/src/partition.rs:257-270`)。后续 `job::files::parquet` 扫描时优先从此缓存读取元数据，避免反复解析 parquet 文件。

### 3.4 persist 与 upload 的职责分界

**重要修正**：`immutable.persist()` 完成后，parquet 文件仅存在于本地 WAL 目录 (`data_wal_dir/files/`)。上传到对象存储是由**独立的后台任务** `job::files::parquet` 完成的，而非 persist 阶段。

```
persist 阶段 (ingester 进程内)          upload 阶段 (job::files::parquet)
───────────────────────────          ─────────────────────────────────

WAL → Memtable → IMMUTABLES          
         ↓                           
   persist() 5步协议                  
         ↓                           
   本地 .parquet 文件  ╶──────╴╴╴╴→  scan_wal_files() 扫描 .parquet
   WAL_PARQUET_METADATA 缓存          ↓
                                     prepare_files() 读取 FileMeta
                                         ↓
                                     move_files()
                                     ├─ merge_files() 合并小文件
                                     ├─ storage::put() 上传对象存储
                                     ├─ db::file_list::set() 写元数据
                                     ├─ [可选] create_tantivy_index()
                                     └─ remove_file() 删除本地文件
```

### 3.5 持久化调度

**持久化工作线程** (`src/ingester/src/lib.rs:134-178`)：
- 启动 `mem_dump_thread_num` 个工作线程，从 channel 接收持久化任务
- 每隔 `mem_persist_interval` 秒，扫描 `IMMUTABLES`，将未处理的 immutable 分发给工作线程
- `PROCESSING_TABLES` 集合防止重复处理

---

## 四、崩溃恢复与数据回放

### 4.1 恢复入口

Ingester 启动时的 `init()` 函数 (`src/ingester/src/lib.rs:93-132`)：
```rust
pub async fn init() -> errors::Result<()> {
    // 1. 先处理未完成的 parquet 文件
    wal::check_uncompleted_parquet_files().await?;

    // 2. 回放所有未持久化的 WAL 文件
    let wal_dir = PathBuf::from(&config.data_wal_dir).join("logs");
    let wal_files = wal::wal_scan_files(&wal_dir, "wal").await?;
    tokio::task::spawn(async move {
        wal::replay_wal_files(wal_dir, wal_files).await;
    });

    // 3. 启动后台任务：TTL 检查、持久化调度
    // ...
}
```

### 4.2 未完成 Parquet 文件恢复

**check_uncompleted_parquet_files()** (`src/ingester/src/wal.rs:50-105`)

根据 5 步持久化协议，崩溃可能发生在任意步骤之间：

| 崩溃时机 | 检测条件 | 恢复动作 |
|---------|---------|---------|
| Step 1 与 Step 2 之间 | 存在 .par 文件但无 .lock 文件 | 删除 .par 文件（数据仍在 .wal 中，后续回放） |
| Step 2 与 Step 3 之间 | 存在 .lock 文件 + .wal 文件 | 删除 .wal，重命名 .par → .parquet，删除 .lock |
| Step 3 与 Step 4 之间 | 存在 .lock 文件，无 .wal 文件 | 重命名 .par → .parquet，删除 .lock |
| Step 4 与 Step 5 之间 | 存在 .lock 文件 + .parquet 文件 | 仅删除 .lock 文件 |

**恢复逻辑**：
```rust
// 1. 处理所有 .lock 文件
for lock_file in lock_files.iter() {
    let wal_file = lock_file.with_extension("wal");
    if wal_file.exists() {
        std::fs::remove_file(&wal_file)?;  // Step 3 补做
    }
    // 读取 .lock 中的 .par 文件列表
    let par_files = read_lock_file(lock_file);
    // 重命名 .par → .parquet（Step 4 补做）
    for par_file in par_files {
        std::fs::rename(&par_file, &parquet_file)?;
    }
    std::fs::remove_file(lock_file)?;  // Step 5 补做
}

// 2. 删除孤立的 .par 文件（无 .lock）
for par_file in par_files {
    std::fs::remove_file(par_file)?;
}
```

### 4.3 WAL 文件回放

**replay_wal_files()** (`src/ingester/src/wal.rs:108-237`)

```rust
pub(crate) async fn replay_wal_files(wal_dir: PathBuf, wal_files: Vec<PathBuf>) -> Result<()> {
    for wal_file in wal_files.iter() {
        // 1. 从文件名解析 org_id, stream_type, idx
        let file_str = wal_file.strip_prefix(&wal_dir).unwrap()...;
        let stream_type = file_columns[len - 2];
        let org_id = file_columns[len - 3];
        let idx: usize = file_columns[len - 4].parse()?;

        // 2. 打开 WAL Reader
        let mut reader = wal::Reader::from_path(wal_file)?;

        // 3. 逐条读取并重放
        let mut memtable = memtable::MemTable::new();
        loop {
            let entry_bytes = reader.read_entry()?;
            let Some(entry_bytes) = entry_bytes else { break };
            
            // 4. 反序列化为 Entry 对象
            let mut entry = Entry::from_bytes(&entry_bytes)?;
            
            // 5. 推断 Schema，转换为 Arrow Batch，写入 Memtable
            let infer_schema = infer_json_schema_from_values(...)?;
            let batch = entry.into_batch(key.stream_type.clone(), infer_schema.clone())?;
            memtable.write(infer_schema, entry, batch)?;
        }

        // 6. 直接持久化到磁盘（不走 IMMUTABLES 队列）
        let immutable = immutable::Immutable::new(idx, key, memtable);
        let stat = immutable.persist(&wal_path).await?;
    }
}
```

### 4.4 错误容忍机制

读取条目时的容错处理 (`src/ingester/src/wal.rs:142-163`)：
```rust
let entry = match reader.read_entry() {
    Ok(entry) => entry,
    Err(wal::Error::UnableToReadData { source }) => {
        log::error!("Unable to read entry, skip");
        continue;
    }
    Err(wal::Error::LengthMismatch { expected, actual }) => {
        log::error!("Length mismatch, skip");
        continue;
    }
    Err(wal::Error::ChecksumMismatch { expected, actual }) => {
        log::error!("Checksum mismatch, skip");
        continue;
    }
    Err(e) => return Err(Error::WalError { source: e }),
};
```

**容错策略**：
- CRC 校验失败 → 跳过该条目
- 长度不匹配 → 跳过该条目
- 无法读取数据 → 跳过该条目
- 其他严重错误 → 终止回放

---

## 五、整体数据流

```
                    ┌──────────────────────────────────────────────┐
                    │           Ingest 请求                        │
                    └─────────────────────┬────────────────────────┘
                                          │
                                          ▼
                    ┌──────────────────────────────────────────────┐
                    │  preprocess_batch()                          │
                    │  - JSON → Bytes (for WAL)                    │
                    │  - JSON → Arrow Batch (for Memtable)        │
                    └─────────────────────┬────────────────────────┘
                                          │
                        ┌─────────────────┼─────────────────┐
                        │ 队列模式?        │                 │
                        ▼                 ▼                 │
              ┌─────────────────┐  ┌──────────────────┐     │
              │  Write Queue    │  │ consume_processed│     │
              │ (异步消费)      │  │ (同步执行)       │     │
              └─────────┬───────┘  └─────────┬────────┘     │
                        │                    │                │
                        └────────────────────┼────────────────┘
                                             │
                              ┌──────────────▼──────────────┐
                              │       rotate() 检查          │
                              │ 达到阈值则轮转 WAL+Memtable │
                              └──────────────┬──────────────┘
                                             │
                        ┌────────────────────┼────────────────────┐
                        ▼                    ▼                    │
              ┌──────────────────┐  ┌──────────────────┐          │
              │  WAL.write()     │  │ Memtable.write() │          │
              │  (Snappy+CRC)    │  │  (Arrow Batch)   │          │
              └─────────┬────────┘  └─────────┬────────┘          │
                        │                    │                    │
                        └────────────────────┼────────────────────┘
                                             │
                      ┌──────────────────────▼──────────────────────┐
                      │ 阈值触发或 TTL 到期 → rotate()               │
                      │  旧 WAL + 旧 Memtable → IMMUTABLES 队列     │
                      └──────────────────────┬──────────────────────┘
                                             │
                              ┌──────────────▼──────────────┐
                              │  persist 调度线程            │
                              │  每隔 mem_persist_interval   │
                              └──────────────┬──────────────┘
                                             │
                              ┌──────────────▼──────────────┐
                              │  immutable.persist()         │
                              │  5 步原子协议写入 Parquet    │
                              │  Step 1: 写 .par 临时文件   │
                              │  Step 2: 写 .lock 文件      │
                              │  Step 3: 删 .wal 文件       │
                              │  Step 4: .par → .parquet    │
                              │  Step 5: 删 .lock 文件      │
                              │  + 写 WAL_PARQUET_METADATA  │
                              └──────────────┬──────────────┘
                                             │
                              ┌──────────────▼──────────────┐
                              │ job::files::parquet 后台任务 │
                              │ 每隔 file_push_interval 扫描 │
                              │ 本地 .parquet 文件           │
                              └──────────────┬──────────────┘
                                             │
                              ┌──────────────▼──────────────┐
                              │ move_files()                 │
                              │ 1. merge_files() 合并小文件  │
                              │ 2. storage::put() 上传存储   │
                              │ 3. db::file_list::set() 写元 │
                              │ 4. [可选] 生成倒排索引       │
                              │ 5. 删除本地 .parquet 文件    │
                              └──────────────┬──────────────┘
                                             │
                                             ▼
                      ┌──────────────────────────────────────────┐
                      │        对象存储中的 Parquet 文件          │
                      │      + file_list DB 元数据记录            │
                      └──────────────────────────────────────────┘
```

---

## 六、崩溃后重启恢复流程

```
                    ┌──────────────────────────────────┐
                    │        节点启动 / Ingester init() │
                    └──────────────────┬───────────────┘
                                       │
                        ┌──────────────▼───────────────┐
                        │ check_uncompleted_parquet_files() │
                        │  扫描 .lock 和 .par 文件        │
                        └──────────────┬───────────────┘
                                       │
                ┌──────────────────────┼──────────────────────┐
                ▼                      ▼                      ▼
    ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
    │ 有 .lock + .wal?    │  │ 有 .lock 无 .wal?   │  │ 只有 .par 无 .lock? │
    │ 删 .wal             │  │ 直接跳过重命名       │  │ 删 .par (数据还在   │
    │ 重命名 .par→.parquet│  │ 删 .lock            │  │  .wal 中后续回放)   │
    │ 删 .lock            │  │                     │  │                     │
    └──────────┬──────────┘  └──────────┬──────────┘  └──────────┬──────────┘
               │                        │                        │
               └────────────────────────┼────────────────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │ replay_wal_files()       │
                           │ 扫描所有 .wal 文件        │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │ 逐个读取 WAL 条目:       │
                           │  - 反序列化 Entry        │
                           │  - Schema 推断 + 合并    │
                           │  - 写入新 Memtable       │
                           │  - 跳过损坏条目 (CRC/长度 │
                           │    不匹配)               │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │ 直接 persist() 到 Parquet │
                           │ (不走 IMMUTABLES 队列)   │
                           │ 同样遵循 5 步原子协议     │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │ 启动正常写入流程          │
                           │ - TTL 检查任务            │
                           │ - 持久化调度任务          │
                           └──────────────────────────┘
```

---

## 七、关键文件索引

| 文件 | 职责 | 关键函数 |
|------|------|----------|
| `src/wal/src/writer.rs` | WAL 底层写入 | `new()`, `write()`, `sync()` |
| `src/wal/src/reader.rs` | WAL 底层读取 | `from_path()`, `read_entry()` |
| `src/ingester/src/writer.rs` | Ingester 写入主逻辑 | `write_batch()`, `consume_processed()`, `rotate()` |
| `src/ingester/src/immutable.rs` | Immutable 持久化 | `persist()` (5 步协议), `persist_table()` |
| `src/ingester/src/wal.rs` | 崩溃恢复逻辑 | `check_uncompleted_parquet_files()`, `replay_wal_files()` |
| `src/ingester/src/lib.rs` | Ingester 初始化入口 | `init()`, `run()` (持久化调度) |
| `src/ingester/src/memtable.rs` | 内存表实现 | `write()`, `read()`, `persist()` |
| `src/job/compactor.rs` | Compactor 调度 | `run()` (合并、保留、删除) |

---

## 八、Compactor 数据合并与 Parquet 生成

### 8.1 合并任务调度架构

Compactor 采用**两级 Worker** 架构 (`src/service/compact/mod.rs` + `src/service/compact/worker.rs`)：

```
                        ┌──────────────────────────────┐
                        │   run_merge() 主调度器        │
                        │  从 DB 获取待处理 Jobs        │
                        │  分发到 JobScheduler         │
                        └───────────────┬──────────────┘
                                        │
                        ┌───────────────▼──────────────┐
                        │   JobScheduler               │
                        │  N 个工作线程                │
                        │  调用 merge_by_stream()      │
                        └───────────────┬──────────────┘
                                        │
                        ┌───────────────▼──────────────┐
                        │   merge_by_stream()          │
                        │  按分区 (prefix) 分组        │
                        │  生成 MergeBatch             │
                        └───────────────┬──────────────┘
                                        │
                        ┌───────────────▼──────────────┐
                        │   MergeWorker                │
                        │  N 个工作线程                │
                        │  调用 merge_files()          │
                        └───────────────┬──────────────┘
                                        ▼
                           合并完成 → 写入 file_list
```

### 8.2 合并任务生成流程

**generate_job_by_stream()** (`src/service/compact/merge.rs:69-172`)

```rust
// 关键时间窗口检查：至少等待 3 * max_file_retention_time
if offset >= time_now_hour
    || time_now.timestamp_micros() - offset
        <= Duration::try_seconds(cfg.limit.max_file_retention_time as i64)
            .unwrap()
            .num_microseconds()
            .unwrap()
            * 3
{
    return Ok(()); // 时间窗口还没到，等一等
}

// 添加合并任务到 DB
infra_file_list::add_job(org_id, stream_type, stream_name, offset).await?;
```

**设计原因**（注释说明）：
- `-- first period`: 最后一小时本地文件上传到存储，写入 file_list
- `-- second period`: 最后一小时 file_list 上传到存储
- `-- third period`: 可以开始合并，至少 3 倍 max_file_retention_time

### 8.3 文件分组与合并策略

**merge_by_stream()** (`src/service/compact/merge.rs:395-642`)

```rust
// Step 1: 按分区前缀分组
for file in files {
    let prefix = file_name[..file_name.rfind('/').unwrap()].to_string();
    partition_files_with_size.entry(prefix).or_default().push(file);
}

// Step 2: 选择合并策略
match job_strategy {
    MergeStrategy::FileSize => files.sort_by_size(),
    MergeStrategy::FileTime => files.sort_by_time(),
    MergeStrategy::TimeRange => files = sort_by_time_range(files),
}

// Step 3: 按文件大小分组（max_file_size）
for file in files_with_size.iter() {
    if new_file_size + file.meta.original_size > cfg.compact.max_file_size as i64 {
        // 生成一个合并批次
        batch_groups.push(MergeBatch { ... });
        new_file_size = 0;
        new_file_list.clear();
    }
    new_file_size += file.meta.original_size;
    new_file_list.push(file.clone());
}

// Step 4: 分发到 MergeWorker
for batch in batch_groups.iter() {
    worker_tx.send((inner_tx.clone(), batch.clone())).await?;
}
```

### 8.4 Parquet 文件合并核心

**merge_files()** (`src/service/compact/merge.rs:655-999`)

```
Step 1: 下载 parquet 文件到本地缓存
    ↓  cache_remote_files() → 从对象存储下载
Step 2: Schema 合并
    ↓  读取所有文件的 Schema → 取并集
Step 3: DataFusion 查询执行
    ↓  merge_parquet_files()
        ├─ SQL: SELECT * FROM tbl ORDER BY _timestamp DESC
        ├─ UnionTableProvider 读取所有 parquet
        └─ 执行排序与合并
Step 4: 写入新的合并 Parquet
    ↓  write_parquet() → AsyncArrowWriter
Step 5: 上传到对象存储
    ↓  storage::put() / storage::put_with_compliance()
Step 6: 生成倒排索引（可选）
    ↓  create_tantivy_index()
Step 7: 更新 file_list 元数据
    ↓  新增新文件 + 标记旧文件 deleted=true
```

**DataFusion 合并引擎** (`src/service/search/datafusion/merge/mod.rs:53-175`)

```rust
pub async fn merge_parquet_files(...) -> Result<MergeParquetResult> {
    // 1. 构造 SQL 排序查询
    let sql = format!("SELECT * FROM tbl ORDER BY {TIMESTAMP_COL_NAME} DESC");
    
    // 2. 注册 Union Table Provider（读取所有待合并文件）
    let union_table = Arc::new(NewUnionTable::new(schema.clone(), tables));
    ctx.register_table("tbl", union_table)?;
    
    // 3. 执行查询，读取 batch stream
    let mut batch_stream = execute_stream(physical_plan, ctx.task_ctx())?;
    
    // 4. 写入新的 parquet 文件
    let mut writer = new_parquet_writer(&mut buf, schema, ...);
    while let Some(batch) = rx.recv().await {
        writer.write(&batch).await?;
    }
    writer.close().await?;
    
    Ok(MergeParquetResult::Single(buf, metadata))
}
```

---

## 九、Ingester vs Compactor 职责划分

### 9.1 边界对比

| 维度 | Ingester | Compactor |
|------|----------|-----------|
| **数据来源** | HTTP/Ingest API 直接接收用户数据 | 对象存储中的 parquet 文件 |
| **处理延迟** | 低延迟，近实时处理 | 高延迟，T+N 处理（3*retention_time） |
| **输出位置** | 本地磁盘 `.parquet` + 上传到对象存储 | 对象存储 `s3/gcs/oss` |
| **文件粒度** | 小文件，频繁生成 | 大文件，按 `max_file_size` 合并 |
| **Schema 处理** | 动态推断、实时演化 | Schema 并集、规范化 |
| **索引生成** | 与 Compactor 条件相同（见下方详解） | 与 Ingester 条件相同 |
| **状态依赖** | 强依赖本地 WAL + Memtable | 依赖 file_list DB 元数据 |
| **并发模型** | 单 Writer + 多持久化 Worker | JobScheduler + MergeWorker 两级 |
| **合并时机** | 上传前合并小 parquet → 大 parquet | 定时合并多个大 parquet |

### 9.2 数据流衔接

```
Ingester 阶段                                    Compactor 阶段
───────────                                    ──────────────

Ingest API → [WAL] → Memtable
                    ↓
            rotate() 阈值触发
                    ↓
          immutable.persist()          ← 仅写本地磁盘
          ├─ .par 临时文件
          ├─ .lock 文件
          ├─ 删除 .wal
          ├─ .par → .parquet
          └─ 删除 .lock
                    ↓
          本地磁盘 .parquet 文件
          + WAL_PARQUET_METADATA 缓存
                    ↓
          job::files::parquet 后台任务
          ├─ scan_wal_files() 扫描本地 .parquet
          ├─ prepare_files() 读取 FileMeta
          ├─ move_files()
          │   ├─ merge_files() 合并小文件 (DataFusion)
          │   ├─ storage::put() 上传到对象存储
          │   ├─ db::file_list::set() 写入 file_list 元数据
          │   ├─ [可选] create_tantivy_index() 生成索引
          │   └─ remove_file() 删除本地 .parquet
          └─ scan_pending_delete_files() 清理延迟删除
                    ↓
          对象存储 .parquet + file_list ╶╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╮
                                                                │
                    等待 3*max_file_retention_time               │
                                                                │
                    ▼                                           │
          generate_job_by_stream()                              │
                    │                                           │
                    ▼                                           │
          run_merge() 拉取 Jobs                                 │
                    │                                           │
                    ▼                                           │
          merge_by_stream() 按分区分组                          │
                    │                                           │
                    ▼                                           │
          merge_files()  ───────────────────────────────────────╯
          ├─ 下载 parquet 到本地缓存
          ├─ DataFusion 排序合并
          ├─ 生成新的大 parquet
          ├─ 上传到对象存储
          ├─ [可选] generate_inverted_index()
          └─ write_file_list()（新增+标记删除）
```

### 9.3 倒排索引生成的开关条件

**Ingester 和 Compactor 的索引生成条件完全一致**，需要同时满足以下三个条件：

```
条件 1: cfg.common.inverted_index_enabled == true
    ↓  全局配置开关
条件 2: stream_type.support_index() == true
    ↓  仅 Logs / Metrics / Traces / Metadata 支持索引
    ↓  (Index / EnrichmentTables / ServiceGraph / Filelist 不支持)
条件 3: need_index == true
    ↓  stream settings 中配置了 full_text_search_fields 或 index_fields
    ↓  且这些字段存在于 latest_schema 中
```

**Ingester 侧** (`src/job/files/parquet.rs:902-935`)：
```rust
// 条件 1 + 2: 全局开关 + stream 类型
if !cfg.common.inverted_index_enabled || !stream_type.support_index() {
    return Ok((account, new_file_key, new_file_meta, retain_file_list));
}
// 条件 3: 是否有可索引字段
let need_index = full_text_search_fields
    .iter()
    .chain(index_fields.iter())
    .any(|f| latest_schema_fields.contains(f));
if !need_index {
    return Ok((account, new_file_key, new_file_meta, retain_file_list));
}
// 生成 tantivy 索引
let index_size = create_tantivy_index("INGESTER", ...).await?;
```

**Compactor 侧** (`src/service/compact/merge.rs:874-934`)：
```rust
// 条件 3: 先检查是否有可索引字段
let need_index = full_text_search_fields
    .iter()
    .chain(index_fields.iter())
    .any(|f| latest_schema_fields.contains(f));
if !need_index {
    log::debug!("skip index generation for stream: ...");
}

// 条件 1 + 2 + 3: 全部满足才生成
if cfg.common.inverted_index_enabled && stream_type.support_index() && need_index {
    generate_inverted_index("COMPACTOR", ...).await?;
}
```

**差异**：Ingester 先检查条件 1+2（可提前 return），再检查条件 3；Compactor 先检查条件 3（打印 debug 日志），再组合检查三个条件。但最终行为一致。

---

## 十、异常场景与恢复衔接分析

### 10.1 文件生命周期状态机

```
              Ingester persist 阶段              Ingester upload 阶段           Compactor 阶段
              ─────────────────────              ────────────────────           ──────────────

  [0] 初始状态
    │
    ▼  ingest 请求
  [1] WAL 写入中
    │  成功
    ▼
  [2] .wal 文件 (完整)
    │
    ▼  rotate()
  [3] IMMUTABLES 队列
    │
    ▼  persist() Step 1
  [4] 写入 .par + WAL_PARQUET_METADATA
    │  ├─ 崩溃 → 重启发现无 .lock → 删 .par
    │  └─ 成功
    ▼  persist() Step 2
  [5] 写入 .lock 文件
    │  ├─ 崩溃 → .wal + .lock + .par
    │  │         → 重启补做: 删.wal, .par→.parquet, 删.lock
    │  └─ 成功
    ▼  persist() Step 3
  [6] 删除 .wal 文件
    │  ├─ 崩溃 → .lock + .par (无 .wal)
    │  │         → 重启补做: .par→.parquet, 删.lock
    │  └─ 成功
    ▼  persist() Step 4
  [7] .par → .parquet (rename)
    │  ├─ 崩溃 → .lock + .parquet
    │  │         → 重启补做: 删.lock
    │  └─ 成功
    ▼  persist() Step 5
  [8] 删除 .lock 文件
    │
    │  ──── persist 完成，本地 .parquet 就绪 ────
    ▼
  [9] PROCESSING_FILES 标记
    │  job::files::parquet 后台扫描
    ▼
  [10] merge_files() 合并小 parquet
    │  DataFusion 排序合并
    │  ├─ 崩溃 → 本地 .parquet 仍在，下次重试
    │  └─ 成功
    ▼
  [11] storage::put() 上传到对象存储
    │  ├─ 崩溃 → 本地 .parquet 仍在，下次重试上传
    │  └─ 成功
    ▼
  [12] db::file_list::set() 写入元数据
    │  ├─ 崩溃 → 对象存储有孤立文件，无元数据
    │  │         → 下次上传新文件时覆盖（UUID 不同）
    │  │         → 孤立文件需 GC 清理
    │  └─ 成功
    ▼
  [13] [可选] create_tantivy_index() 生成索引
    │  ├─ 崩溃 → 索引缺失，搜索降级
    │  │         → compactor 合并时重新生成
    │  └─ 成功
    ▼
  [14] 删除本地 .parquet 文件
    │  ├─ 文件被 WAL 锁定 → 加入 pending_delete 列表
    │  │         → 下次 scan_pending_delete_files() 清理
    │  ├─ 删除失败 → 加入 pending_delete 列表
    │  └─ 成功 → 从 PROCESSING_FILES 移除
    ▼
  [15] 对象存储 .parquet + file_list 元数据
    │
    │  等待 3*max_file_retention_time
    ▼
  [16] compactor 合并 + 索引重新生成
    │
    ▼  delay_delete
  [17] 延迟删除旧文件
```

### 10.2 异常场景矩阵

#### persist 阶段异常

| 崩溃位置 | 现场特征 | 恢复策略 | 数据风险 |
|---------|---------|---------|---------|
| **WAL 写入中** | `.wal` 文件截断、损坏 | replay 时跳过 CRC/长度错误的条目 | 低：最后几条可能丢失 |
| **rotate 后 persist 前** | `.wal` 完整，无 `.par` | 正常回放 `.wal` | 无 |
| **Step 1 写 .par 中** | 存在 `.par` + `.wal`，无 `.lock` | 删 `.par`，回放 `.wal` | 无 |
| **Step 2 写 .lock 后** | `.lock` + `.wal` + `.par` | 删 `.wal`，`.par`→`.parquet`，删 `.lock` | 无 |
| **Step 3 删 .wal 后** | `.lock` + `.par`（无 `.wal`） | `.par`→`.parquet`，删 `.lock` | 无 |
| **Step 4 重命名中** | 部分 `.par` 已重命名 | 遍历 `.lock` 列表，补做重命名 | 无 |
| **Step 5 删 .lock 前** | `.lock` + `.parquet` | 删 `.lock` | 无 |

#### upload 阶段异常（job::files::parquet）

| 崩溃位置 | 现场特征 | 恢复策略 | 数据风险 |
|---------|---------|---------|---------|
| **merge_files() 合并中** | 本地 `.parquet` 仍在 | `PROCESSING_FILES` 标记仍在，下次循环重新合并 | 无：原文件未动 |
| **storage::put() 上传中** | 本地 `.parquet` 仍在，对象存储可能有部分数据 | 下次循环重新上传（新文件名含 UUID，旧部分由 GC 清理） | 低：对象存储可能有孤立文件 |
| **db::file_list::set() 写元数据前** | 对象存储有文件，file_list 无记录 | 下次上传产生新文件，旧孤立文件由 GC 清理 | 中：存储泄漏直到 GC |
| **db::file_list::set() 重试** | set() 内部最多重试 5 次，每次间隔 1 秒 | 5 次均失败后释放 PROCESSING_FILES 标记，下次重试 | 低 |
| **create_tantivy_index() 索引生成失败** | parquet 已上传，索引缺失 | 搜索降级走 parquet 扫描；compactor 合并时重新生成 | 低：搜索性能下降 |
| **删除本地文件时文件被锁定** | 文件被 WAL 读锁持有 | 加入 `pending_delete` 列表，后续 `scan_pending_delete_files()` 清理 | 无 |
| **删除本地文件 IO 错误** | 文件残留 | 同上，加入 `pending_delete`，下次清理 | 低：磁盘临时占用 |
| **DB 健康检查失败** | 跳过本轮扫描 | 避免 DB 不可用时生成孤立对象存储文件 | 无：主动跳过 |

#### compactor 阶段异常

| 崩溃位置 | 现场特征 | 恢复策略 | 数据风险 |
|---------|---------|---------|---------|
| **合并查询执行中** | 新文件未生成 | Job 超时释放，下次重试 | 无 |
| **上传新 parquet 后** | 新文件已上传，file_list 未更新 | `write_file_list()` 重试 5 次；孤立文件 GC 清理 | 中：存储泄漏 |
| **write_file_list() 部分成功** | batch_process 事务性操作 | 重试保证最终一致 | 低 |
| **标记删除旧文件后** | 旧文件 deleted=true 但未物理删除 | delay_delete 后续清理 | 低：存储临时占用 |

### 10.3 Compactor Job 容错机制

**Job 状态更新心跳** (`src/service/compact/mod.rs:360-383`)

```rust
// 创建后台线程，每隔 ttl 秒更新一次 job 状态
// ttl = max(60, job_run_timeout / 4)
let ttl = std::cmp::max(60, cfg.compact.job_run_timeout / 4) as u64;

tokio::task::spawn(async move {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(ttl)) => {}
            _ = rx.recv() => { return; }
        }
        // 更新 job 的 updated_at，防止其他节点接管
        infra_file_list::update_running_jobs(&job_ids).await;
    }
});
```

**Job 超时接管逻辑**：
1. Job 被节点 A 领取后，持续更新 `updated_at`
2. 如果节点 A 崩溃，心跳停止
3. 其他节点通过 `check_running_jobs` 检查超时
4. 超时后重置 `node` 字段，其他节点可重新领取

**关键配置**：
- `compact.job_run_timeout`: Job 执行超时时间（秒）
- `compact.delete_files_delay_hours`: 延迟删除等待时间

### 10.4 幂等性与重复数据

| 场景 | 是否可能重复 | 处理方式 |
|-----|------------|---------|
| WAL replay 重复读取 | 否 | 处理完直接删除 `.wal` |
| Compactor Job 重复执行 | 是 | file_list 版本检查，幂等更新 |
| 合并文件重复上传 | 是 | 文件名含 UUID，旧文件通过 GC 清理 |
| file_list 重复写入 | 是 | 事务性 batch_process，基于 ID 去重 |

### 10.5 极端场景：双写冲突

**场景**：节点 A 正在合并，节点 B 因为网络分区也开始合并同一批文件

**防护机制**：
1. **分布式锁** (`dist_lock`)：生成 job 前获取锁
2. **一致性哈希**：同一 stream 固定路由到同一 compactor 节点
3. **Job 心跳**：超时后才允许其他节点接管
4. **file_list 事务**：`batch_process` 原子更新，避免部分成功

---

## 十一、关键文件索引（补充）

| 文件 | 职责 | 关键函数 |
|------|------|----------|
| `src/service/compact/mod.rs` | Compactor 主入口 | `run_merge()`, `run_generate_job()` |
| `src/service/compact/merge.rs` | 合并逻辑 | `generate_job_by_stream()`, `merge_by_stream()`, `merge_files()` |
| `src/service/compact/worker.rs` | Worker 调度 | `JobScheduler`, `MergeWorker` |
| `src/service/compact/dump.rs` | File list dump | `dump()`, `generate_dump()` |
| `src/service/search/datafusion/merge/mod.rs` | DataFusion 合并引擎 | `merge_parquet_files()`, `write_parquet()` |
| `src/service/file_list/mod.rs` | File list 元数据 | `set()`, `progress()` |
| `src/service/db/file_list/local.rs` | 本地文件状态管理 | `add_pending_delete()`, `add_removing()` |
| `src/job/compactor.rs` | Job 总调度 | `run()` 循环调用 run_merge/run_retention |
| `src/job/files/parquet.rs` | Ingester parquet 上传任务 | `run()`, `move_files()`, `merge_files()` |
| `src/job/files/mod.rs` | Ingester 文件任务入口 | `run()`, `generate_storage_file_name()` |
| `src/ingester/src/partition.rs` | Memtable 持久化与分区 | `persist()`, `WAL_PARQUET_METADATA` 写入 |

---

## 十二、Ingester Parquet 上传至对象存储的完整流程

### 12.1 启动入口

`job::files::mod.rs` 在 Ingester 节点启动时注册 (`src/job/files/mod.rs:25-38`)：

```rust
pub async fn run() -> Result<(), anyhow::Error> {
    if !LOCAL_NODE.is_ingester() {
        return Ok(());
    }
    crate::service::db::file_list::local::load_pending_delete().await?;
    tokio::task::spawn(parquet::run());    // parquet 上传任务
    tokio::task::spawn(broadcast::run());   // file_list 广播
    tokio::task::spawn(clean_empty_dirs()); // 空目录清理
}
```

### 12.2 主循环

`job::files::parquet::run()` (`src/job/files/parquet.rs:70-178`)：

```
loop {
    sleep(file_push_interval)              // 每隔 file_push_interval 秒
        ↓
    infra::file_list::health_check()      // DB 健康检查，失败则跳过本轮
        ↓
    scan_pending_delete_files()            // 清理待删除文件
        ↓
    scan_wal_files(tx)                    // 扫描本地 .parquet 文件
        ↓
    [Worker threads] ← (prefix, files)    // 工作线程消费
        ↓
    move_files(thread_id, prefix, files)  // 合并+上传
}
```

**关键保护**：DB 健康检查 (`parquet.rs:137-142`)：
```rust
if let Err(e) = infra::file_list::health_check().await {
    log::error!(
        "[INGESTER:JOB] DB health check failed, skip uploading to avoid orphaned files in object store: {e}"
    );
    continue;
}
```
DB 不可用时主动跳过上传，避免对象存储产生无元数据的孤立文件。

### 12.3 文件准备阶段

**prepare_files()** (`src/job/files/parquet.rs:284-349`)：

```rust
// 1. 遍历扫描到的 .parquet 文件
for file in files {
    let file_key = strip_prefix(&file, &wal_dir);  // 获取相对路径
    
    // 2. 检查是否正在处理
    if PROCESSING_FILES.read().await.contains(&file_key) {
        continue;  // 跳过已在处理的文件
    }
    
    // 3. 读取 FileMeta（优先从内存缓存）
    let parquet_meta = if let Some(meta) = WAL_PARQUET_METADATA.read().await.get(&file_key) {
        meta.clone()  // 优先从内存缓存读取
    } else if let Ok(parquet_meta) = read_metadata_from_file(&file).await {
        parquet_meta  // 回退到解析文件
    } else {
        continue;  // 无法获取元数据，跳过
    };
    
    // 4. 空文件直接删除
    if parquet_meta.eq(&FileMeta::default()) {
        remove_file(wal_dir.join(&file)).await;
        continue;
    }
    
    // 5. 按分区前缀分组，标记为处理中
    PROCESSING_FILES.write().await.insert(file_key);
}
```

### 12.4 文件移动（合并 + 上传 + 清理）

**move_files()** (`src/job/files/parquet.rs:351-623`)：

```
Step 1: 前置检查
    ├─ 检查 stream 是否正在删除 → 直接删除文件
    ├─ 获取 latest schema → 失败则释放文件
    ├─ 检查 stream 是否存在 → 不存在则删除文件
    └─ 检查数据保留期 → 过期则删除文件

Step 2: 合并阈值判断
    ├─ 文件总大小 < min(max_file_size_on_disk, compact.max_file_size)
    │   且字段数 < file_move_fields_limit
    │   → 检查是否有过期文件（created_at < now - max_file_retention_time）
    │   → 没有过期文件则释放，等待更多数据积累
    └─ 满足阈值或有过期文件 → 继续合并

Step 3: 合并循环
    loop {
        merge_files() → 返回 (account, new_file_key, new_file_meta, old_file_list)
            ↓
        db::file_list::set() → 写入 file_list 元数据（重试 5 次）
            ↓
        删除旧文件:
            ├─ wal::lock_files_exists() → 文件被锁定
            │   → add_pending_delete() → 延迟删除
            └─ 文件未被锁定
                → remove_file() → 删除本地文件
                → 从 PROCESSING_FILES 移除
    }
```

### 12.5 Ingester 合并文件细节

**merge_files()** (`src/job/files/parquet.rs:627-937`)：

与 Compactor 的 `merge_files()` 逻辑相似但有以下区别：

| 维度 | Ingester merge_files() | Compactor merge_files() |
|------|----------------------|------------------------|
| 数据源 | 本地 WAL 目录 `.parquet` | 对象存储下载到缓存 |
| 最大文件大小 | `min(max_file_size_on_disk, compact.max_file_size)` | `compact.max_file_size` |
| 合并引擎 | `merge::merge_parquet_files()` | `merge::merge_parquet_files()` |
| 结果类型 | 仅支持 `Single`（Multiple 会 panic） | 支持 `Single` 和 `Multiple` |
| 上传方法 | `storage::put()` | `storage::put()` / `put_with_compliance()` |
| 索引来源 | 从 stream_settings 读取 FTS/Index 字段 | 同左 |
| 文件存储类型 | `StorageType::Wal` | `StorageType::Wal` |
| 写入 file_list | `db::file_list::set()` 单条写入 | `write_file_list()` 批量写入 |

### 12.6 file_list 元数据写入

**db::file_list::set()** (`src/service/db/file_list/mod.rs:47-80`)：

```rust
pub async fn set(account: &str, key: &str, meta: Option<FileMeta>, deleted: bool) -> Result<()> {
    let mut file_data = FileKey::new(0, account, key, meta.unwrap_or_default(), deleted);
    
    // 1. 写入 file_list 存储，最多重试 5 次
    for _ in 0..5 {
        match progress(account, key, meta.as_ref(), deleted).await {
            Ok(id) => { file_data.id = id; break; }
            Err(e) => {
                log::error!("[FILE_LIST] Error saving file to storage, retrying: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    
    // 2. 通知其他节点（如果 cache_latest_files.enabled）
    if cfg.cache_latest_files.enabled {
        broadcast::BROADCAST_QUEUE.write().await.push(file_data);
    }
}
```

**progress()** 内部：
- `!deleted` → `infra::file_list::add()` + `incr_stream_stats()`（本地模式）
- `deleted` → `infra::file_list::remove()`

### 12.7 Pending Delete 延迟删除机制

当本地 `.parquet` 文件被 WAL 读取任务锁定时（`wal::lock_files_exists()` 返回 true），不能立即删除。此时通过 **pending_delete** 机制延迟处理：

```
move_files()
    ↓  删除本地文件时
    ├─ wal::lock_files_exists(file_key) == true?
    │   ├─ Yes → db::file_list::local::add_pending_delete()
    │   │        写入 LOCAL_CACHE + 内存集合
    │   └─ No  → 直接 remove_file() + remove_removing()
    ↓
scan_pending_delete_files()  （每轮循环执行）
    ↓
    ├─ wal::lock_files_exists(file_key) == true?
    │   ├─ Yes → 跳过（仍在使用）
    │   └─ No  → 删除文件 + 从 pending_delete 移除
```

**数据持久化**：`pending_delete` 列表同时写入 `infra::file_list::LOCAL_CACHE`（SQLite），确保重启后不丢失。

---

## 十三、修正记录

本文档相比初始版本，进行了以下关键修正：

### 修正 1：persist 与 upload 是两个独立阶段

**原结论**：persist 5 步协议完成后，数据即"上传到对象存储 / Compactor"。
**修正**：`immutable.persist()` 仅将 Memtable 落盘为本地 `.parquet` 文件。上传到对象存储是由独立后台任务 `job::files::parquet` 完成的，两阶段之间通过 `WAL_PARQUET_METADATA` 内存缓存衔接 FileMeta。

### 修正 2：Ingester 也会做文件合并

**原结论**：persist 完成后直接上传到对象存储。
**修正**：`job::files::parquet::move_files()` 先调用 `merge_files()` 通过 DataFusion 合并多个小 parquet 文件为一个较大的 parquet，然后才上传。这与 Compactor 的合并逻辑相似，但数据源不同（本地文件 vs 对象存储）。

### 修正 3：索引生成条件 Ingester 与 Compactor 相同

**原结论**：索引生成"Ingester 可选，Compactor 必选"。
**修正**：两端的索引生成条件完全一致，均需同时满足：`inverted_index_enabled == true` + `stream_type.support_index() == true`（仅 Logs/Metrics/Traces/Metadata）+ stream settings 中有可索引字段。区别仅在检查顺序不同。

### 修正 4：WAL_PARQUET_METADATA 的角色

**原结论**：未提及。
**修正**：`memtable.persist()` 在写入 `.par` 文件前，以 `.parquet` 路径为 key 将 `FileMeta` 写入全局缓存 `WAL_PARQUET_METADATA`。后续 `job::files::parquet` 扫描时优先从此缓存读取元数据，避免反复解析 parquet 文件的 I/O 开销。该缓存在上传完成后清理，在 `lib.rs` 的后台任务中定期 shrink_to_fit。

### 修正 5：上传阶段的异常场景更丰富

**原结论**：仅笼统描述"上传中断，下次重新上传"。
**修正**：详细区分了 DB 健康检查失败（主动跳过）、merge 失败（保留原文件重试）、put 失败（新 UUID 覆盖）、set 失败（孤立文件 + 5 次重试）、索引生成失败（搜索降级）、文件被锁定（pending_delete 延迟删除）等多种场景。
