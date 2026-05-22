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

#### 两阶段恢复流程

该函数采用**两阶段扫描**策略，严格按照代码执行顺序：

```rust
// 阶段1: 扫描并处理所有 .lock 文件（logs/ 目录下）
let lock_files = wal_scan_files(wal_dir, "lock").await?;
for lock_file in lock_files.iter() {
    // 读取 .lock 文件中记录的 .par 文件列表
    // 删除对应的 .wal 文件（如果存在）
    // 重命名 .par -> .parquet
    // 删除 .lock 文件
}

// 阶段2: 扫描并删除所有孤立的 .par 文件（files/ 目录下）
let par_files = wal_scan_files(parquet_dir, "par").await?;
for par_file in par_files.iter() {
    // 直接删除！这些是没有 .lock 标记的临时文件
    std::fs::remove_file(par_file)?;
}
```

#### 各中断场景的真实恢复步骤

根据代码实际执行路径，各中断场景的恢复逻辑如下：

| 中断时机 | 磁盘文件状态 | 恢复执行路径 | 数据完整性 |
|---------|-------------|-------------|-----------|
| **场景1: 步骤1后、步骤2前**<br>.par 已写，.lock 未写 | `files/xxx.par` ✔️<br>`logs/xxx.lock` ❌<br>`logs/xxx.wal` ✔️ | 1. 阶段1：无 .lock，跳过<br>2. 阶段2：扫描到孤立 .par，直接删除<br>3. 后续重放：.wal 仍存在，被 `replay_wal_files` 重放 | ✅ 数据安全，通过 WAL 重放恢复 |
| **场景2: 步骤2后、步骤3前**<br>.lock 已写，.wal 未删 | `files/xxx.par` ✔️<br>`logs/xxx.lock` ✔️<br>`logs/xxx.wal` ✔️ | 1. 阶段1：找到 .lock<br>   - 删除 .wal<br>   - 重命名 .par→.parquet<br>   - 删除 .lock<br>2. 阶段2：无孤立 .par，跳过 | ✅ 数据安全，从断点续传完成 |
| **场景3: 步骤3后、步骤4前**<br>.wal 已删，.par 未改名 | `files/xxx.par` ✔️<br>`logs/xxx.lock` ✔️<br>`logs/xxx.wal` ❌ | 1. 阶段1：找到 .lock<br>   - .wal 已删，跳过删除<br>   - 重命名 .par→.parquet<br>   - 删除 .lock<br>2. 阶段2：无孤立 .par，跳过 | ✅ 数据安全，从断点续传完成 |
| **场景4: 步骤4后、步骤5前**<br>.parquet 已写，.lock 未删 | `files/xxx.parquet` ✔️<br>`logs/xxx.lock` ✔️<br>`logs/xxx.wal` ❌ | 1. 阶段1：找到 .lock<br>   - .wal 已删，跳过删除<br>   - .par 已改名，跳过重命名<br>   - 删除 .lock<br>2. 阶段2：无孤立 .par，跳过 | ✅ 数据安全，仅需清理锁文件 |

> **重要修正**：场景1中 `.wal` 文件**仍然存在**！这是之前理解的关键误区。.par 被删除是因为它没有锁标记，不能保证完整性，但原始数据在 WAL 中，会通过重放机制完整恢复，不会丢失。

**关键代码位置**：
- `src/ingester/src/wal.rs:50` - `check_uncompleted_parquet_files` 函数
- `src/ingester/src/wal.rs:93-103` - 孤立 .par 文件删除逻辑

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

### 6.4 重放进行中与压实调度的时序边界

#### 启动顺序与任务初始化

从 `src/main.rs:286-295` 可以看出严格的初始化顺序：

```rust
// 步骤1: 初始化 ingester（包含崩溃恢复）
if let Err(e) = ingester::init().await {
    panic!("ingester init failed: {e}");
}

// 步骤2: 初始化 job（包含上传和压实调度）
if let Err(e) = job::init().await {
    panic!("job init failed: {e}");
}
```

`ingester::init()` 内部执行顺序 (`src/ingester/src/lib.rs:93-131`)：
```rust
pub async fn init() -> errors::Result<()> {
    // 1. 同步执行：检查未完成的 parquet 文件
    wal::check_uncompleted_parquet_files().await?;
    
    // 2. 异步启动：WAL 重放（不阻塞）
    tokio::task::spawn(async move {
        wal::replay_wal_files(wal_dir, wal_files).await;
    });
    
    // 3. 同步启动：MemTable TTL 检查任务
    tokio::task::spawn(async move {
        loop {
            sleep(Duration::from_secs(max_file_retention_time)).await;
            writer::check_ttl().await;
        }
    });
    
    // 4. 同步启动：Immutable 持久化任务
    tokio::task::spawn(async move {
        run().await; // 内部按 mem_persist_interval 循环
    });
    
    Ok(()) // 返回，不等待重放完成
}
```

`job::init()` 内部启动 (`src/job/mod.rs` → `src/job/files/mod.rs:33` → `src/job/compactor.rs:29`)：
```rust
// 文件上传任务启动（ingester 节点）
tokio::task::spawn(parquet::run()); 
// 内部循环：sleep(file_push_interval) → scan_wal_files → 上传

// 压实调度任务启动（compactor 节点）
spawn_pausable_job!("run_generate_job", compact.interval, { ... });
spawn_pausable_job!("run_merge", compact.interval + 2, { ... });
// 内部循环：sleep(compact.interval) → 生成/执行压实任务
```

#### 完整时间线分析（默认配置）

假设配置：
- `file_push_interval = 10s`（文件上传扫描周期）
- `compact.interval = 60s`（压实调度周期）
- `max_file_retention_time = 300s`（文件最大保留时间）
- `mem_persist_interval = 10s`（Immutable 持久化周期）

```
时间轴 (T=服务启动时刻)

T+0ms
  ├─ ingester::init() 开始
  │   ├─ check_uncompleted_parquet_files() 执行 (同步)
  │   │   └─ 两阶段扫描：处理 .lock + 删除孤立 .par
  │   ├─ 启动 WAL 重放任务 (后台异步)
  │   ├─ 启动 MemTable TTL 检查任务 (后台，周期 300s)
  │   └─ 启动 Immutable 持久化任务 (后台，周期 10s)
  └─ ingester::init() 返回 ✓

T+1ms ~ T+N (WAL 重放进行中)
  ├─ 重放线程读取 WAL 文件，写入临时 MemTable
  ├─ 每完成一个 WAL 文件 → immutable.persist() → 写入本地 .parquet
  │   └─ 此时数据可通过本地 files/ 目录被查询 ✓
  └─ 同时新的写入也在正常进行

T+10s  (第1次文件上传扫描)
  ├─ scan_wal_files() 扫描本地 .parquet 文件
  ├─ 包括：
  │   ├─ 重放已完成并持久化的文件 ✓ 可上传
  │   └─ 新写入产生的文件 ✓ 可上传
  └─ 满足阈值的文件被上传到对象存储
      └─ 写入 file_list 元数据后，数据可从存储查询 ✓

T+60s  (第1次压实调度)
  ├─ run_generate_job() 生成本小时压实任务
  └─ 时间窗口检查：3 * 300s = 900s
      └─ T+60s < 900s → 跳过，不压实 ✗

T+300s (MemTable TTL 首次检查)
  └─ 检查活跃 MemTable 是否超时，触发旋转

T+600s (第10次压实调度)
  └─ T+600s < 900s → 仍跳过 ✗

T+900s (第15次压实调度)
  ├─ T+900s >= 900s → 时间窗口满足 ✓
  ├─ 生成 T-900s 之前小时的压实任务
  └─ 开始执行压实：
      ├─ 从对象存储下载小文件
      ├─ DataFusion 合并
      ├─ 上传大文件
      └─ 原子更新 file_list（标记旧文件删除）
          └─ 压实完成，查询性能提升 ✓
```

#### 数据可查询性的四层边界

| 阶段 | 数据位置 | 查询路径 | 时间点 |
|-----|---------|---------|-------|
| **层1: 重放中内存** | 重放临时 MemTable | ❌ 不可查询（不在 IMMUTABLES 中） | 重放进行中 |
| **层2: 重放完成本地** | `files/*.parquet` | ✅ 可查询（本地文件扫描） | 单 WAL 文件重放完成后 |
| **层3: 上传完成存储** | 对象存储 + file_list | ✅ 可查询（对象存储扫描） | 上传完成 + file_list 写入后 |
| **层4: 压实完成优化** | 对象存储（合并后） | ✅ 可查询（性能最优） | T + 3 * max_file_retention_time 后 |

#### 关键时序保护机制

**保护机制1: WAL 重放不阻塞服务启动**
```rust
// src/ingester/src/lib.rs:105
tokio::task::spawn(async move {
    // 异步执行，不阻塞 init 返回
    wal::replay_wal_files(wal_dir, wal_files).await;
});
```
- ✅ 服务快速恢复写入能力
- ❌ 重放期间这部分数据暂不可查询

**保护机制2: 压实 3 倍时间窗口**
```rust
// src/service/compact/merge.rs:138
if time_now.timestamp_micros() - offset 
    <= Duration::try_seconds(max_file_retention_time as i64)
        .unwrap().num_microseconds().unwrap() * 3 {
    return Ok(()); // 时间未到，等待
}
```
- 确保重放产生的文件有足够时间上传
- 避免压实正在上传/重放的数据

**保护机制3: 本地文件扫描的 PROCESSING 标记**
```rust
// src/job/files/parquet.rs:309
if PROCESSING_FILES.read().await.contains(&file_key) {
    continue; // 跳过正在处理的文件
}
```
- 防止重放持久化和上传任务同时操作同一文件

**保护机制4: 上传任务的 DB 健康检查**
```rust
// src/job/files/parquet.rs:137
if let Err(e) = infra::file_list::health_check().await {
    continue; // DB 不可用时跳过，避免产生孤立文件
}
```
- 防止 DB 故障时上传文件但无法写入元数据

#### 重放与压实的潜在交互边界

| 交互场景 | 结果 | 保护机制 |
|---------|------|---------|
| 重放产生的文件正在被上传，压实调度启动 | 压实时时间窗口未到，直接跳过 | 3× 时间窗口 |
| 重放持久化正在写 .parquet，上传扫描启动 | 文件被标记 PROCESSING，跳过 | PROCESSING_FILES 锁 |
| 重放进行中节点再次崩溃 | 重启后重新执行整个恢复流程 | 恢复逻辑幂等 |
| 上传到一半节点崩溃 | 重启后重新扫描，重新上传 | 上传操作幂等，元数据去重 |

---

### 6.5 重放写盘与上传扫描的判定条件拆分

#### 阶段 A: 重放写盘阶段（Replay → Local Disk）

**所属模块**：`src/ingester/src/`

**核心代码路径**：
- `replay_wal_files()` → `immutable.persist()` → 5 步持久化

**判定条件（仅依赖持久化层内部状态）**：

| 判定条件 | 代码位置 | 说明 |
|---------|---------|------|
| WAL 文件存在 | `wal.rs:112` | 遍历启动时扫描到的 WAL 文件列表 |
| WAL 文件可打开 | `wal.rs:129-134` | 打开失败则跳过该文件 |
| 条目 CRC 校验通过 | `wal.rs:154-158` | 校验失败跳过该条目，不影响整个文件 |
| 条目长度匹配 | `wal.rs:148-152` | 长度不匹配跳过该条目 |
| Entry 反序列化成功 | `wal.rs:167-175` | 反序列化失败跳过该条目 |
| PROCESSING_TABLES 无冲突 | `immutable.rs:147` | 防止同一 WAL 被多个持久化线程重复处理 |

**关键特性**：
- ✅ **无阈值限制**：只要 WAL 文件重放完成就立即写盘，不等待大小/时间阈值
- ✅ **无外部依赖**：不依赖 DB、不依赖对象存储，纯本地文件系统操作
- ✅ **文件粒度并行**：多个 WAL 文件可以并行重放和持久化
- ✅ **幂等操作**：5 步持久化设计确保中断后可安全重试
- ❌ **无并发生成**：重放产生的是临时 MemTable，不会和正常写入的 MemTable 冲突

**锁机制**：`PROCESSING_TABLES`（ingester 内部锁）
- 类型：`RwAHashSet<PathBuf>`
- 作用域：仅在 `immutable.rs` 内使用
- 保护对象：IMMUTABLES 队列中的待持久化表

---

#### 阶段 B: 上传扫描阶段（Local Disk → Object Storage）

**所属模块**：`src/job/files/parquet.rs`

**核心代码路径**：
- `scan_wal_files()` → `prepare_files()` → `move_files()` → 上传

**判定条件（分为准入判定、阈值判定、健康判定三层）**：

##### 层1: 准入判定（prepare_files）

| 判定条件 | 代码位置 | 说明 |
|---------|---------|------|
| 文件扩展名是 .parquet | `parquet.rs:239` | 扫描时只匹配 parquet 扩展名 |
| 路径可规范化 | `parquet.rs:294-304` | canonicalize + strip_prefix 失败跳过 |
| PROCESSING_FILES 无冲突 | `parquet.rs:309-311` | 正在处理的文件跳过 |
| 元数据可读 | `parquet.rs:313-318` | 优先读缓存 `WAL_PARQUET_METADATA`，失败则读磁盘 |
| 文件非空 | `parquet.rs:320-328` | `FileMeta::default()` 表示空文件，直接删除 |

##### 层2: 阈值判定（move_files）

**三取一逻辑**，满足任一即可上传：

```rust
// src/job/files/parquet.rs:469-506
// 条件1: 大小阈值
if total_original_size >= min(max_file_size_on_disk, compact.max_file_size)
    // 条件2: 字段数量阈值（用于控制合并开销）
    || (file_move_fields_limit > 0 && stream_fields_num >= file_move_fields_limit)
    // 条件3: 时间阈值
    || has_expired_files  // 文件创建时间早于 now - max_file_retention_time
{
    // 开始上传
}
```

| 阈值条件 | 默认值 | 说明 |
|---------|-------|------|
| 大小阈值 | 256MB | `min(max_file_size_on_disk, compact.max_file_size)` |
| 时间阈值 | 300s | `now - file_created > max_file_retention_time` |
| 字段阈值 | 0（禁用） | `stream_fields_num >= file_move_fields_limit` |

##### 层3: 健康判定（scan_wal_files 循环入口）

| 判定条件 | 代码位置 | 说明 |
|---------|---------|------|
| 非离线模式 | `parquet.rs:112-114` | 集群离线则停止上传 |
| DB 健康检查通过 | `parquet.rs:137-142` | DB 不可用跳过，避免产生孤立文件 |

**关键特性**：
- ✅ **数据源中立**：不关心文件是重放产生的还是正常写入产生的，一视同仁
- ✅ **批量处理**：按分区分组后批量合并上传，减少对象存储 API 调用
- ❌ **依赖外部服务**：需要 DB 和对象存储可用

**三步真实执行顺序**（严格按照代码实现）：

```
move_files()
    │
    ├─ [merge_files()] 内部执行
    │   ├─ 读取本地 .parquet 文件到内存
    │   ├─ DataFusion 合并小文件
    │   └─ ① 对象存储上传  (parquet.rs:824)
    │       storage::put(&account, &new_file_key, buf)
    │
    ├─ ② 元数据写入  (parquet.rs:542)
    │   db::file_list::set(&account, &new_file_name, meta)
    │   └─ 失败则释放所有 PROCESSING_FILES 并返回
    │
    └─ ③ 旧本地文件清理  (parquet.rs:579)
        ├─ 检查 wal::lock_files_exists()
        ├─ 无锁: remove_file() 直接删除
        └─ 有锁: add_pending_delete() 加入待删队列
```

| 步骤 | 操作 | 代码位置 | 失败影响 |
|-----|------|---------|---------|
| ① | 对象存储上传 | `parquet.rs:824` | 重试 merge_files，不清理本地文件 |
| ② | 元数据写入 | `parquet.rs:542` | 释放 PROCESSING_FILES，下次扫描重试 |
| ③ | 本地文件清理 | `parquet.rs:579` | 加入 pending_delete 队列，后续异步清理 |

> **重要修正**：之前描述的"先删本地后传存储"是错误的。真实顺序是**先上传对象存储 → 再写元数据 → 最后删本地文件**。这样即使上传或元数据写入失败，本地文件仍然存在，可以重试。

**锁机制**：`PROCESSING_FILES`（job 模块内部锁）
- 类型：`RwAHashSet<String>`
- 作用域：仅在 `parquet.rs` 内使用
- 保护对象：待上传的本地文件
- 设置时机：`prepare_files()` 中扫描发现后立即设置
- 释放时机：上传成功后删除文件时，或元数据写入失败时

---

#### 两层锁的本质区别

| 维度 | PROCESSING_TABLES | PROCESSING_FILES |
|-----|------------------|-----------------|
| 所属模块 | ingester | job/files |
| 保护对象 | 内存中的 IMMUTABLE 表 | 磁盘上的 .parquet 文件 |
| 生命周期 | 持久化开始 → 持久化结束 | 扫描发现 → 上传完成/失败 |
| 重放阶段是否使用 | ✅ 是（重放持久化） | ✅ 是（上传重放产物） |
| 并发保护目标 | 多个持久化线程重复处理同一表 | 多次扫描重复处理同一文件 |

> **关键澄清**：这是两套**完全独立**的并发保护机制，作用于不同阶段、保护不同对象。之前混淆为"同一层并发保护"是错误的。

---

### 6.6 故障排查指引

#### 核心判定边界（避免混淆）

| 判定维度 | 重放未完成 | 上传等待 | 上传中 | 上传完成待清理 |
|---------|-----------|---------|-------|-------------|
| **logs/ 有 .wal?** | ✅ 是 | ❌ 否 | ❌ 否 | ❌ 否 |
| **files/ 有 .parquet?** | ❌ 无 | ✅ 是 | ✅ 是 | ⚠️ 有（待删） |
| **对象存储有新文件?** | ❌ 否 | ❌ 否 | ⚠️ 部分 | ✅ 是 |
| **本地可查询?** | ❌ 否 | ✅ 是 | ✅ 是 | ✅ 是 |
| **存储可查询?** | ❌ 否 | ❌ 否 | ⚠️ 部分 | ✅ 是 |

> **关键区分原则**：只要 `logs/` 目录下还有 `.wal` 文件，就是**重放未完成**。只要 `.wal` 已删、`.parquet` 在 `files/` 中，就是**上传阶段问题**，不要再怀疑重放。

---

#### 场景 1: 判断重放未完成

**定义**：WAL 文件未被完整重放并持久化到本地 `.parquet`

**现象特征**：

1. **日志特征**
   ```
   warn: replay wal file: "logs/xxx/0/xxx.wal" starting...
   warn: replay wal file: "logs/xxx/0/xxx.wal", entries: 1000, records: 50000
   # 但没有出现：
   warn: replay wal file: "logs/xxx/0/xxx.wal" done, json_size: ...
   ```

2. **磁盘特征（金标准）**
   - `data_wal_dir/logs/` 目录下**仍有 `.wal` 文件存在**
   - `data_wal_dir/logs/` 目录下可能有对应 `.lock` 文件（如果持久化进行中崩溃）
   - `data_wal_dir/files/` 目录下**无对应** `.parquet` 文件（或只有不完整的 .par）

3. **查询特征**
   - 崩溃前写入的数据**本地查询不到**（重放进行中，数据在临时 MemTable）
   - 对象存储中没有对应时间段的新文件

4. **指标特征**
   - `ingest_wal_used_bytes` 指标持续高位不下降
   - `ingest_memtable_bytes` 指标有临时波动（重放写入 MemTable）

**排查步骤**：
1. **首先检查**：`logs/` 目录是否还有残留 `.wal` 文件（这是金标准）
2. 检查日志中是否有 `replay wal file: ... starting` 但无 `done`
3. 检查 ingester 进程 CPU/IO 是否异常（可能重放卡死）
4. 如进程正常，耐心等待；如进程异常，重启触发再次重放

---

#### 场景 2: 判断上传阶段各子状态

根据三步执行顺序（上传→写元数据→删本地），上传阶段可细分为三种子状态：

##### 子状态 2a: 上传等待（未达阈值）

**定义**：数据已在本地 `.parquet`，但未满足上传阈值，等待触发条件

**现象特征**：

1. **日志特征**
   ```
   # 只有扫描日志，无合并上传日志：
   debug: scan files get total: 15, took: 12 ms
   # 但没有出现：
   info: merge small file: files/...
   info: merged 5 files into a new file: ...
   ```

2. **磁盘特征**
   - `data_wal_dir/logs/` 目录下**无 `.wal` 文件**（重放已完成）
   - `data_wal_dir/files/` 目录下有大量 `.parquet` 文件
   - 文件总大小 < 256MB（未达大小阈值）
   - 文件最旧创建时间 < 300s（未达时间阈值）

3. **查询特征**
   - 本地查询可以查到数据（走本地文件扫描）
   - 对象存储查询查不到（未上传）

4. **指标特征**
   - `ingest_parquet_files` 指标持续增长
   - `ingest_wal_used_bytes` 指标下降（WAL 已删）但不为 0

**排查步骤**：
1. 检查 `files/` 目录下文件总大小和最旧文件创建时间
2. 检查日志中是否有 `DB health check failed`（DB 故障导致跳过上传）
3. 检查 `max_file_retention_time` 配置是否过大
4. 如急需上传，可等待时间阈值触发（文件创建时间不变，重启不重置）

---

##### 子状态 2b: 上传进行中（正在执行三步）

**定义**：正在执行 `merge_files()` 或 `db::file_list::set()`，尚未完成

**现象特征**：

1. **日志特征**
   ```
   info: merge small file: files/default/logs/xxx/...
   info: merged 5 files into a new file: files/default/logs/..., original_size: 128MB
   # 但还没有出现：
   info: move files to s3 success, files: 5, size: 128MB
   ```

2. **磁盘特征**
   - `data_wal_dir/files/` 目录下的 `.parquet` 文件**仍然存在**（本地文件要等元数据写入后才删）
   - 这些文件在 `PROCESSING_FILES` 集合中（可通过 status API 查看）

3. **查询特征**
   - 本地可查（文件还在）
   - 对象存储**部分可查**（如果 `storage::put` 已完成但 `db::file_list::set` 未完成）

**排查步骤**：
1. 检查 ingester 网络 IO 是否正常（可能在等对象存储响应）
2. 检查 DB 连接是否正常（可能在等元数据写入）
3. 如长时间无进展，检查对象存储带宽/限流配置

---

##### 子状态 2c: 上传完成待清理（本地文件待删）

**定义**：已完成上传和元数据写入，但本地文件因被查询锁定而暂未删除

**现象特征**：

1. **日志特征**
   ```
   warn: the file is in use, set to pending delete list: files/...
   ```

2. **磁盘特征**
   - 对象存储已有对应文件（可手动验证）
   - `data_wal_dir/files/` 目录下的 `.parquet` 文件**仍然存在**
   - 文件在 `pending_delete` 列表中（`db/file_list/local`）

3. **查询特征**
   - 本地和对象存储都可查

**排查步骤**：
1. 这是正常现象，文件被查询引用时不会立即删除
2. 查询释放后，`scan_pending_delete_files` 会异步清理
3. 如长时间不清理，检查是否有查询泄漏

---

#### 场景 3: 混合场景排查流程

```
发现数据查询不到
    │
    ├─ 🔍 第一判定：logs/ 目录有 .wal 文件吗?
    │   ├─ ✅ 有 → 【重放未完成】
    │   │       ├─ 检查 replay 日志是否有 starting 无 done
    │   │       ├─ 检查进程 CPU/IO 是否正常
    │   │       └─ 等待重放完成，或重启重试
    │   │
    │   └─ ❌ 无 → 重放已完成，进入上传阶段判定
    │
    ├─ 🔍 第二判定：files/ 目录有 .parquet 文件吗?
    │   ├─ ✅ 有 → 【上传阶段问题】
    │   │       ├─ 检查日志：有 merge small file 日志吗?
    │   │       │   ├─ ❌ 无 → 【上传等待（未达阈值）】
    │   │       │   │       ├─ 检查文件总大小是否 < 256MB
    │   │       │   │       ├─ 检查最旧文件创建时间是否 < 300s
    │   │       │   │       ├─ 检查是否有 DB health check failed
    │   │       │   │       └─ 等待阈值触发，或检查配置
    │   │       │   │
    │   │       │   └─ ✅ 有 → 【上传进行中】
    │   │       │           ├─ 检查是否有 "merged ... into a new file"
    │   │       │           ├─ 检查网络/DB 是否正常
    │   │       │           ├─ 如长时间卡住，检查对象存储限流
    │   │       │           └─ 等待上传完成
    │   │       │
    │   │       └─ 额外检查：对象存储有对应文件吗?
    │   │               ├─ ✅ 有 → 【上传完成待清理】
    │   │               │       ├─ 检查是否有 "set to pending delete list" 日志
    │   │               │       └─ 正常现象，等待查询释放后自动清理
    │   │               │
    │   │               └─ ❌ 无 → 【上传失败】
    │   │                       ├─ 检查对象存储权限/网络
    │   │                       ├─ 检查 access key/secret key 配置
    │   │                       └─ 重启重试
    │   │
    │   └─ ❌ 无 → 本地文件已删除，进入元数据/压实判定
    │
    ├─ 🔍 第三判定：对象存储有对应文件吗?
    │   ├─ ❌ 无 → 【上传失败】
    │   │       ├─ 检查对象存储连通性
    │   │       ├─ 检查 bucket 权限
    │   │       └─ 检查网络策略/防火墙
    │   │
    │   └─ ✅ 有 → 已上传，进入元数据/压实判定
    │
    ├─ 🔍 第四判定：file_list 表有对应元数据吗?
    │   ├─ ❌ 无 → 【元数据写入失败】
    │   │       ├─ 检查 DB 状态
    │   │       ├─ 检查 db::file_list::set 相关错误日志
    │   │       └─ 重启触发重新上传（本地文件已删? 检查 pending_delete）
    │   │
    │   └─ ✅ 有 → 元数据已写，进入压实判定
    │
    └─ 🔍 第五判定：压实 offset 推进到该时间段吗?
        ├─ ❌ 未到 → 【等待时间窗口】
        │       └─ 需等待 3 × max_file_retention_time（默认 900s）
        │
        └─ ✅ 已到 → 【压实问题】
                ├─ 检查压实任务是否被阻塞
                ├─ 检查 compactor 节点状态
                └─ 检查 compact_files 表的 offset 记录
```

---

#### 场景 4: 常见疑难杂症快速定位

| 现象 | 可能原因 | 验证方法 |
|-----|---------|---------|
| logs/ 无 .wal，files/ 有 .parquet，对象存储无文件，且超过 300s | DB 健康检查失败 | 搜索日志 `DB health check failed` |
| 对象存储有文件但查询不到 | file_list 元数据未写入 | 检查 `db/file_list` 表是否有该文件记录 |
| 本地文件超过 300s 仍不删除 | 文件被查询锁定 | 搜索日志 `set to pending delete list` |
| 重放日志显示 done，但数据仍查不到 | 重放持久化时写入了不同的 schema_key | 检查 parquet 文件路径中的 schema_key 是否匹配 |
| 上传成功但 `ingest_wal_used_bytes` 不下降 | pending_delete 积压 | 检查 `db/file_list/local/pending_delete` 表大小 |

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
| 文件上传主函数 | `src/job/files/parquet.rs` | 351 |
| 上传准入判定 | `src/job/files/parquet.rs` | 284 |
| 上传阈值判定 | `src/job/files/parquet.rs` | 469 |
| 对象存储上传 | `src/job/files/parquet.rs` | 824 |
| 元数据写入 | `src/job/files/parquet.rs` | 542 |
| 本地文件清理 | `src/job/files/parquet.rs` | 579 |
| DB 健康检查 | `src/job/files/parquet.rs` | 137 |
| PROCESSING_TABLES 锁 | `src/ingester/src/immutable.rs` | 40 |
| PROCESSING_FILES 锁 | `src/job/files/parquet.rs` | 46 |
| 待删除文件扫描 | `src/job/files/parquet.rs` | 181 |
| 压实任务生成 | `src/service/compact/merge.rs` | 72 |
| 压实时间窗口检查 | `src/service/compact/merge.rs` | 138 |
| 压实执行 | `src/service/compact/merge.rs` | 395 |
| 文件合并核心 | `src/service/compact/merge.rs` | 655 |
| 压实调度启动 | `src/job/compactor.rs` | 29 |
| 启动恢复入口 | `src/ingester/src/lib.rs` | 93 |
| 主启动顺序 | `src/main.rs` | 286 |

---

## 9. 总结

WAL 归档、压实调度与崩溃恢复三者通过以下机制紧密协作：

1. **WAL 作为事实来源**：所有写入先确认到 WAL，确保数据不丢失
2. **5 步持久化作为衔接桥梁**：通过原子操作序列在 WAL 和磁盘文件之间建立安全的状态转移
3. **两阶段恢复作为容错基础**：先处理带锁标记的文件，再清理无锁临时文件，确保场景1的数据通过 WAL 重放恢复
4. **时间窗口作为安全边界**：3 倍保留时间确保本地上传完成后才开始压实
5. **偏移量作为进度标记**：每个流的压实进度通过 offset 追踪，支持节点故障转移
6. **多层时序保护**：异步重放、PROCESSING 锁、DB 健康检查四重机制确保重放与压实互不干扰

### 关键修正澄清

- **场景1误解修正**：".par 已写无 .lock" 时 `.wal` 文件**仍然存在**，数据通过 WAL 重放完整恢复，不会丢失
- **时序边界澄清**：重放是异步后台执行，与上传、压实任务并行，但通过 3× 时间窗口、PROCESSING 标记等机制确保正确性
- **判定条件拆分**：重放写盘和上传扫描是两个独立阶段，使用不同锁机制（`PROCESSING_TABLES` vs `PROCESSING_FILES`）、不同判定条件，不可混淆
- **数据可查询性分层**：重放中内存 → 重放完成本地 → 上传完成存储 → 压实完成优化，四层边界清晰

整个设计的核心哲学是：**通过可预测的文件系统操作和明确的状态标记，在不依赖复杂分布式事务的前提下，实现数据的最终一致性和故障可恢复性。**
