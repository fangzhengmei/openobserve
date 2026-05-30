# Compactor 文件清单维护与压缩协作流程

## 整体架构概览

Compactor 是 OpenObserve 中负责文件合并与压缩的核心组件，采用多阶段异步协作模式：

```
┌─────────────────────────────────────────────────────────────────┐
│                        Job 调度层                                │
│  ┌───────────┐    ┌───────────┐    ┌────────────────────────┐  │
│  │ generate  │    │ run_merge │    │ check_running_jobs     │  │
│  │   job     │    │           │    │ clean_done_jobs        │  │
│  └─────┬─────┘    └─────┬─────┘    └───────────┬────────────┘  │
└────────┼────────────────┼──────────────────────┼───────────────┘
         │                │                      │
         ▼                ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                      任务执行层                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                JobScheduler (调度器)                    │   │
│  │  - 接收 MergeJob 任务                                   │   │
│  │  - 定时更新 running job 状态                            │   │
│  │  - 调用 merge_by_stream 执行合并                        │   │
│  └──────────────────────┬──────────────────────────────────┘   │
│                         │                                      │
│                         ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                MergeWorker (工作线程池)                  │   │
│  │  - 接收 MergeBatch 批处理任务                            │   │
│  │  - 调用 merge_files 执行实际文件合并                     │   │
│  │  - 返回合并结果或错误                                    │   │
│  └──────────────────────┬──────────────────────────────────┘   │
└─────────────────────────┼──────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    文件清单持久化层                              │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐    │
│  │ batch_add   │   │ batch_process│   │ batch_add_deleted│    │
│  │ set_job_done│   │ set_job_pending │   check_running    │    │
│  └─────────────┘   └──────────────┘   └──────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. 任务生成流程 (Job Generation)

### 1.1 触发入口

**文件**: `src/job/compactor.rs:29-212`

Compactor 启动时注册多个定时任务：

```rust
// 生成当前数据合并任务 (interval 间隔)
spawn_pausable_job!("run_generate_job", interval, {
    compact::run_generate_job(CompactionJobType::Current).await
});

// 生成历史数据合并任务
spawn_pausable_job!("run_generate_old_data_job", old_data_interval, {
    compact::run_generate_job(CompactionJobType::Historical).await
});

// 执行合并 (interval + 2秒)
spawn_pausable_job!("run_merge", interval + 2, {
    compact::run_merge(scheduler.tx().clone()).await
});

// 检查超时运行任务 (job_run_timeout 间隔)
spawn_pausable_job!("compactor_check_running_jobs", job_run_timeout, {
    infra::file_list::check_running_jobs(updated_at).await
});

// 清理已完成任务 (job_clean_wait_time 间隔)
spawn_pausable_job!("compactor_clean_done_jobs", job_clean_wait_time, {
    infra::file_list::clean_done_jobs(updated_at).await
});
```

### 1.2 任务生成核心逻辑

**文件**: `src/service/compact/mod.rs:100-191`

```
run_generate_job(job_type)
    │
    ├─ 遍历所有组织 (orgs)
    │   └─ 遍历所有流类型 (ALL_STREAM_TYPES)
    │       └─ 遍历所有流 (streams)
    │           ├─ 一致性哈希: 确认当前节点负责该流
    │           ├─ 检查流是否正在删除中
    │           └─ 调用 merge::generate_job_by_stream()
    │
    └─ **偏移量管理**: service/db/compact/files.rs
        ├─ get_offset() - 读取当前压缩偏移量
        ├─ 分布式锁: /compact/merge/{org}/{stream_type}/{stream_name}
        └─ set_offset() - 更新偏移量并绑定节点
```

**文件**: `src/service/compact/merge.rs:68-172`

```
generate_job_by_stream(org_id, stream_type, stream_name)
    │
    ├─ 1. 获取压缩偏移量 (offset)
    │   ├─ 检查其他节点是否正在处理
    │   └─ 分布式锁竞争
    │
    ├─ 2. 时间窗口检查
    │   ├─ 必须等待至少 3 * max_file_retention_time
    │   └─ offset 不能是未来时间
    │
    ├─ 3. 创建合并任务
    │   └─ infra_file_list::add_job(org_id, stream_type, stream_name, offset)
    │       └─ 写入 file_list_jobs 表, status = Pending(0)
    │
    └─ 4. 更新偏移量
        └─ offset + hour_micros(1)
```

---

## 2. 任务执行流程 (Job Execution)

### 2.1 任务获取与分发

**文件**: `src/service/compact/mod.rs:272-397`

```
run_merge(job_tx)
    │
    ├─ 1. 获取待处理任务
    │   └─ infra_file_list::get_pending_jobs(node_uuid, batch_size)
    │
    ├─ 2. 任务过滤
    │   ├─ 检查数据保留时间 (跳过即将被删除的数据)
    │   ├─ 检查流是否正在删除中
    │   ├─ 每日分区流: 单节点排他执行
    │   └─ need_release_ids / need_done_ids 分类处理
    │
    ├─ 3. 运行中任务心跳
    │   └─ 后台线程每 ttl 秒调用 update_running_jobs()
    │
    └─ 4. 分发任务
        └─ job_tx.send(MergeJob) 发送给 JobScheduler
```

### 2.2 JobScheduler 调度器

**文件**: `src/service/compact/worker.rs:51-146`

```
JobScheduler::run()
    │
    ├─ 启动 N 个调度线程 (N = file_merge_thread_num)
    │
    └─ 每个线程循环:
        ├─ 接收 MergeJob
        ├─ 启动后台心跳线程 update_running_jobs()
        ├─ 调用 merge_by_stream() 执行合并
        └─ 清除流运行标记 clear_running()
```

### 2.3 merge_by_stream 核心合并流程

**文件**: `src/service/compact/merge.rs:395-642`

```
merge_by_stream(worker_tx, org_id, stream_type, stream_name, job_id, offset)
    │
    ├─ 1. 查询待合并文件
    │   └─ file_list::query_for_merge(date_start, date_end)
    │
    ├─ 2. 按分区分组
    │   └─ partition_files_with_size: HashMap<prefix, Vec<FileKey>>
    │
    ├─ 3. 并发处理每个分区 (Semaphore 控制并发数)
    │   │
    │   ├─ 3.1 文件分组策略
    │   │   ├─ MergeStrategy::FileSize - 按文件大小排序
    │   │   ├─ MergeStrategy::FileTime - 按时间排序
    │   │   └─ MergeStrategy::TimeRange - 按时间范围无重叠分组
    │   │
    │   ├─ 3.2 分批合并
    │   │   ├─ max_file_size 限制单批大小
    │   │   ├─ max_group_files 限制单批文件数
    │   │   └─ 生成 MergeBatch
    │   │
    │   ├─ 3.3 发送给工作线程
    │   │   └─ worker_tx.send((inner_tx, MergeBatch))
    │   │
    │   ├─ 3.4 接收合并结果
    │   │   ├─ 成功: (batch_id, new_files)
    │   │   └─ 失败: 记录错误, continue 下一批
    │   │
    │   └─ 3.5 事务性更新文件清单
    │       └─ write_file_list(events)
    │           ├─ 新文件: FileKey { deleted: false }
    │           └─ 旧文件: FileKey { deleted: true }
    │
    └─ 4. 标记任务完成
        └─ infra_file_list::set_job_done(&[job_id])
```

---

## 3. 文件合并工作线程 (MergeWorker)

### 3.1 工作线程池

**文件**: `src/service/compact/worker.rs:174-247`

```
MergeWorker::run()
    │
    ├─ 启动 N 个工作线程
    │
    └─ 每个线程循环:
        ├─ 接收 (MergeSender, MergeBatch)
        ├─ 调用 merge_files() 执行合并
        ├─ 成功: tx.send(Ok((batch_id, new_files)))
        └─ 失败: tx.send(Err(e))
```

### 3.2 merge_files 实际合并

**文件**: `src/service/compact/merge.rs:655-1000`

```
merge_files(thread_id, org_id, stream_type, stream_name, prefix, files)
    │
    ├─ 1. 预检查
    │   └─ 文件数 <= 1 且无需降采样 → 直接返回
    │
    ├─ 2. 缓存远程文件
    │   └─ cache_remote_files(files)
    │       ├─ 下载到本地磁盘缓存
    │       └─ 检测无效文件并从 file_list 删除
    │
    ├─ 3. 合并统计信息
    │   ├─ min_ts, max_ts
    │   ├─ total_records
    │   └─ original_size
    │
    ├─ 4. 数据融合 (DataFusion)
    │   ├─ TableBuilder 构建查询表
    │   └─ merge::merge_parquet_files()
    │       └─ 输出: 合并后的 parquet 字节数据
    │
    ├─ 5. 倒排索引生成 (可选)
    │   └─ generate_inverted_index()
    │       └─ create_tantivy_index()
    │
    └─ 6. 上传到存储
        ├─ storage::put(new_file_key, buf)
        └─ 返回 (new_files, retain_file_list)
```

---

## 4. 文件清单更新机制 (File List Update)

### 4.1 事务性写入

**文件**: `src/service/compact/merge.rs:1035-1106`

```
write_file_list(org_id, stream_type, events)
    │
    ├─ 1. 构造删除列表
    │   └─ del_items: Vec<FileListDeleted>
    │
    ├─ 2. 重试 5 次事务更新
    │   ├─ Step 1: infra_file_list::batch_process(events)
    │   │   ├─ deleted=false → batch_add (新增文件记录)
    │   │   └─ deleted=true → batch_remove (标记删除)
    │   │
    │   └─ Step 2: infra_file_list::batch_add_deleted(del_items)
    │       └─ 写入 file_list_deleted 表 (延迟删除用)
    │
    ├─ 3. Filelist 类型流特殊处理
    │   └─ dump::handle_dump_stats_on_merge()
    │
    └─ 4. 广播更新 (缓存同步)
        └─ db::file_list::broadcast::send(events)
```

### 4.2 数据库层面实现

**文件**: `src/infra/src/file_list/postgres.rs:1401-1550`

| 函数 | 作用 | 状态变更 |
|------|------|---------|
| `add_job()` | 创建合并任务 | status = Pending(0) |
| `get_pending_jobs()` | 获取待处理任务 | status = Running(1), node = 当前节点 |
| `set_job_pending()` | 释放任务 | status = Pending(0), node = '' |
| `set_job_done()` | 标记任务完成 | status = Done(2) |
| `update_running_jobs()` | 更新任务心跳 | updated_at = now |
| `check_running_jobs()` | 检查超时任务 | updated_at < before_date → status = Pending |
| `clean_done_jobs()` | 清理完成任务 | status = Done → 删除记录 |

---

## 5. 失败处理与回滚机制 (Failure Handling)

### 5.1 失败场景分类

| 失败阶段 | 影响范围 | 回滚策略 |
|---------|---------|---------|
| **任务生成阶段** | 单个流 | 分布式锁自动释放, offset 不更新 |
| **merge_files 合并** | 单个批次 | 不上传新文件, 不更新清单 |
| **write_file_list** | 单个批次 | 重试 5 次, 失败则批次跳过 |
| **任务执行超时** | 整个任务 | check_running_jobs 重置为 Pending |

### 5.2 超时任务回滚流程

**文件**: `src/job/compactor.rs:141-153`

```
check_running_jobs 定时任务
    │
    ├─ 计算超时时间: updated_at < now - job_run_timeout
    │
    └─ infra_file_list::check_running_jobs(before_date)
        └─ SQL: UPDATE file_list_jobs
             SET status = 0, node = ''
             WHERE status = 1 AND updated_at < before_date
```

**设计意图**:
- 防止节点崩溃导致任务永远卡在 Running 状态
- 超时后其他节点可以重新 pickup 该任务
- job_run_timeout / 4 作为心跳间隔 (安全系数 4x)

### 5.3 批次失败隔离

**文件**: `src/service/compact/merge.rs:572-616`

```rust
for ret in worker_results {
    match ret {
        Ok((batch_id, new_files)) => {
            // 成功批次: 更新清单
            write_file_list(events).await;
        }
        Err(e) => {
            // 失败批次: 仅记录日志, 不影响其他批次
            log::error!("[COMPACTOR] merge files failed: {e}");
            last_error = Some(e);
            continue;
        }
    }
}
```

**关键特性**:
- 批次间相互隔离
- 单个批次失败不影响同流的其他批次
- 失败批次的源文件保持不变, 下次合并可重试

### 5.4 延迟删除机制

**文件**: `src/service/compact/mod.rs:399-446`

```
run_delay_deletion()
    │
    ├─ 1. 查询 file_list_deleted 表
    │   └─ created_at < now - delete_files_delay_hours
    │
    ├─ 2. 从存储删除物理文件
    │   └─ storage::del()
    │
    └─ 3. 从 file_list_deleted 移除记录
```

**安全设计**:
- 延迟 2 小时以上才真正删除文件
- 防止清单更新与存储删除的竞态条件
- 给系统留出足够时间完成一致性同步

---

## 6. 关键数据结构

### 6.1 MergeJob (任务级)

```rust
pub struct MergeJob {
    pub org_id: String,
    pub stream_type: StreamType,
    pub stream_name: String,
    pub job_id: i64,      // file_list_jobs.id
    pub offset: i64,      // 时间窗口微秒
}
```

### 6.2 MergeBatch (批次级)

```rust
pub struct MergeBatch {
    pub batch_id: usize,
    pub org_id: String,
    pub stream_type: StreamType,
    pub stream_name: String,
    pub prefix: String,   // 分区路径
    pub files: Vec<FileKey>,
}
```

### 6.3 FileListJobStatus

```rust
pub enum FileListJobStatus {
    Pending = 0,  // 等待执行
    Running = 1,  // 执行中
    Done = 2,     // 已完成
}
```

---

## 7. 核心流程时序图

```
  调度线程         JobScheduler        MergeWorker        DB/Storage
    │                  │                  │                  │
    │  get_pending_jobs│                  │                  │
    ├───────────────────────────────────────────────────────►│
    │                  │                  │                  │
    │  send(MergeJob)  │                  │                  │
    ├────────────────► │                  │                  │
    │                  │                  │                  │
    │                  │ merge_by_stream  │                  │
    │                  ├───────────────► │                  │
    │                  │                  │                  │
    │                  │ send(MergeBatch) │                  │
    │                  ├────────────────► │                  │
    │                  │                  │                  │
    │                  │                  │ merge_files()    │
    │                  │                  │ ├─ download      │
    │                  │                  │ ├─ merge parquet │
    │                  │                  │ ├─ index build   │
    │                  │                  │ └─ upload        │
    │                  │                  │                  │
    │                  │  Result(batch)   │                  │
    │                  │ ◄────────────────┤                  │
    │                  │                  │                  │
    │                  │ write_file_list  │                  │
    │                  │ ├─ batch_process ──────────────────►│
    │                  │ └─ add_deleted   ──────────────────►│
    │                  │                  │                  │
    │                  │ set_job_done()   │                  │
    │                  └────────────────────────────────────►│
    │                  │                  │                  │
```

---

## 8. 关键设计要点

### 8.1 分布式协调

- **一致性哈希**: 流级别任务分发, 避免多节点冲突
- **分布式锁**: `generate_job_by_stream` 阶段节点竞争
- **心跳机制**: Running 状态定时更新, 超时自动回滚

### 8.2 容错设计

- **批次隔离**: 单批次失败不影响全局
- **重试机制**: 文件清单写入重试 5 次
- **延迟删除**: 物理文件延迟删除, 防止数据丢失
- **幂等性**: 重复合并不会产生数据不一致

### 8.3 性能优化

- **线程池分层**: JobScheduler + MergeWorker 两级线程池
- **并发控制**: Semaphore 限制分区并发数
- **批量处理**: 按文件大小/数量分组, 平衡合并粒度
- **本地缓存**: 磁盘缓存远程文件, 减少重复下载

### 8.4 事务一致性

- **清单更新原子性**: batch_process 单批次事务
- **最终一致性**: 延迟删除 + 广播同步
- **状态机明确**: Pending → Running → Done 三态流转

---

## 9. 任务分流机制：need_release_ids 与 need_done_ids

`run_merge`（`src/service/compact/mod.rs:272-397`）从数据库批量获取 Pending 任务后，并不直接全部执行，而是先做一轮分流，将任务归入三个去向之一：

```
get_pending_jobs() 返回的每个 job
        │
        ├──────────────────────────────────────────────────────┐
        │                                                      │
        ▼                                                      ▼
  ┌─────────────┐    ┌──────────────────┐    ┌────────────────────────┐
  │ merge_jobs  │    │ need_done_ids    │    │ need_release_ids       │
  │ (进入执行)  │    │ (直接标记完成)    │    │ (释放回 Pending)       │
  └─────────────┘    └──────────────────┘    └────────────────────────┘
        │                    │                          │
        ▼                    ▼                          ▼
  job_tx.send()       set_job_done()            set_job_pending()
  → JobScheduler      → status = Done(2)        → status = Pending(0)
  → 实际合并           → node = ''               → node = ''
```

### 9.1 need_done_ids：直接标记完成的条件

当 job 满足以下**任一**条件时，无需真正合并，直接标记为 Done：

| 条件 | 代码位置 | 含义 |
|------|---------|------|
| `job.offsets <= stream_data_retention_end` | `mod.rs:307-309` | 该时间窗口的数据已被或即将被 retention 删除，合并无意义 |
| `is_deleting_stream()` | `mod.rs:312-314` | 流正在执行全量/按日期删除，合并会与删除冲突 |
| `files.is_empty()` | `merge.rs:444-449` | 该时间窗口无文件可合并（已在 merge_by_stream 内处理） |
| `schema == Schema::empty()` | `merge.rs:408-413` | 流已被删除 |

**set_job_done 的 SQL 行为**（`postgres.rs:1442-1464`）：

```sql
UPDATE file_list_jobs
SET status = 2,           -- Done
    updated_at = now_micros,
    dumped = ~file_list_dump_enabled,  -- dump 未启用则 true（可被清理）
    node = ''
WHERE id IN (...);
```

**对后续重试的影响**：
- `set_job_done` 是**终态操作**，job 进入 Done(2) 后不会被任何自动机制重新变为 Pending
- 因此 `need_done_ids` 的判断**必须保守**：只有确认该时间窗口的数据确实无需合并时才能放入
- 如果误判（例如流后来取消了删除状态），该时间窗口将**永远不会**被重新处理，因为 offset 已前移（见第 11 节分析）

### 9.2 need_release_ids：释放回 Pending 的条件

当 job 满足以下**任一**条件时，需要释放回 Pending，让其他节点或其他轮次拾取：

| 条件 | 代码位置 | 含义 |
|------|---------|------|
| Daily 分区流 + 当前节点不是一致性哈希目标节点 | `mod.rs:316-326` | 该流应由其他节点处理 |
| Daily 分区流 + `is_running(stream)` 为 true | `mod.rs:329-331` | 同流已有另一个 job 正在执行，避免并发冲突 |
| `merge_by_stream` 内 `merge_files` 合并失败后 job 超时 | 由 `check_running_jobs` 兜底 | 节点崩溃或严重超时 |

**set_job_pending 的 SQL 行为**（`postgres.rs:1401-1440`）：

```sql
UPDATE file_list_jobs
SET status = 0           -- Pending
WHERE id IN (...);
```

注意：`set_job_pending` **不清除 node 字段**（与 `check_running_jobs` 不同），因为 `get_pending_jobs` 在分配 Running 状态时会重新设置 node。

**对后续重试的影响**：
- 释放回 Pending 后，下一轮 `get_pending_jobs` 可再次拾取
- `get_pending_jobs` 采用 `GROUP BY stream, ORDER BY num DESC`（`postgres.rs:1323-1334`），按每个流的任务堆积量排序拾取，被释放的 job 会在后续轮次中被重新调度
- **关键**：释放操作本身如果失败（`set_job_pending` 返回 Err），该 job 会**卡在 Running 状态**，直到 `check_running_jobs` 超时兜底

### 9.3 Daily 分区流的排他机制

`src/service/db/compact/stream.rs` 实现了进程内的流级排他锁：

```rust
static STREAMS: Lazy<RwHashSet<String>> = Lazy::new(Default::default);

pub fn is_running(stream: &str) -> bool { STREAMS.contains(stream) }
pub fn set_running(stream: &str)       { STREAMS.insert(stream.to_string()); }
pub fn clear_running(stream: &str)     { STREAMS.remove(stream); }
```

仅在 `partition_time_level == PartitionTimeLevel::Daily` 时启用：

```
Daily 分区流 job 到达 run_merge
    │
    ├─ 一致性哈希检查 → 不是本节点 → need_release_ids
    │
    ├─ is_running() → true → need_release_ids (同流并发保护)
    │
    └─ is_running() → false → set_running() → merge_jobs
                                                     │
                                    merge_by_stream 结束后 → clear_running()
                                    (在 worker.rs:138 中)
```

**设计意图**：Daily 分区流一次处理整个天（00-23时），合并量大耗时长，必须防止同一流上的多个 job 并发执行导致文件清单冲突。Hourly 分区流每次只处理一小时窗口，天然不会有跨 job 重叠，因此无需此排他保护。

### 9.4 三条分流路径的状态迁移对比

```
                    ┌─────────────────────────────────────────────┐
                    │          get_pending_jobs 拾取后            │
                    │          status = Running(1)                │
                    │          node = current_node                │
                    └─────────────┬───────────────────────────────┘
                                  │
              ┌───────────────────┼───────────────────────┐
              ▼                   ▼                       ▼
      need_done_ids         merge_jobs            need_release_ids
              │                   │                       │
              ▼                   ▼                       ▼
       set_job_done()      执行合并逻辑           set_job_pending()
       status = Done(2)    ┌────┴────┐           status = Pending(0)
       node = ''           │成功     │失败         node 不变
       dumped = ~dump_en   │         │             (node 仍为当前节点
       更新 updated_at     ▼         ▼              但 Pending 状态下
       ★ 终态，不可重入   set_job   job 超时后    get_pending_jobs 会
                          _done()   check_running  重新设置 node)
                          同左     _jobs 兜底
                                   status → Pending
                                   node → ''
```

| 维度 | set_job_done | set_job_pending |
|------|-------------|----------------|
| 目标状态 | Done(2) | Pending(0) |
| 是否可重入 | **否** — 终态 | **是** — 可被再次拾取 |
| node 处理 | 清空 `node = ''` | **不清空** node |
| dumped 字段 | 设置 `dumped = ~file_list_dump_enabled` | 不修改 |
| updated_at | 更新为 now | 不修改 |
| 使用场景 | 数据无需合并/已被删除 | 节点不匹配/流级排他 |

---

## 10. set_job_pending 与 set_job_done 对后续重试的影响

### 10.1 完整的状态生命周期

```
                 add_job()
                    │
                    ▼
              ┌──────────┐
              │ Pending  │ ◄─────────────────────────────────────┐
              │  (0)     │                                       │
              └────┬─────┘                                       │
                   │ get_pending_jobs()                          │
                   │ (事务内: status→Running, node→current)      │
                   ▼                                             │
              ┌──────────┐                                       │
              │ Running  │──── 超时 ──── check_running_jobs ────┘
              │  (1)     │                    (status→Pending,
              └────┬─────┘                     node→'')
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
   ┌────────┐ ┌────────┐ ┌──────────────────┐
   │ 成功   │ │ 释放   │ │ 标记完成          │
   │        │ │        │ │ (need_done_ids)  │
   ▼        ▼ ▼        │ ▼                  │
set_job    set_job    set_job_done          │
_done()    _pending() │                     │
   │          │       │                     │
   ▼          ▼       ▼                     │
┌────────┐ ┌────────┐ ┌──────────┐          │
│ Done   │ │Pending │ │  Done    │          │
│ (2)    │ │ (0)    │ │  (2)     │          │
│dumped  │ │可重试  │ │ dumped   │          │
│=true*  │ └───┬───┘ │ =~dump   │          │
│终态    │     │     │ 终态     │          │
└────────┘     │     └──────────┘          │
               │                            │
               └──── 下轮 get_pending_jobs ─┘
                    重新拾取执行
```

*注：`dumped = true` 的条件是 `!cfg.compact.file_list_dump_enabled`，即 dump 功能未启用时直接标记为可清理。

### 10.2 重试路径分析

**路径 A：正常成功**
```
Pending → Running → set_job_done → Done (终态)
```
job 永久结束，后续 `clean_done_jobs` 会清理。

**路径 B：节点不匹配/流级排他（need_release_ids）**
```
Pending → Running → set_job_pending → Pending → (下一轮) → Running → ...
```
job 被释放回队列。由于 `set_job_pending` 不清空 node，但 `get_pending_jobs` 会在分配时重新设置 node，所以不影响后续拾取。下一轮调度中，正确的节点会拾取此 job。

**路径 C：执行超时/节点崩溃**
```
Pending → Running → (节点崩溃/心跳中断) → check_running_jobs → Pending → ...
```
这是最关键的安全网。`check_running_jobs` 不仅重置 status，还会清空 node，确保其他节点可以立即拾取。

**路径 D：直接标记完成（need_done_ids）**
```
Pending → Running → set_job_done → Done (终态)
```
job 不再重试。**这是不可逆的**——如果判断错误，该时间窗口的数据将永远不会被合并。

### 10.3 set_job_pending 的隐藏风险

`set_job_pending` 在 `run_merge` 中被调用时（`mod.rs:346-350`），如果操作本身失败：

```rust
if let Err(e) = infra_file_list::set_job_pending(&need_release_ids, 0, None).await {
    log::error!("[COMPACTOR] set_job_pending failed: {e}");
}
```

后果是：这些 job 仍然处于 Running(1) 状态，绑定在当前节点上。它们不会被任何正常流程重新拾取，只能等待 `check_running_jobs` 超时兜底。这引入了一个**最长等待窗口 = job_run_timeout** 的延迟。

---

## 11. Offset 前移与时间窗口漏处理防护

### 11.1 offset 前移的时机

`generate_job_by_stream`（`merge.rs:153-169`）中，offset 前移发生在 **add_job 成功之后**：

```rust
// 步骤 1: 创建合并任务 (offset 对应的时间窗口)
infra_file_list::add_job(org_id, stream_type, stream_name, offset).await?;

// 步骤 2: 立即前移 offset (无论合并是否成功)
let offset = offset + hour_micros(1);
db::compact::files::set_offset(
    org_id, stream_type, stream_name,
    offset, Some(&LOCAL_NODE.uuid.clone()),
).await?;
```

这意味着：**offset 的前移和 job 的执行是解耦的**。add_job 成功 → offset 立即前移 → 下一轮 generate_job 会从新的 offset 开始。

### 11.2 offset 前移后失败的场景分析

考虑以下时序：

```
时间轴:
  T0: offset = 10:00
  T1: generate_job_by_stream → add_job(offset=10:00) → 成功
  T2: set_offset(offset = 11:00) → 成功
  T3: 下一轮 generate_job → add_job(offset=11:00) → 成功
  T4: set_offset(offset = 12:00) → 成功
  T5: 10:00 的 job 执行失败
```

此时 offset 已经推进到 12:00，但 10:00 的数据**没有被合并**。是否会造成漏处理？

**答案：不会**，原因如下：

### 11.3 防漏处理机制：job 记录与 offset 独立

#### 机制一：job 记录独立于 offset

`add_job` 在 `file_list_jobs` 表中创建了一条**独立的 job 记录**（`postgres.rs:1227-1299`）：

```sql
INSERT INTO file_list_jobs (org, stream, offsets, status, node, started_at, updated_at)
VALUES ($1, $2, $3, 0, '', 0, 0)
ON CONFLICT DO NOTHING;
```

关键约束：`ON CONFLICT DO NOTHING` + `(org, stream, offsets)` 唯一索引。

这意味着：
- **offset 前移不会删除已有的 job 记录**
- 即使 offset 已推进到 12:00，10:00 的 job 记录仍然存在于 `file_list_jobs` 表中
- 只要 job 未被 `set_job_done`，它就会一直等待被执行

#### 机制二：add_job 的去重与复活

`add_job` 的实现中有一个**关键的复活逻辑**（`postgres.rs:1277-1293`）：

```rust
let status = ret.try_get::<i64, &str>("status").unwrap_or_default();
if id > 0
    && FileListJobStatus::from(status) == FileListJobStatus::Done
    // 如果该 offset 的 job 已经是 Done 状态，则复活为 Pending
{
    sqlx::query("UPDATE file_list_jobs SET status = $1 WHERE status = $2 AND id = $3;")
        .bind(FileListJobStatus::Pending)
        .bind(FileListJobStatus::Done)
        .bind(id)
        .execute(&mut *tx)
        .await?;
}
```

这意味着：即使某个 offset 的 job 被标记为 Done（例如通过 `need_done_ids`），如果后续 `generate_old_data_job_by_stream` 发现该小时仍有旧数据需要合并，`add_job` 会**将 Done 状态复活为 Pending**，重新触发合并。

#### 机制三：历史数据扫描覆盖

`generate_old_data_job_by_stream`（`merge.rs:178-272`）专门处理 offset 前移后可能遗漏的历史窗口：

```
generate_old_data_job_by_stream
    │
    ├─ 计算 end_time = offset - old_data_min_hours
    ├─ 计算 start_time = end_time - data_retention_days
    │
    ├─ 查询该时间范围内所有"有旧数据的小时"
    │   └─ infra_file_list::query_old_data_hours(start_time, end_time)
    │
    └─ 为每个有数据的小时创建 job
        └─ add_job(org_id, stream_type, stream_name, hour_offset)
           └─ ON CONFLICT DO NOTHING (已存在则跳过)
              └─ 如果已存在且为 Done → 复活为 Pending
```

**注意**：`generate_old_data_job_by_stream` **不前移 offset**，它只是扫描历史范围内的小时，为有数据但可能未合并的小时补建 job。

### 11.4 完整的防漏处理保障链

```
                offset 前移后 job 执行失败
                        │
         ┌──────────────┼───────────────────┐
         ▼              ▼                   ▼
   job 仍在 DB 中    check_running_jobs   generate_old_data
   (status=Running)  超时后重置为         _job_by_stream
         │           Pending              定期扫描历史窗口
         │              │                   │
         │              ▼                   ▼
         │         下一轮 run_merge      发现遗漏的小时
         │         重新拾取执行          add_job 复活 Done→Pending
         │              │                   │
         ▼              ▼                   ▼
    ┌─────────────────────────────────────────────┐
    │         最终: 所有时间窗口都会被处理          │
    └─────────────────────────────────────────────┘
```

### 11.5 offset 前移的合理性解释

offset 前移的设计看似激进（不等合并完成就推进），实则有充分理由：

1. **解耦生成与执行**：generate_job 只负责"发现需要合并的时间窗口"，不负责执行。前移 offset 可以让下一轮继续发现新窗口，不会因单个窗口执行缓慢而阻塞整个流的推进。

2. **job 记录是真实的工作单元**：offset 只是"已扫描到哪里"的进度标记，真正的合并承诺由 `file_list_jobs` 表中的记录承载。

3. **多重兜底**：
   - `check_running_jobs`：超时回滚 → 可重试
   - `generate_old_data_job_by_stream`：定期扫描 → 补建遗漏 job
   - `add_job` 的 Done→Pending 复活：已完成的窗口如果仍有旧数据 → 重新激活

4. **唯一真正的不可逆场景**：只有当 `need_done_ids` 误判（例如流正在删除但后来取消了）+ offset 已前移 + 历史扫描未发现该小时有旧数据时，才可能漏处理。但这在实践中极低概率，因为：
   - `is_deleting_stream` 是用户显式触发的操作
   - `generate_old_data_job_by_stream` 的扫描范围覆盖整个数据保留期
   - 只要该小时仍有小文件，`query_old_data_hours` 就会返回该小时

### 11.6 get_pending_jobs 的拾取策略

`get_pending_jobs`（`postgres.rs:1301-1399`）不是简单的 FIFO，而是采用了**流公平调度**：

```sql
SELECT stream, max(id) as id, COUNT(*) AS num
FROM file_list_jobs
WHERE status = 0        -- Pending
GROUP BY stream
ORDER BY num DESC       -- 堆积最多的流优先
LIMIT $2;
```

然后在同一事务内将这些 job 的 status 更新为 Running：

```sql
UPDATE file_list_jobs
SET status = 1, node = $2, started_at = $3, updated_at = $4
WHERE id IN (...);
```

**对重试的影响**：
- 被释放回 Pending 的 job 会累积在其流的计数中
- 如果某个流积累了大量待处理 job（例如反复失败导致的堆积），它会被优先调度
- 每次只取每个流中 `max(id)` 对应的一条 job（不是全部），避免单个流占满所有工作线程
