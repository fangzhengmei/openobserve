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
