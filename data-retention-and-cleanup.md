# OpenObserve 数据保留策略与后台清理任务协同工作分析

> 基于源码 `src/service/compact/`、`src/job/compactor.rs`、`src/config/src/config.rs`、`src/config/src/metrics.rs` 等模块梳理。

---

## 1. 策略维护层级

数据保留策略由三层配置协同决定，优先级从高到低：

| 层级 | 配置项 | 来源文件 | 默认值 | 说明 |
|------|--------|----------|--------|------|
| **流级** | `StreamSettings.data_retention` | `src/config/src/meta/stream.rs:924` | `0`（未设置） | 针对单个 Stream 的保留天数，`>0` 时覆盖全局配置 |
| **流级扩展** | `StreamSettings.extended_retention_days` | `src/config/src/meta/stream.rs:940` | `[]` | 一组 `TimeRange`，定义"红色日期"——即使全局/流级保留要求删除，该时间段内的数据仍保留 |
| **全局** | `Compact.data_retention_days` | `src/config/src/config.rs:1904` | `3650` | 全局默认数据保留天数 |
| **全局扩展** | `Compact.extended_data_retention_days` | `src/config/src/config.rs:1900` | `3650` | 扩展保留的回溯天数上限，决定 `last_retained_time` 的计算基准 |
| **时间窗口** | `Compact.retention_allowed_hours` | `src/config/src/config.rs:1955-1959` | `""`（无限制） | 逗号分隔的小时列表，仅在指定小时允许运行 retention |

### 保留截止时间计算逻辑

```
lifecycle_end = now - Duration::days(data_retention_days)   // 全局
stream_data_retention_end = now - Duration::days(stream_settings.data_retention)  // 流级优先
last_retained_time = lifecycle_end - Duration::days(extended_data_retention_days) // 扩展保留回溯边界
```

源码位置：`src/service/compact/retention.rs:40-118`（`generate_jobs`）及 `src/service/compact/mod.rs:300-306`（`run_merge` 中的同类判断）。

### 扩展保留范围（Extended Retention / "红色日期"）

`generate_time_ranges_for_deletion` 函数（`retention.rs:132-224`）负责将扩展保留区间从原始删除时间范围中"挖掉"：

1. 先用 `TimeRange::flatten_overlapping_ranges` 合并重叠的扩展保留区间
2. 过滤掉超出 `original_time_range` 或早于 `last_retained_time` 的区间
3. 依次用扩展保留区间对原始范围执行 `split_by_range`，产生不重叠的待删除片段
4. 最终得到一组有序、无重叠的删除时间范围

---

## 2. 清理任务粒度推进方式

### 2.1 任务调度总览

Compactor 节点启动后通过 `src/job/compactor.rs:run()` 注册以下定时任务：

| 任务名 | 调度间隔 | 功能 | 源码位置 |
|--------|----------|------|----------|
| `run_generate_job` | `compact.interval`（默认 10s） | 为当前数据生成 merge job | `compactor.rs:51-56` |
| `run_generate_old_data_job` | `old_data_interval + 1`（默认 3601s） | 为旧数据生成 merge job | `compactor.rs:58-67` |
| `run_merge` | `interval + 2` | 执行 merge 作业 | `compactor.rs:88-93` |
| `run_retention` | `interval + 3` | 生成并执行 retention 删除 | `compactor.rs:95-100` |
| `run_delay_deletion` | `interval + 4` | 延迟物理删除存储文件 | `compactor.rs:102-107` |
| `compactor_sync_to_db` | `sync_to_db_interval`（默认 600s） | 将缓存的 offset 同步到 DB | `compactor.rs:109-118` |
| `compactor_check_running_jobs` | `job_run_timeout`（默认 600s） | 回收超时作业 | `compactor.rs:141-153` |
| `compactor_clean_done_jobs` | `job_clean_wait_time`（默认 7200s） | 清理已完成作业 | `compactor.rs:155-167` |
| `run_compactor_pending_jobs_metric` | `pending_jobs_metric_interval`（默认 300s） | 上报待处理作业指标 | `compactor.rs:169-207` |

### 2.2 Retention 作业推进（天粒度）

`run_retention` 的执行分两步：

**步骤一：`retention::generate_jobs()`**（`retention.rs:40-118`）
- 遍历所有 org → stream_type → stream
- 通过一致性哈希确定该 stream 应由哪个 compactor 节点处理
- 跳过 `EnrichmentTables` 和 `Filelist` 类型
- 检查 `retention_allowed_hours` 时间窗口
- 计算保留截止时间，调用 `generate_retention_job`

**步骤二：`generate_retention_job()` → `delete_by_date()`**（`retention.rs:227-347`、`398-520`）
- 查询 file_list 中的 `min_date`，确定数据的最早时间
- 如果设置了扩展保留区间，通过 `generate_time_ranges_for_deletion` 计算最终删除范围
- **以"天"为粒度逐步推进**：`while start < time_range.end { start += day_micros(1) }`
- 每天调用 `db::compact::retention::delete_stream` 创建一个删除 job

**步骤三：执行删除 job**（`mod.rs:46-98` 的 `run_retention`）
- 从 `db::compact::retention::list()` 获取所有待处理 job
- 再次通过一致性哈希确定执行节点
- 调用 `delete_all` 或 `delete_by_date` 完成实际删除

### 2.3 Merge 作业推进（小时粒度）

**当前数据**：`generate_job_by_stream`（`merge.rs:72-172`）
- 获取 stream 的 compaction offset（上次压缩到的位置）
- 通过分布式锁确保同一 stream 只有一个节点处理
- offset 对齐到小时，且必须满足"至少 3 × max_file_retention_time"的冷却期
- 调用 `infra_file_list::add_job` 创建作业，offset 推进一小时

**历史数据**：`generate_old_data_job_by_stream`（`merge.rs:178-272`）
- 查询 `old_data_min_hours`（默认 2h）之前的、仍存在小文件的小时
- 为每个小时创建 merge job

**作业执行**：`run_merge`（`mod.rs:273-397`）
- 从 `infra_file_list::get_pending_jobs` 获取待处理作业
- 检查作业 offset 是否在保留期内（避免合并即将被删除的数据）
- 检查 `is_deleting_stream` 跳过正在删除的 stream
- 通过 `JobScheduler` → `MergeWorker` 两级线程池执行
- 启动后台协程以 `job_run_timeout / 4` 的间隔更新作业心跳

### 2.4 延迟删除（物理文件清理）

`run_delay_deletion`（`mod.rs:399-446`）：
- 查询 `file_list_deleted` 表中 `created_at` 早于 `delete_files_delay_hours`（默认 2 小时）的记录
- 以 `BATCH_SIZE = 10000` 分批处理
- 依次删除：存储上的 parquet 文件 → 倒排索引 puffin 文件 → flattened 文件 → `file_list_deleted` 表记录
- 删除完成后更新 org 级 offset

### 2.5 Flatten Compactor

独立的 `FlattenCompactor` 角色（`src/job/flatten_compactor.rs`）：
- 仅处理 `Logs` 类型且配置了 `defined_schema_fields` 的 stream
- 将 `_all` 字段中的 JSON 拆解为独立列，生成新的 parquet 文件

---

## 3. 删除操作对查询的影响规避措施

### 3.1 两阶段删除：逻辑删除 → 延迟物理删除

这是最核心的规避机制：

1. **逻辑删除阶段**（`retention.rs:522-568` `delete_from_file_list`）
   - 将文件标记为 `deleted = true`，写入 file_list 表
   - 同时写入 `file_list_deleted` 表，记录待物理删除的文件信息
   - 此时文件在存储上仍然存在，查询引擎通过 file_list 中的 `deleted` 标记自动跳过

2. **物理删除阶段**（`deleted.rs:25-139` + `mod.rs:399-446`）
   - 仅在 `delete_files_delay_hours`（默认 2 小时）后才执行
   - 保证正在进行的查询仍有时间读取旧文件

源码关键注释（`mod.rs:400-401`）：
> 1. get pending deleted files from file_list_deleted table, created_at > 2 hours
> 2. delete files from storage

### 3.2 file_list_deleted_mode 配置

`Compact.file_list_deleted_mode`（`config.rs:1916-1917`）控制删除记录的处理方式：

| 模式 | 行为 |
|------|------|
| `"deleted"` | 默认。写入 `file_list_deleted` 表，延迟物理删除存储文件 |
| `"history"` | 仅将文件信息写入历史表（`batch_add_history`），不实际删除 |
| `"none"` | 不做额外处理（但仍执行逻辑删除） |

### 3.3 Retention 与 Merge 的互斥保护

1. **`is_deleting_stream` 检查**：
   - `run_generate_job` 中（`mod.rs:142-155`）：如果 stream 正在执行删除，跳过 merge job 生成
   - `run_merge` 中（`mod.rs:311-315`）：如果 stream 正在删除，将该 merge job 标记为 done 而非执行
   - `dump::run` 中（`dump.rs:107-109`）：同理跳过

2. **Merge 跳过保留期外的数据**（`mod.rs:302-309`）：
   - 如果 merge job 的 offset 早于 `stream_data_retention_end`，直接将 job 标记为 done
   - 避免对即将被 retention 删除的数据执行无意义的合并

3. **流级运行锁**（`db/compact/stream.rs:20-32`）：
   - `is_running` / `set_running` / `clear_running` 通过内存 HashSet 防止同一 stream 同时执行多个 merge job
   - 仅对 `PartitionTimeLevel::Daily` 的 stream 启用（避免跨节点冲突）

4. **分布式锁**（`merge.rs:84-105`）：
   - merge job 生成前通过 `dist_lock::lock` 获取 `/compact/merge/{org}/{type}/{stream}` 锁
   - 防止多节点同时为同一 stream 生成作业

### 3.4 节点分配与一致性哈希

所有任务（retention、merge、flatten）均通过 `get_node_from_consistent_hash` 确定处理节点，确保：
- 同一 stream 的 retention 和 merge 不会分散到不同节点导致冲突
- 节点变更时自动释放旧节点持有的 offset（`mod.rs:120-137`）

### 3.5 作业心跳与超时回收

- **心跳更新**：作业执行期间，后台协程以 `max(60, job_run_timeout / 4)` 秒为间隔更新 `updated_at`（`mod.rs:367-383`）
- **超时检测**：`compactor_check_running_jobs` 定时任务将超过 `job_run_timeout` 未更新的作业重置为 pending（`compactor.rs:141-153`），允许其他节点重新拾取
- **已完结清理**：`compactor_clean_done_jobs` 清理完成超过 `job_clean_wait_time` 的作业记录

---

## 4. 失败重试机制

### 4.1 文件列表写入重试

`retention.rs:571-636`（`write_file_list`）和 `merge.rs:1057-1078`（`write_file_list`）均采用 **5 次重试 + 1 秒间隔** 的策略：

```rust
for _ in 0..5 {
    if let Err(e) = infra_file_list::batch_process(&events).await {
        log::error!("[COMPACTOR] batch_delete to db failed, retrying: {e}");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        continue;
    }
    // ...
    success = true;
    break;
}
if !success {
    return Err(anyhow::anyhow!("[COMPACTOR] batch_write to db failed"));
}
```

重试涵盖两个步骤：
1. `batch_process`：从 file_list 表中逻辑删除旧文件 / 写入新文件
2. `batch_add_deleted`：写入 `file_list_deleted` 表

两步必须都成功才算完成；如果 `batch_add_deleted` 失败，会从 `batch_process` 重新开始（幂等保证）。

### 4.2 Dump 相关写入重试

`dump.rs:476-494`（`delete_by_time_range`）同样采用 5 次重试模式，处理 `batch_process` 和 `batch_add_deleted`。

### 4.3 Merge 作业层面

- **作业级重试**：如果 merge 执行失败（`merge_by_stream` 返回 Err），作业不会被标记为 done，下一轮 `run_merge` 会重新拾取
- **文件级容错**：`merge.rs:574-616` 中，单个 batch 合并失败不会中断整个 stream 的处理，错误被记录后继续处理其他 batch
- **缓存下载容错**：`cache_remote_files`（`merge.rs:1188-1277`）中，如果文件下载失败且错误为 "not found"，自动从 file_list 中删除该无效记录

### 4.4 延迟删除容错

`deleted.rs:46-52`：物理删除存储文件时，如果返回 "not found" 错误，视为文件已被删除，不阻塞流程。

### 4.5 Retention Job 缓存去重

`db/compact/retention.rs:55-59`：同一删除任务在 1 小时内不会重复创建：
```rust
if v.value() + hour_micros(1) > now_micros() {
    return Ok((db_key, false)); // already in cache
}
```

---

## 5. 可观测指标的暴露情况

### 5.1 Compactor 相关 Prometheus 指标

所有指标前缀为 `zo_`（namespace = `zo`），定义在 `src/config/src/metrics.rs`：

| 指标名 | 类型 | 标签 | 说明 | 源码位置 |
|--------|------|------|------|----------|
| `zo_compact_used_time` | Counter | `organization`, `stream_type` | Compactor 累计耗时（秒） | `metrics.rs:502-513` |
| `zo_compact_merged_files` | IntCounter | `organization`, `stream_type` | 合并的文件总数 | `metrics.rs:514-525` |
| `zo_compact_merged_bytes` | IntCounter | `organization`, `stream_type` | 合并的原始字节数 | `metrics.rs:526-537` |
| `zo_compact_pending_jobs` | IntGauge | `organization`, `stream_type` | 当前待处理作业数 | `metrics.rs:538-549` |

### 5.2 Stream Stats 聚合指标

| 指标名 | 类型 | 标签 | 说明 | 源码位置 |
|--------|------|------|------|----------|
| `zo_stream_stats_scan_duration_seconds` | Histogram | `organization`, `stream_type`, `scan_type` | Stats 聚合扫描耗时 | `metrics.rs:552-564` |
| `zo_stream_stats_scan_total` | IntCounter | `organization`, `stream_type`, `scan_type` | Stats 聚合扫描次数 | `metrics.rs:566-577` |
| `zo_stream_stats_scan_errors_total` | IntCounter | `organization`, `stream_type`, `scan_type` | Stats 聚合错误次数 | `metrics.rs:579-590` |
| `zo_stream_stats_streams_total` | IntGauge | — | 被追踪的 Stream 总数 | `metrics.rs:592-603` |
| `zo_stream_stats_last_scan_timestamp` | IntGauge | — | 最近一次聚合扫描的 Unix 时间戳（微秒） | `metrics.rs:605-617` |

### 5.3 存储相关指标

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `zo_storage_original_bytes` | IntGauge | `organization`, `stream_type` | 存储原始字节数 |
| `zo_storage_compressed_bytes` | IntGauge | `organization`, `stream_type` | 存储压缩字节数 |
| `zo_storage_files` | IntGauge | `organization`, `stream_type` | 存储文件数 |
| `zo_storage_records` | IntGauge | `organization`, `stream_type` | 存储记录数 |

### 5.4 日志可观测性

代码中使用 `[COMPACTOR]`、`[COMPACTOR::JOB]`、`[COMPACTOR:WORKER]`、`[COMPACTOR:SCHEDULER]`、`[FLATTEN_COMPACTOR]`、`[COMPACTOR::DUMP]`、`[DOWNSAMPLING]` 等 prefix 进行日志分级，关键操作均有 `log::info!` / `log::error!` 输出。

### 5.5 缺失的指标

源码中有一处 `TODO` 标注（`metrics.rs:619`）：
```rust
// TODO deletion / archiving stats
```
表明**删除/归档操作的专用指标尚未实现**。当前无法通过 Prometheus 指标直接观测：
- 每日删除的文件数/字节数
- Retention job 执行耗时
- 延迟删除队列深度
- 扩展保留区间的命中情况

---

## 6. 整体协同流程图

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Compactor Node 启动                            │
│                  (job/compactor.rs::run)                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────┐    ┌──────────────────┐                      │
│  │ run_generate_job │    │ run_generate_old  │                      │
│  │  (interval=10s)  │    │ _data_job(3601s)  │                      │
│  │  当前数据 merge   │    │  历史数据 merge    │                      │
│  └────────┬─────────┘    └────────┬─────────┘                      │
│           │                       │                                 │
│           ▼                       ▼                                 │
│     infra_file_list::add_job(offset)                               │
│           │                                                         │
│           ▼                                                         │
│  ┌──────────────────┐                                              │
│  │    run_merge     │ ← JobScheduler → MergeWorker                 │
│  │  (interval+2)    │    (多线程执行 merge_by_stream)               │
│  └────────┬─────────┘                                              │
│           │  merge 完成 → write_file_list → batch_add_deleted      │
│           │                                                         │
│           ▼                                                         │
│  ┌──────────────────┐    ┌────────────────────┐                    │
│  │  run_retention   │───▶│ retention::generate │                    │
│  │  (interval+3)    │    │ _jobs()             │                    │
│  └────────┬─────────┘    └────────┬───────────┘                    │
│           │                       │ 按天生成 delete job             │
│           │                       ▼                                 │
│           │              db::compact::retention::                    │
│           │              delete_stream()                             │
│           │                       │                                 │
│           ▼                       ▼                                 │
│  ┌──────────────────┐    delete_by_date()                           │
│  │ run_delay_deletion│    ├── delete_from_file_list (逻辑删除)      │
│  │  (interval+4)    │    └── batch_add_deleted (入延迟队列)         │
│  └────────┬─────────┘                                              │
│           │ 延迟 delete_files_delay_hours 后                        │
│           ▼                                                         │
│     deleted::delete()                                               │
│     ├── storage::del (parquet 文件)                                 │
│     ├── storage::del (倒排索引文件)                                  │
│     ├── storage::del (flattened 文件)                               │
│     └── batch_remove_deleted (清理表记录)                            │
│                                                                     │
│  ┌──────────────────┐    ┌──────────────────┐                      │
│  │ check_running    │    │ clean_done_jobs  │                      │
│  │ _jobs(600s)      │    │ (7200s)          │                      │
│  │ 超时作业回收      │    │ 已完成作业清理     │                      │
│  └──────────────────┘    └──────────────────┘                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 7. 关键配置速查

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `ZO_COMPACT_ENABLED` | `true` | 是否启用 Compactor |
| `ZO_COMPACT_INTERVAL` | `10` | 基础调度间隔（秒） |
| `ZO_COMPACT_DATA_RETENTION_DAYS` | `3650` | 全局数据保留天数 |
| `ZO_COMPACT_EXTENDED_DATA_RETENTION_DAYS` | `3650` | 扩展保留回溯天数 |
| `ZO_COMPACT_DELETE_FILES_DELAY_HOURS` | `2` | 物理删除延迟（小时） |
| `ZO_COMPACT_FILE_LIST_DELETED_MODE` | `"deleted"` | 删除记录模式 |
| `ZO_COMPACT_JOB_RUN_TIMEOUT` | `600` | 作业超时（秒） |
| `ZO_COMPACT_JOB_CLEAN_WAIT_TIME` | `7200` | 已完成作业清理等待时间（秒） |
| `ZO_COMPACT_RETENTION_ALLOWED_HOURS` | `""` | Retention 允许运行的小时列表 |
| `ZO_COMPACT_BATCH_SIZE` | `0` | 批量获取作业数 |
| `ZO_COMPACT_MAX_FILE_SIZE` | `2048` | 合并后文件最大尺寸（MB） |
| `ZO_COMPACT_MAX_GROUP_FILES` | `10000` | 单次合并最大文件数 |
| `ZO_COMPACT_STRATEGY` | `"file_time"` | 合并排序策略 |
