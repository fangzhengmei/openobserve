# OpenObserve 数据保留与后台清理协同机制——源码深度分析

> 基于 `src/service/compact/`、`src/service/db/compact/`、`src/infra/src/file_list/`、`src/infra/src/cluster/mod.rs` 等源码梳理。

---

## 1. file_list 删除在不同 FileListBookKeepMode 下的实际行为

### 1.1 三种模式定义

`FileListBookKeepMode` 枚举定义于 `src/config/src/meta/stream.rs:1325-1330`：

```rust
pub enum FileListBookKeepMode {
    History,     // "history" — 写入 file_list_history 表
    #[default]
    Deleted,     // "deleted" — 写入 file_list_deleted 表
    None,        // "none"    — 不做额外簿记
}
```

由环境变量 `ZO_COMPACT_FILE_LIST_DELETED_MODE` 控制，默认 `"deleted"`。

### 1.2 核心操作：batch_process 的内部行为

`batch_process`（`src/infra/src/file_list/postgres.rs:1878-2022`）接收一组 `FileKey`，在一个事务内完成：

1. **`deleted = false` 的条目** → `INSERT INTO file_list ... ON CONFLICT DO NOTHING`（新增合并后的文件）
2. **`deleted = true` 的条目** → 先按 `(stream, date, file)` 查出 `id`，再 `DELETE FROM file_list WHERE id IN (...)`（物理删除旧文件记录）

**关键点**：`batch_process` 本身是对 `file_list` 表的增删操作，与 `file_list_deleted_mode` 无关。`file_list_deleted_mode` 仅控制 `batch_process` 之后的"簿记"步骤。

### 1.3 三种模式在各操作路径中的实际行为差异

#### 1.3.1 Retention 删除路径（`retention.rs:571-636 write_file_list`）

此路径**严格遵循** `file_list_deleted_mode`：

| 步骤 | `Deleted`（默认） | `History` | `None` |
|------|-------------------|-----------|--------|
| ① `batch_add_history` | 跳过 | ✅ 写入 `file_list_history`（deleted=false） | 跳过 |
| ② `batch_process` | ✅ 从 file_list 删除记录 | ✅ 从 file_list 删除记录 | ✅ 从 file_list 删除记录 |
| ③ `batch_add_deleted` | ✅ 写入 `file_list_deleted`（待物理删除） | 跳过 | 跳过 |

`History` 模式的意图是：将待删文件信息（以 `deleted=false` 形式）归档到 `file_list_history` 表，但**不触发物理删除**。存储上的 parquet 文件将永久保留。

`None` 模式的意图是：只从 `file_list` 中移除记录，不做任何额外簿记。存储文件将成为孤儿文件。

#### 1.3.2 Merge 合并路径（`merge.rs:1035-1095 write_file_list`）

此路径**不检查** `file_list_deleted_mode`，始终执行：

1. `batch_process(events)` — 在 file_list 中插入新文件、删除旧文件
2. `batch_add_deleted(org_id, created_at, &del_items)` — 将旧文件写入 `file_list_deleted` 表

**差异影响**：即使配置为 `History` 模式，Merge 产生的旧文件仍写入 `file_list_deleted` 而非 `file_list_history`。后续 `run_delay_deletion` 仍会物理删除这些文件。这是**合理的**：Merge 是替换操作（旧文件 → 新文件），旧文件的存储空间必须回收；而 Retention 是生命周期删除，`History` 模式下需保留数据以备审计。

#### 1.3.3 Dump 删除路径（`dump.rs:476-494 delete_by_time_range`）

此路径也**不检查** `file_list_deleted_mode`，与 Merge 路径一致：始终调用 `batch_process` + `batch_add_deleted`。

### 1.4 延迟物理删除流程（仅 `Deleted` 模式产生数据）

`run_delay_deletion`（`mod.rs:399-445`）的处理：

1. 查询 `file_list_deleted` 表中 `created_at < (now - delete_files_delay_hours)` 的记录
2. 按 `BATCH_SIZE = 10000` 分批
3. 依次删除：存储 parquet 文件 → 倒排索引 puffin 文件 → flattened 文件
4. 最后从 `file_list_deleted` 表中删除记录

**`History` 模式下**：`file_list_deleted` 表中没有 Retention 产生的记录（但仍有 Merge/Dump 产生的记录），所以 `run_delay_deletion` 只处理 Merge/Dump 的旧文件。

**`None` 模式下**：`file_list_deleted` 表中只有 Merge/Dump 产生的记录，Retention 删除的文件直接从 file_list 移除，存储文件将成为孤儿。

### 1.5 模式选择对存储的影响总结

| 模式 | Retention 删除的文件 | Merge 替换的旧文件 | 孤儿文件风险 |
|------|---------------------|-------------------|------------|
| `Deleted` | 延迟物理删除 ✅ | 延迟物理删除 ✅ | 无 |
| `History` | 保留在存储 ✅（归档到 history 表） | 延迟物理删除 ✅ | 无 |
| `None` | **成为孤儿文件** ⚠️ | 延迟物理删除 ✅ | **Retention 产生的孤儿** |

---

## 2. Retention 与 Merge 的互斥边界条件

### 2.1 is_deleting_stream 机制详解

`is_deleting_stream` 定义于 `src/service/db/compact/retention.rs:107-114`：

```rust
pub fn is_deleting_stream(
    org_id: &str, stream_type: StreamType, stream_name: &str,
    date_range: Option<(&str, &str)>,
) -> bool {
    CACHE.contains_key(&mk_key(org_id, stream_type, stream_name, date_range))
}
```

`mk_key`（同文件 :29-39）生成的 key 格式：

| date_range 参数 | 生成 key | 产生场景 |
|-----------------|---------|---------|
| `None` | `{org}/{type}/{stream}/all` | 仅 `delete_all` 场景 |
| `Some(("2023-01-01","2023-01-02"))` | `{org}/{type}/{stream}/2023-01-01,2023-01-02` | 按天 retention 删除 |

CACHE 的更新机制：
- **写入**：`delete_stream`（:45-70）→ `db::put("/compact/delete/{key}", "OK")` → 触发 `watch` → `CACHE.insert(key, now_micros())`
- **执行锁**：`process_stream`（:73-89）→ `db::put("/compact/delete/{key}", node_uuid)` → 触发 `watch` → `CACHE.insert(key, now_micros())`
- **清除**：`delete_stream_done`（:116-135）→ `db::delete_if_exists("/compact/delete/{key}")` → 触发 `watch` → `CACHE.remove(key)`

### 2.2 关键边界条件：按天 Retention 不阻断 Merge

**所有调用点均使用 `date_range = None`**：

| 调用位置 | 检查表达式 | 生成的 key |
|----------|-----------|-----------|
| `mod.rs:142` run_generate_job | `is_deleting_stream(&org_id, stream_type, &stream_name, None)` | `{org}/{type}/{stream}/all` |
| `mod.rs:312` run_merge | `is_deleting_stream(&org_id, stream_type, &stream_name, None)` | `{org}/{type}/{stream}/all` |
| `dump.rs:107` | `is_deleting_stream(&org_id, stream_type, &stream_name, None)` | `{org}/{type}/{stream}/all` |
| `stream.rs:371` 流设置更新 | `is_deleting_stream(org_id, stream_type, stream_name, None)` | `{org}/{type}/{stream}/all` |
| `flight.rs:132` 查询入口 | `is_deleting_stream(&org_id, stream_type, &stream_name, None)` | `{org}/{type}/{stream}/all` |
| `ingestion/mod.rs:472` 数据写入 | `is_deleting_stream(org_id, stream_type, stream_name, None)` | `{org}/{type}/{stream}/all` |

而**按天 retention 删除**在 CACHE 中的 key 是 `{org}/{type}/{stream}/2023-01-01,2023-01-02`，与 `{org}/{type}/{stream}/all` **不匹配**。

**结论**：**日常的按天 Retention 删除不会阻断任何 Merge、Dump、写入或查询操作**。`is_deleting_stream(_, None)` 仅在整流删除（`delete_all`）时返回 true。

### 2.3 保留期偏移量检查——Merge 的另一道防线

`run_merge`（`mod.rs:302-309`）：

```rust
let stream_data_retention_end = if stream_settings.data_retention > 0 {
    now - Duration::try_days(stream_settings.data_retention).unwrap()
} else {
    data_lifecycle_end
};
if job.offsets <= stream_data_retention_end.timestamp_micros() {
    need_done_ids.push(job.id); // the data will be deleted by retention, just skip
    continue;
}
```

此检查确保 Merge 不会合并即将被 Retention 删除的数据。但**它只检查 Merge 作业的 offset 是否落在保留期外**，不阻止保留期内的 Merge 与 Retention 并发执行。

### 2.4 互斥边界条件完整矩阵

| 场景 | is_deleting_stream 块？ | offset 检查块？ | 实际行为 |
|------|------------------------|-----------------|---------|
| **delete_all 正在进行 + Merge** | ✅ 块 | N/A | Merge 生成和执行均被跳过 |
| **delete_all 正在进行 + 写入** | ✅ 块 | N/A | 写入被拒绝 |
| **delete_all 正在进行 + 查询** | ✅ 块 | N/A | 查询返回 "stream is being deleted" |
| **按天 Retention + Merge（保留期外 offset）** | ❌ 不块 | ✅ 块 | Merge 作业标记为 done |
| **按天 Retention + Merge（保留期内 offset）** | ❌ 不块 | ❌ 不块 | **并发执行** |
| **按天 Retention + 写入** | ❌ 不块 | N/A | 正常写入 |
| **按天 Retention + 查询** | ❌ 不块 | N/A | **并发执行** |

### 2.5 并发执行时的安全性分析

按天 Retention 与保留期内的 Merge 并发执行是**安全的**，因为：

1. **操作范围不重叠**：Retention 按**天**粒度删除，Merge 按**小时**粒度合并。同一 stream 在同一天内，Retention 删除的是该天全部文件，Merge 处理的是更细粒度的小时文件。
2. **两阶段删除保护**：Retention 先逻辑删除（file_list 表中 `deleted=true`），Merge 的 `batch_process` 在事务内执行，如果文件已被逻辑删除则查不到 `id`，`DELETE` 操作对不存在的记录无影响。
3. **Merge 产生新文件不受影响**：Retention 的 `delete_from_file_list` 查询时，Merge 刚写入的新文件如果时间范围不在删除范围内，不会被选中。

**但存在一个窗口风险**：如果 Merge 正在合并某小时的文件，同时 Retention 正在删除同一天的文件，可能出现：
- Merge 读取旧文件列表 → Retention 逻辑删除部分文件 → Merge 写入新文件 + 逻辑删除旧文件
- 此时 Merge 的 `batch_process` 事务中，旧文件的 `id` 可能已被 Retention 的 `batch_process` 删除，导致 Merge 的 `DELETE` 找不到记录但事务仍成功（不报错）
- 最终状态：新文件已插入，旧文件已删除，结果正确

**极端风险**：Merge 合并后产生新文件的时间范围与 Retention 删除范围重叠时，Merge 新写入的文件会逃脱 Retention（因为 Retention 查询在 Merge 写入之前完成）。但下一轮 Retention 将覆盖这些文件。

### 2.6 delete_by_date 中的节点级执行锁

`delete_by_date`（`retention.rs:398-520`）在执行前会调用 `process_stream` 将节点 uuid 写入 `/compact/delete/{key}`。`delete_all` 和 `delete_by_date` 在执行前均检查 `get_stream` 返回的节点 uuid：

```rust
let node = db::compact::retention::get_stream(org_id, stream_type, stream_name, Some(date_range)).await;
if !node.is_empty() && LOCAL_NODE.uuid.ne(&node) && get_node_by_uuid(&node).await.is_some() {
    log::warn!("... is deleting by {node}");
    return Ok(()); // not this node, just skip
}
```

这确保了**同一个 date_range 的 retention job 不会被两个节点同时执行**。但不同 date_range 的 job 可以在不同节点并行。

---

## 3. Hasher 键作为基准的节点分配差异

### 3.1 一致性哈希实现

`get_node_from_consistent_hash`（`src/infra/src/cluster/mod.rs:99-126`）：

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
    if let Some((_, name)) = iter.next() {
        return Some(name.clone());
    }
    if let Some((_, name)) = nodes.first_key_value() {
        return Some(name.clone());
    }
    None
}
```

核心：`gxhash::new().sum64(key)` → 在 BTreeMap 中找到第一个 `>= hash` 的节点。

### 3.2 各操作路径的 Hash 键

| 操作 | 阶段 | Hash 键 | 键示例 | 角色 |
|------|------|---------|--------|------|
| **Retention 生成** | generate_jobs | `stream_name` | `"my_stream"` | Compactor |
| **Retention 执行** | run_retention | `job`（完整 key） | `"my_org/logs/my_stream/2023-01-01,2023-01-02"` | Compactor |
| **Merge 生成** | run_generate_job | `stream_name` | `"my_stream"` | Compactor |
| **Merge 执行（Daily）** | run_merge | `stream_name` | `"my_stream"` | Compactor |
| **Merge 执行（Hourly）** | run_merge | 无节点检查（任何节点可拾取） | — | — |
| **Dump 执行（Daily）** | dump::run | `stream_name` | `"my_stream"` | Compactor |
| **Flatten 生成** | generate_jobs | `stream_name` | `"my_stream"` | FlattenCompactor |
| **Downsampling 生成** | run_generate_downsampling_job | `stream_name` | `"my_stream"` | Compactor |

### 3.3 关键差异：Retention 生成与执行使用不同 Hash 键

源码注释（`mod.rs:61-62`）明确说明了这一点：

> here we use job to get the compactor node, so that we can use different compactor for different job of same stream

**生成阶段**用 `stream_name` 决定哪个节点负责为该 stream 创建 retention job；
**执行阶段**用完整 job key（`{org}/{type}/{stream}/{date_range}`）决定哪个节点负责执行该 job。

这意味着：同一 stream 的不同日期的 retention job **可能被分配到不同的 Compactor 节点执行**。

**示例**（假设 3 个 Compactor 节点 A/B/C）：

```
stream_name = "app_logs"
  → sum64("app_logs") = 0x7F3A... → 节点 B 生成 retention job

Job 1: "default/logs/app_logs/2023-01-01,2023-01-02"
  → sum64("default/logs/app_logs/2023-01-01,2023-01-02") = 0x2B1C... → 节点 A 执行

Job 2: "default/logs/app_logs/2023-01-02,2023-01-03"
  → sum64("default/logs/app_logs/2023-01-02,2023-01-03") = 0xE9D4... → 节点 C 执行
```

节点 B 生成 job，但节点 A 和 C 分别执行。

### 3.4 Merge 生成与执行的一致性

Merge 的节点分配更加一致：
- **生成阶段**用 `stream_name`
- **执行阶段**（Daily partition）也用 `stream_name`
- **Hourly partition** 不做节点检查，任何持有该 job 的节点都可以执行

这意味着**同一 stream 的 Merge 生成和 Daily partition 的 Merge 执行始终在同一节点**。

### 3.5 FlattenCompactor 使用独立角色

Flatten 作业使用 `Role::FlattenCompactor`，与 `Role::Compactor` 使用完全不同的哈希环。即使 FlattenCompactor 和 Compactor 在同一物理节点上，它们在各自的哈希环中是独立映射的。

### 3.6 节点分配差异对作业冲突的影响

#### 3.6.1 Retention 生成节点 ≠ 执行节点的影响

| 影响 | 分析 |
|------|------|
| **作业冲突** | 无影响。`delete_by_date` 内部有 `process_stream` + `get_stream` 节点级锁，确保同一 date_range 的 job 只由一个节点执行 |
| **CACHE 一致性** | 无影响。CACHE 通过 `watch` 机制在所有节点间同步（`db::Event::Put` → `CACHE.insert`，`db::Event::Delete` → `CACHE.remove`） |
| **负载均衡** | 正面影响。不同日期的 job 分散到不同节点，避免单节点过载 |

#### 3.6.2 Merge 与 Retention 在不同节点的并发风险

考虑以下场景（3 节点 A/B/C）：

```
1. stream "app_logs" 的 Merge 生成节点 = B（hash by stream_name）
2. stream "app_logs" 的 Retention 执行节点 = A（hash by job key）
3. 节点 A 正在执行 Retention 删除 2023-01-01 的文件
4. 节点 B 正在执行 Merge 合并 2023-01-02 的文件
```

由于两者操作不同的时间范围且 file_list 表操作在事务中完成，不会产生数据库层面的冲突。但存在**查询一致性的短暂窗口**：

- 节点 A 完成 Retention 的 `batch_process`（逻辑删除）后、完成 `batch_add_deleted` 之前
- 此时查询该 stream 的 2023-01-01 数据将返回空结果（file_list 中已无记录）
- 但存储文件尚未物理删除（在 `delete_files_delay_hours` 之后才删除）

这是**预期行为**——逻辑删除后查询即不可见，物理删除是延迟的后台操作。

#### 3.6.3 Daily Partition 的特殊互斥

`PartitionTimeLevel::Daily` 的 stream 在 Merge 执行时会额外检查：
1. 一致性哈希确认节点归属（与生成阶段使用相同 hash key = `stream_name`）
2. `db::compact::stream::is_running` 检查（内存级 HashSet，单节点内互斥）

`is_running`/`set_running`/`clear_running`（`src/service/db/compact/stream.rs:20-32`）使用进程内 `RwHashSet`，**仅对同一进程内的同一 stream 生效**。跨节点的同一 stream 的 Merge job 不会被此机制阻止——但一致性哈希已确保 Daily partition 的 stream 只由一个节点处理。

### 3.7 节点变更时的处理

`run_generate_job`（`mod.rs:117-137`）中，当发现当前节点不再是某 stream 的归属节点时，会主动释放该 stream 的 offset：

```rust
if LOCAL_NODE.name.ne(&node_name) {
    if let Some((offset, _)) = db::compact::files::get_offset_from_cache(
        &org_id, stream_type, &stream_name,
    ).await {
        db::compact::files::set_offset(
            &org_id, stream_type, &stream_name, offset, None,
        ).await?;
    }
    continue;
}
```

`set_offset(..., None)` 将节点字段设为 None，允许新归属节点在下一轮拾取。类似逻辑也存在于 Downsampling 路径。

---

## 4. 查询一致性与作业冲突风险综合分析

### 4.1 查询一致性的保障层次

| 层次 | 机制 | 保障范围 | 源码位置 |
|------|------|---------|---------|
| **L1: 逻辑删除隔离** | file_list 表 `deleted=true` 标记 | 查询引擎通过 file_list 过滤，逻辑删除的文件不可见 | `retention.rs:557` |
| **L2: 延迟物理删除** | `delete_files_delay_hours`（默认 2h） | 正在进行的查询有足够时间完成对旧文件的读取 | `mod.rs:399-446` |
| **L3: 整流删除阻断** | `is_deleting_stream(_, None)` | `delete_all` 时阻断写入和查询 | `flight.rs:132`、`stream.rs:371` |
| **L4: 事务原子性** | `batch_process` 在单个数据库事务中完成 | 新增和删除操作原子性，不会出现部分写入 | `postgres.rs:1878-2022` |

### 4.2 作业冲突风险矩阵

| 冲突场景 | 风险等级 | 现有防护 | 残留风险 |
|----------|---------|---------|---------|
| **delete_all + Merge** | 低 | `is_deleting_stream(_, None)` 阻断 | 无 |
| **delete_all + 写入/查询** | 低 | `is_deleting_stream(_, None)` 阻断 | 无 |
| **按天 Retention + Merge（不同时间范围）** | 低 | 两阶段删除 + 事务原子性 | 无 |
| **按天 Retention + Merge（同一天不同小时）** | 中 | 无显式互斥 | Merge 新文件可能逃脱当轮 Retention（下轮覆盖） |
| **两个不同日期的 Retention job 在不同节点** | 低 | `process_stream` 节点级锁 | 无 |
| **Retention + 查询（按天删除进行中）** | 低 | 逻辑删除后查询立即不可见 | 符合预期 |
| **Merge + 查询（合并进行中）** | 低 | Merge 不修改原始数据，合并完成后原子替换 | 短暂窗口查询可能看到合并前或后的文件集合 |
| **None 模式 Retention** | 高 | 无 | 孤儿文件无法回收 |

### 4.3 按天 Retention 不阻断查询/写入的设计合理性

`is_deleting_stream(_, None)` 在按天 Retention 场景下始终返回 false，这意味着：
- **写入不受影响**：数据持续写入，不会因某天的数据正在被清理而中断
- **查询不受影响**：用户可以正常查询所有未被逻辑删除的数据
- **查询一致性**：逻辑删除是即时生效的——`batch_process` 事务提交后，查询引擎立刻看不到被删除的文件

这是**有意设计**：按天 Retention 是常规的后台清理操作，不应影响业务。只有整流删除（`delete_all`，通常是用户手动触发）才会阻断业务。

### 4.4 batch_process 中的幂等性保证

`batch_process` 的 INSERT 使用 `ON CONFLICT DO NOTHING`（`postgres.rs:1905-1933`），DELETE 使用 `WHERE id IN (...)`。这意味着：

- 如果 Merge 的新文件已被插入（重试场景），`INSERT` 不会报错
- 如果 Retention 已先删除了 Merge 要删除的旧文件，`DELETE` 找不到 `id` 也不会报错（`Ok(None)` → `continue`，见 `postgres.rs:1981`）

这为 Retention 与 Merge 的并发操作提供了**天然的幂等性**。

### 4.5 CACHE 一致性窗口

CACHE 的更新依赖 `watch` 机制（`retention.rs:148-175`），通过 etcd/数据库的 event 通知同步。存在一个**极短的一致性窗口**：

1. 节点 A 调用 `delete_stream_done` → `db::delete_if_exists("/compact/delete/{key}")` 
2. 事件传播到节点 B 的 `watch` → `CACHE.remove(key)`
3. 在步骤 1 和 2 之间，节点 B 仍认为该 stream 正在删除

但此窗口的影响有限：
- 导致 Merge job 多等一轮调度（最坏情况：多等 `compact.interval` 秒）
- 不会导致数据损坏或查询不一致

---

## 5. 失败重试机制对比

### 5.1 Retention write_file_list 重试

```rust
// retention.rs:580-631
for _ in 0..5 {
    // History 模式：batch_add_history
    // batch_process（必须成功）
    if let Err(e) = infra_file_list::batch_process(&events).await { ... continue; }
    // Deleted 模式：batch_add_deleted
    if let Err(e) = infra_file_list::batch_add_deleted(...).await { ... continue; }
    success = true;
    break;
}
```

**问题**：如果 `batch_process` 成功但 `batch_add_deleted` 失败，重试时 `batch_process` 会再次执行。由于 INSERT 使用 `ON CONFLICT DO NOTHING`，DELETE 对已删除的记录无影响，所以重试是**幂等的**。但 `batch_add_deleted` 也会重试，如果前一次实际已写入则可能产生重复记录——这由 `file_list_deleted` 表的主键/唯一约束保证去重。

### 5.2 Merge write_file_list 重试

```rust
// merge.rs:1062-1078
for _ in 0..5 {
    if !mark_deleted_done && let Err(e) = infra::file_list::batch_process(events).await { ... continue; }
    mark_deleted_done = true;  // 标记 batch_process 已成功
    if !del_items.is_empty() && let Err(e) = infra_file_list::batch_add_deleted(...).await { ... continue; }
    success = true;
    break;
}
```

Merge 使用 `mark_deleted_done` 标志位，确保重试时**不重复执行 `batch_process`**，只重试 `batch_add_deleted`。这比 Retention 的实现更精确。

### 5.3 Dump delete_by_time_range 重试

```rust
// dump.rs:477-494
for _ in 0..5 {
    if !mark_deleted_done && let Err(e) = infra::file_list::batch_process(&items).await { ... continue; }
    mark_deleted_done = true;
    if let Err(e) = infra::file_list::batch_add_deleted(...).await { ... continue; }
    break;
}
```

与 Merge 相同的模式，使用 `mark_deleted_done` 标志。

### 5.4 作业级重试

- **Retention job**：`delete_by_date` 失败后 job 记录不会被删除（`delete_stream_done` 未调用），下一轮 `run_retention` 会重新拾取
- **Merge job**：`merge_by_stream` 失败后 job 不会被标记为 done，`compactor_check_running_jobs` 在超时后将其重置为 pending
- **Dump job**：类似 Merge，超时后重置为 pending

---

## 6. 完整协同流程图

```
                          ┌──────────────────────────────────────────────┐
                          │           Compactor Node 启动                │
                          │        (job/compactor.rs::run)               │
                          └──────────────────────────────────────────────┘
                                         │
        ┌────────────────────────────────┼────────────────────────────────┐
        │                                │                                │
        ▼                                ▼                                ▼
  ┌─────────────┐  ┌──────────────────┐  ┌──────────────────┐
  │run_generate  │  │run_generate_old  │  │ run_retention    │
  │_job (10s)   │  │_data_job (3601s) │  │ (interval+3)     │
  │             │  │                  │  │                  │
  │hash key:    │  │hash key:         │  │ ① generate_jobs  │
  │stream_name  │  │stream_name       │  │   hash key:       │
  │             │  │                  │  │   stream_name     │
  │is_deleting_ │  │is_deleting_      │  │                  │
  │stream(_,    │  │stream(_,         │  │ ② run_retention  │
  │  None) 检查 │  │  None) 检查      │  │   hash key:       │
  └──────┬──────┘  └──────┬───────────┘  │   完整 job key   │
         │                │               └──────┬───────────┘
         ▼                ▼                       │
    add_job(offset)  add_job(offset)              ▼
         │                │               ┌──────────────────┐
         └────────┬───────┘               │ db::compact::     │
                  │                       │ retention::list() │
                  ▼                       │                    │
         ┌──────────────────┐             │ 对每个 job:        │
         │    run_merge     │             │ hash key = job     │
         │  (interval+2)    │             │ → 决定执行节点     │
         │                  │             │                    │
         │ offset ≤ 保留截止?│             │ delete_all?        │
         │ → 标记 done      │             │ → delete_all()     │
         │                  │             │                    │
         │ is_deleting_     │             │ delete_by_date?    │
         │ stream(_, None)? │             │ → delete_by_date() │
         │ → 标记 done      │             │   process_stream() │
         │                  │             │   → 节点级锁       │
         │ Daily partition? │             └──────────────────┘
         │ → hash stream_name
         │ → is_running?    │                     │
         │   → release job  │                     ▼
         │                  │             ┌──────────────────┐
         │ → JobScheduler   │             │ delete_from_      │
         │   → MergeWorker  │             │ file_list()       │
         └──────────────────┘             │                    │
                                          │ ① file_list::query │
                                          │ ② 标记 deleted=true│
                                          │ ③ write_file_list  │
                                          │   ├ History模式:   │
                                          │   │ batch_add_     │
                                          │   │ history()      │
                                          │   ├ batch_process()│
                                          │   └ Deleted模式:  │
                                          │     batch_add_     │
                                          │     deleted()      │
                                          └────────┬─────────┘
                                                   │
                                                   ▼
                                          ┌──────────────────┐
                                          │ run_delay_deletion│
                                          │ (interval+4)      │
                                          │                   │
                                          │ 查询 file_list_   │
                                          │ deleted 表        │
                                          │ created_at <      │
                                          │ now - delay_hours │
                                          │                   │
                                          │ 物理删除:          │
                                          │ ① parquet 文件    │
                                          │ ② puffin 索引     │
                                          │ ③ flattened 文件  │
                                          │ ④ 删除表记录       │
                                          └──────────────────┘
```

---

## 7. 关键配置速查

| 环境变量 | 默认值 | 影响范围 |
|----------|--------|---------|
| `ZO_COMPACT_DATA_RETENTION_DAYS` | `3650` | 全局数据保留天数 |
| `ZO_COMPACT_EXTENDED_DATA_RETENTION_DAYS` | `3650` | 扩展保留回溯天数上限 |
| `ZO_COMPACT_DELETE_FILES_DELAY_HOURS` | `2` | 物理删除延迟（小时） |
| `ZO_COMPACT_FILE_LIST_DELETED_MODE` | `"deleted"` | 文件删除簿记模式 |
| `ZO_COMPACT_RETENTION_ALLOWED_HOURS` | `""` | Retention 允许运行的小时列表 |
| `ZO_COMPACT_JOB_RUN_TIMEOUT` | `600` | 作业超时（秒） |
| `ZO_COMPACT_JOB_CLEAN_WAIT_TIME` | `7200` | 已完成作业清理等待时间（秒） |
| `ZO_COMPACT_FILE_LIST_DELETED_BATCH_SIZE` | `1000` | 删除记录批量大小 |
