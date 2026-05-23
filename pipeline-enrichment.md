# 采集管道与富化表查询协作方式分析

## 1. 架构概述

OpenObserve 的采集管道（Pipeline）与富化表（Enrichment Table）查询采用了分层协作的架构设计：

- **采集管道层**：负责数据的实时/定时采集、转换、过滤和路由
- **富化表层**：提供静态参考数据的存储、加载和查询能力
- **函数执行层**：通过 VRL/JS 函数实现管道与富化表的数据关联
- **查询优化层**：针对富化表 JOIN 查询进行特殊的广播连接优化

## 2. 字段转换机制

### 2.1 转换函数类型

管道支持两种函数运行时，均支持富化表查询：

**VRL 函数（推荐）** - `src/service/ingestion/mod.rs:138-206`
- 编译时注入富化表注册表 `TableRegistry`
- 支持 `get_enrichment_table_record` 等 VRL 内置函数
- 高性能 AST 解释执行

**JS 函数** - `src/service/ingestion/mod.rs:129-136`
- 通过 QuickJS 运行时执行
- 用于复杂逻辑处理场景

### 2.2 函数编译流程

`src/service/pipeline/batch_execution.rs:135-168`

```rust
// 1. 获取函数定义
let transform = get_transforms(&self.org, &func_params.name).await?;

// 2. 编译 VRL 函数并加载富化表
let vrl_runtime_config = compile_vrl_function(&transform.function, &self.org)?;
let registry = vrl_runtime_config.config.get_custom::<TableRegistry>().unwrap();
registry.finish_load();  // 完成富化表加载

// 3. 编译结果缓存
CompiledFunctionRuntime::VRL(Box::new(VRLResultResolver { ... }), is_result_array)
```

### 2.3 富化表注册表构建

`src/common/utils/functions.rs:45-89`

```rust
pub fn get_vrl_compiler_config(org_id: &str) -> VRLCompilerConfig {
    let registry = TableRegistry::default();
    let mut tables: HashMap<String, Box<dyn Table + Send + Sync>> = HashMap::new();

    // 加载用户自定义富化表
    for table in ENRICHMENT_TABLES.iter() {
        if table.org_id == org_id || table.org_id == DEFAULT_ORG {
            tables.insert(table.stream_name.to_owned(), Box::new(table.value().clone()));
        }
    }

    // 加载 GeoIP 富化表（城市、ASN、企业版）
    if let Some(v) = GEOIP_CITY_TABLE.read().as_ref() {
        tables.insert(GEO_IP_CITY_ENRICHMENT_TABLE.to_owned(), Box::new(v.clone()));
    }

    registry.load(tables);
    config.set_custom(registry);
}
```

### 2.4 字段转换执行

`src/service/pipeline/batch_execution.rs:903-1001`

```rust
// VRL 函数执行
record = match apply_vrl_fn(
    &mut vrl_runtime_state,
    vrl_resolver,
    record,
    &org_id,
    std::slice::from_ref(&stream_name),
) {
    (res, None) => res,           // 转换成功
    (res, Some(error)) => {       // 转换失败 - 错误旁路
        // 记录错误但继续处理
        error_sender.send((node_id, node_type, err_msg, Some(func_name))).await;
        res  // 返回原始记录
    }
};

// 结果数组模式（批量转换）
// - 收集一批记录后一次性转换
// - 适用于需要聚合上下文的场景
```

## 3. 关联表加载机制

### 3.1 富化表数据结构

`src/service/enrichment/mod.rs:34-38`

```rust
#[derive(Debug, Clone)]
pub struct StreamTable {
    pub org_id: String,
    pub stream_name: String,
    pub data: Arc<Vec<vrl::value::Value>>,  // 内存中的富化数据
}
```

### 3.2 富化表查询接口

`src/service/enrichment/mod.rs:51-95`

实现 `vector_enrichment::Table` trait，支持：

- **精确匹配查询** (`find_table_row`)
  - 大小写敏感/不敏感匹配
  - 多条件 AND 逻辑
  
- **范围查询** (`find_table_rows`)
  - 日期范围过滤 (`FromDate`, `ToDate`, `BetweenDates`)
  - 字段投影 (`select` 参数)

### 3.3 两级缓存加载策略

`src/service/enrichment/mod.rs:173-235`

```rust
pub async fn get_enrichment_table_inner(org_id: &str, table_name: &str, ...) -> Result<Values> {
    // 1. 检查元数据更新时间
    let db_stats = enrichment_table::get_meta_table_stats(org_id, table_name).await?;
    let local_last_updated = storage::local::get_last_updated_at(org_id, table_name).await?;

    // 2. 远程拉取（如果本地缓存过期）
    let values = if db_stats.end_time > local_last_updated || local_last_updated == 0 {
        // 通过 SQL 查询从数据库获取完整数据
        enrichment_table::get_enrichment_table_data(org_id, table_name, ...).await?
    } else {
        // 3. 本地缓存加载（Parquet 文件）
        storage::local::retrieve(org_id, table_name).await?
    };

    // 4. 异步更新本地缓存
    storage::local::store_data_if_needed_background(...).await?;
}
```

### 3.4 查询时富化表加载（EnrichmentExec）

`src/service/search/datafusion/distributed_plan/enrichment_exec.rs:201-342`

```rust
async fn fetch_data(...) -> Result<SendableRecordBatchStream> {
    // 第一优先级：磁盘 Parquet 文件（高性能）
    let disk_result = read_from_disk(&org_id, &stream_name, &schema).await;
    if let Ok(batches) = disk_result {
        return Ok(Box::pin(MemoryStream::try_new(batches, schema, None)?));
    }

    // 第二优先级：内存缓存（ENRICHMENT_TABLES）
    let enrichment_data = match ENRICHMENT_TABLES.get(&key) {
        Some(stream_table) => stream_table.data.clone(),
        None => Arc::new(vec![]),
    };

    // 并行转换 VRL Value 到 RecordBatch
    let batches: Result<Vec<_>, _> = pool.install(|| {
        chunks.into_par_iter()
            .map(|chunk| convert_vrl_to_record_batch(&schema, chunk))
            .collect()
    });
}
```

### 3.5 广播连接优化

`src/service/search/datafusion/optimizer/physical_optimizer/enrichment.rs:34-112`

```rust
pub fn enrichment_broadcast_join_rewrite(plan: Arc<dyn ExecutionPlan>, ...) -> Result<Arc<dyn ExecutionPlan>> {
    // 1. 识别 HashJoinExec 节点
    // 2. 将 NewEmptyExec 替换为 EnrichmentExec（加载富化数据）
    // 3. 转换为广播连接（小表广播到大表侧）
    // 适用条件：
    //   - 只有一个 HashJoin
    //   - 左表是 enrichment_tables/enrich schema
    //   - 右表是普通日志/指标流
}
```

## 4. 错误旁路机制

### 4.1 管道执行错误处理架构

`src/service/pipeline/batch_execution.rs:331-503`

```rust
// 双通道设计：结果通道 + 错误通道
let (result_sender, mut result_receiver) = channel::<(usize, StreamParams, Value)>(batch_size);
let (error_sender, mut error_receiver) = channel::<(String, String, String, Option<String>)>(batch_size);

// 每个节点独立错误处理
// - 不因为单个节点失败导致整个管道失败
// - 错误记录包含：节点ID、节点类型、错误消息、函数名
```

### 4.2 节点级错误旁路

**Stream 节点错误** - `src/service/pipeline/batch_execution.rs:660-677`
- Flatten 失败：记录错误，跳过该记录，继续处理下一条
- 动态流名解析失败：记录警告，丢弃该记录

**Condition 节点错误** - `src/service/pipeline/batch_execution.rs:803-824`
- Flatten 失败：记录错误，跳过该记录
- 条件评估失败：继续下一条记录

**Function 节点错误** - `src/service/pipeline/batch_execution.rs:875-936`
- VRL/JS 函数执行失败：记录错误，返回原始记录（不丢弃）
- 结果数组模式错误：记录错误，中止当前批次处理

**跨类型目标节点错误** - `src/service/pipeline/batch_execution.rs:735-773`
- 异步后台 ingestion 失败：仅记录日志，不影响主管道
- 使用 `tokio::spawn` 隔离执行上下文

### 4.3 错误收集与持久化

`src/service/pipeline/batch_execution.rs:428-503`

```rust
// 独立的错误收集任务
let error_task = tokio::spawn(async move {
    while let Some((node_id, node_type, error, fn_name)) = error_receiver.recv().await {
        pipeline_error.add_node_error(node_id, node_type, error, fn_name);
    }
    if count > 0 { Some(pipeline_error) } else { None }
});

// 错误发布到 self_reporting 流
if let Some(pipeline_errors) = error_task.await? {
    publish_error(ErrorData {
        _timestamp: Utc::now().timestamp_micros(),
        stream_params: source_stream_params,
        error_source: ErrorSource::Pipeline(pipeline_errors),
    }).await;
}
```

### 4.4 错误持久化存储

`src/service/db/pipeline_errors.rs:29-84`

```rust
pub async fn upsert(pipeline_id: &str, ..., error_data: &PipelineError) -> Result<()> {
    // 幂等性优化：仅在错误内容变化时更新
    if let Some(existing_model) = existing {
        let error_changed = existing_model.error_summary != error_data.error
            || existing_model.node_errors != node_errors_json;
        if error_changed {
            // 执行更新
        }
        // 错误未变化则跳过写入，降低 DB 负载
    }
}

// 定期清理：delete_older_than(cutoff_timestamp)
```

## 5. 统计指标体系

### 5.1 管道执行统计

`src/service/pipeline/batch_execution.rs:304-329`

```rust
// 源流摄入统计
let source_size: f64 = records.iter()
    .map(|record| record.to_string().len() as f64)
    .sum::<f64>() / config::SIZE_IN_MB;

if source_size > 0.0 {
    report_request_usage_stats(
        RequestStats {
            size: source_size,
            records: batch_size as i64,
            response_time: 0.0,
            ..Default::default()
        },
        org_id,
        &self.id,                    // pipeline ID
        source_stream_type,
        UsageType::Pipeline,
        0,                            // 函数数量
        timestamp,
    ).await;
}
```

### 5.2 远程目标统计

`src/service/pipeline/batch_execution.rs:1274-1295`

```rust
// WAL 写入成功后报告远程目标使用量
let data_size_mb = data_size as f64 / config::SIZE_IN_MB;
if data_size_mb > 0.0 {
    report_request_usage_stats(
        RequestStats {
            size: data_size_mb,
            records: records_len,
            ..Default::default()
        },
        &org_id,
        &remote_stream.destination_name,
        StreamType::Logs,
        UsageType::RemotePipeline,
        0,
        chrono::Utc::now().timestamp_micros(),
    ).await;
}
```

### 5.3 富化查询执行指标

`src/service/search/datafusion/distributed_plan/enrichment_exec.rs:103-142`

```rust
#[derive(Debug, Clone)]
pub struct EnrichmentMetrics {
    pub fetch_data_time: metrics::Time,        // 内存缓存读取时间
    pub read_disk_time: metrics::Time,         // 磁盘 Parquet 读取时间
    pub vrl_to_record_batch_time: metrics::Time, // VRL 到 RecordBatch 转换时间
    pub output_rows: metrics::Count,           // 输出行数
}

// 指标埋点示例
let timer = metrics.read_disk_time.timer();
let disk_result = read_from_disk(...).await;
timer.done();

metrics.record_output(total_rows);
```

### 5.4 函数执行错误指标

`src/service/ingestion/mod.rs:171-178`

```rust
metrics::INGEST_ERRORS
    .with_label_values(&[
        org_id,
        StreamType::Logs.as_str(),
        &format!("{stream_name:?}"),
        TRANSFORM_FAILED,  // "transform_failed"
    ])
    .inc();
```

### 5.5 缓存状态指标

`src/service/db/pipeline.rs:185-190`

```rust
pub async fn get_cache_stats() -> (usize, usize) {
    let realtime_count = STREAM_EXECUTABLE_PIPELINES.read().await.len();
    let scheduled_count = SCHEDULED_PIPELINES.read().await.len();
    (realtime_count, scheduled_count)
}
```

## 6. 数据流转时序

### 6.1 实时管道 + VRL 富化查询流程

```
数据摄入
    ↓
[Ingestion Service]
    ↓ 查找管道
[STREAM_EXECUTABLE_PIPELINES 缓存]
    ↓
[ExecutablePipeline::process_batch]
    ├─ 初始化节点任务（每个节点一个 tokio task）
    │   ├─ Source Node (Stream)
    │   │   └─ send_to_children → 发送到所有子节点
    │   ├─ Function Node (VRL)
    │   │   ├─ 检查是否需要 flatten
    │   │   ├─ apply_vrl_fn → 调用 VRL 运行时
    │   │   │   └─ VRL 程序中调用 get_enrichment_table_record
    │   │   │       └─ TableRegistry → StreamTable::find_table_row
    │   │   │           └─ 内存中条件匹配查询
    │   │   ├─ ✅ 成功：发送转换后记录到子节点
    │   │   └─ ❌ 失败：发送原始记录 + 错误到 error_channel
    │   ├─ Condition Node
    │   │   ├─ flatten（如需要）
    │   │   ├─ 条件评估
    │   │   ├─ ✅ 通过：发送到子节点
    │   │   └─ ❌ 不通过：丢弃
    │   └─ Leaf Node (Stream)
    │       ├─ flatten（如需要）
    │       ├─ 动态流名解析
    │       ├─ 同类型：result_sender → 返回给调用者
    │       └─ 跨类型：tokio::spawn → 后台 ingestion
    ├─ result_task 收集输出
    ├─ error_task 收集错误
    └─ publish_error → 错误自报告流
```

### 6.2 富化表 JOIN 查询流程

```
SQL 查询 (SELECT * FROM logs JOIN enrichment_tables.geoip ON ...)
    ↓
[SQL Parser]
    ↓
[Logical Plan]
    ↓
[Physical Optimizer]
    ├─ should_use_enrichment_broadcast_join?
    │   ├─ 检查：只有一个 HashJoin？
    │   ├─ 检查：左表是 enrichment_tables schema？
    │   └─ 检查：右表是普通流且无复杂算子？
    └─ enrichment_broadcast_join_rewrite
        ├─ 将 NewEmptyExec 替换为 EnrichmentExec
        └─ 转换为广播连接执行计划
    ↓
[EnrichmentExec::execute]
    ├─ 尝试磁盘 Parquet 读取
    ├─ 失败则回退到内存缓存
    ├─ VRL Value → RecordBatch 并行转换
    └─ 输出到 HashJoinExec
    ↓
[Broadcast HashJoin]
    ↓
查询结果
```

## 7. 关键设计决策

### 7.1 富化表内存驻留设计
- **决策**：所有富化表数据全量加载到内存
- **理由**：富化表通常是小表（参考数据），内存查询性能远高于磁盘
- **权衡**：占用额外内存，不适用于超大型富化表

### 7.2 错误旁路而非中断
- **决策**：单条记录转换失败不中断管道，返回原始记录
- **理由**：数据可用性优先，避免单个坏数据导致整批失败
- **权衡**：可能输出未经转换的"脏"数据，需要依赖错误监控

### 7.3 两级缓存策略
- **决策**：内存 + 本地磁盘 Parquet 双缓存
- **理由**：
  - 内存：最低延迟查询
  - 磁盘：进程重启后快速恢复，避免全量远程拉取
- **权衡**：磁盘占用额外存储空间

### 7.4 广播连接优化
- **决策**：富化表 JOIN 自动转换为广播连接
- **理由**：富化表是小表，广播可以避免数据 shuffle
- **权衡**：仅适用于左表为富化表的场景

## 8. 核心文件索引

| 模块 | 文件路径 | 主要职责 |
|------|---------|---------|
| 管道核心 | `src/service/pipeline/batch_execution.rs` | 管道批量执行、节点处理、错误旁路 |
| 管道管理 | `src/service/pipeline/mod.rs` | 管道 CRUD、验证、版本控制 |
| 富化表实现 | `src/service/enrichment/mod.rs` | StreamTable 实现、两级缓存加载 |
| 富化执行计划 | `src/service/search/datafusion/distributed_plan/enrichment_exec.rs` | 查询时富化数据加载、指标统计 |
| 富化表提供者 | `src/service/search/datafusion/table_provider/enrich_table.rs` | DataFusion TableProvider 实现 |
| 广播连接优化 | `src/service/search/datafusion/optimizer/physical_optimizer/enrichment.rs` | 富化 JOIN 查询重写 |
| 函数编译 | `src/common/utils/functions.rs` | VRL 编译器配置、富化表注册表 |
| 函数执行 | `src/service/ingestion/mod.rs` | VRL/JS 函数编译与执行 |
| 错误持久化 | `src/service/db/pipeline_errors.rs` | 管道错误存储与查询 |
| 管道缓存 | `src/service/db/pipeline.rs` | ExecutablePipeline 缓存管理 |
| 全局配置 | `src/common/infra/config.rs` | ENRICHMENT_TABLES 等全局缓存 |
