# OpenObserve Function Pipeline 表达式解析与执行实现分析

## 概述

OpenObserve 的 Function Pipeline 是一个基于 VRL (Vector Remap Language) 的数据转换管道系统，支持在数据摄取阶段对日志、指标等进行实时转换。本文从代码实现角度深入分析表达式编译为执行计划、ingest 阶段字段改写与错误处理副作用控制的实现机制。

---

## 一、表达式编译为执行计划

### 1.1 核心数据结构

#### 1.1.1 函数定义结构 (`Transform`)

**位置**: `src/config/src/meta/function.rs:37-52`

```rust
pub struct Transform {
    pub function: String,           // 函数体 (VRL 或 JavaScript)
    pub name: String,               // 函数名称
    pub params: String,             // 参数列表
    pub num_args: u8,               // 参数数量
    pub trans_type: Option<u8>,     // 0=VRL, 1=JavaScript
    pub streams: Option<Vec<StreamOrder>>,
}
```

**关键方法**:
- `is_vrl()` / `is_js()`: 判断函数类型
- `is_result_array_vrl()` / `is_result_array_js()`: 判断是否为数组批量处理模式
- 通过 `#ResultArray#` 注释标记启用批量处理模式

#### 1.1.2 VRL 编译配置

**位置**: `src/config/src/meta/function.rs:161-182`

```rust
pub struct VRLCompilerConfig {
    pub config: CompileConfig,          // VRL 编译配置
    pub functions: Vec<Box<dyn Function>>,  // 可用函数列表
}

pub struct VRLRuntimeConfig {
    pub config: CompileConfig,          // 编译后配置
    pub program: Program,               // 编译后的 VRL 程序 (执行计划)
    pub fields: Vec<String>,            // 输出字段列表
}

pub struct VRLResultResolver {
    pub program: Program,               // 可执行程序
    pub fields: Vec<String>,            // 字段列表
}
```

#### 1.1.3 Pipeline 结构

**位置**: `src/config/src/meta/pipeline/mod.rs:43-60`

```rust
pub struct Pipeline {
    pub id: String,
    pub version: i32,
    pub enabled: bool,
    pub org: String,
    pub name: String,
    pub source: PipelineSource,         // Realtime 或 Scheduled
    pub nodes: Vec<Node>,               // 节点列表
    pub edges: Vec<Edge>,               // 边列表 (连接关系)
}
```

**节点类型** (`NodeData`):
- `Stream(StreamParams)`: 源流/目标流节点
- `Function(FunctionParams)`: 函数转换节点
- `Condition(ConditionParams)`: 条件过滤节点
- `Query(DerivedStream)`: 查询源节点 (定时管道)
- `RemoteStream(RemoteStreamParams)`: 远程目标节点
- `LlmEvaluation(LlmEvaluationParams)`: LLM 评估节点 (企业版)

### 1.2 VRL 表达式编译流程

#### 1.2.1 编译入口 (`compile_vrl_function`)

**位置**: `src/service/ingestion/mod.rs:93-119`

```rust
pub fn compile_vrl_function(func: &str, org_id: &str) -> Result<VRLRuntimeConfig, std::io::Error> {
    // 安全检查：禁止使用 get_env_var
    if func.contains("get_env_var") {
        return Err(std::io::Error::other("get_env_var is not supported"));
    }

    let external = state::ExternalEnv::default();
    let vrl_config = get_vrl_compiler_config(org_id);
    
    // 调用 VRL 编译器编译表达式
    match vrl::compiler::compile_with_external(
        func,
        &vrl_config.functions,
        &external,
        vrl_config.config,
    ) {
        Ok(CompilationResult { program, warnings: _, config }) => Ok(VRLRuntimeConfig {
            program,    // 编译后的执行计划 (AST)
            config,
            fields: vec![],
        }),
        Err(e) => Err(std::io::Error::other(
            vrl::diagnostic::Formatter::new(func, e).to_string(),
        )),
    }
}
```

**编译过程**:
1. **安全检查**: 禁止 `get_env_var` 等危险函数
2. **获取编译器配置**: 加载标准库函数和 enrichment 表
3. **调用 VRL 编译器**: `compile_with_external` 将 VRL 源代码编译为 `Program` (AST 执行计划)
4. **错误格式化**: 使用 VRL 诊断格式化器生成友好的错误信息

#### 1.2.2 VRL 编译器配置 (`get_vrl_compiler_config`)

**位置**: `src/common/utils/functions.rs:45-89`

```rust
pub fn get_vrl_compiler_config(org_id: &str) -> VRLCompilerConfig {
    let mut functions = vrl::stdlib::all();               // VRL 标准库
    functions.append(&mut vector_enrichment::vrl_functions()); // Enrichment 函数
    
    let registry = TableRegistry::default();
    let mut tables: HashMap<String, Box<dyn Table + Send + Sync>> = HashMap::new();
    
    // 加载 enrichment 表 (用户自定义 + GeoIP)
    for table in en_tables.iter() {
        if table.org_id == org_id || table.org_id == DEFAULT_ORG {
            tables.insert(table.stream_name.to_owned(), Box::new(table.value().clone()));
        }
    }
    
    // 加载 GeoIP 表
    if let Some(v) = GEOIP_CITY_TABLE.read().as_ref() {
        tables.insert(GEO_IP_CITY_ENRICHMENT_TABLE.to_owned(), Box::new(v.clone()));
    }
    
    registry.load(tables);
    let mut config = vrl::compiler::CompileConfig::default();
    config.set_custom(registry);
    
    VRLCompilerConfig { config, functions }
}
```

**关键要点**:
- 每个组织有独立的编译配置，隔离 enrichment 表
- 支持 GeoIP 城市级和 ASN 级 enrichment
- 企业版支持企业级 MaxMind 数据库

### 1.3 Pipeline 执行计划构建

#### 1.3.1 可执行 Pipeline (`ExecutablePipeline`)

**位置**: `src/service/pipeline/batch_execution.rs:171-191`

```rust
pub struct ExecutablePipeline {
    id: String,
    name: String,
    source_node_id: String,                           // 源节点 ID
    sorted_nodes: Vec<String>,                        // 拓扑排序后的节点列表
    function_map: HashMap<String, CompiledFunctionRuntime>,  // 编译后的函数
    node_map: HashMap<String, ExecutableNode>,        // 节点映射
}
```

#### 1.3.2 执行计划构建流程 (`ExecutablePipeline::new`)

**位置**: `src/service/pipeline/batch_execution.rs:217-286`

```rust
pub async fn new(pipeline: &Pipeline) -> Result<Self> {
    // 1. 构建节点映射
    let node_map = pipeline.nodes.iter().map(|node| {
        (node.get_node_id(), ExecutableNode {
            id: node.get_node_id(),
            node_data: node.get_node_data(),
            children: pipeline.edges.iter()
                .filter(|edge| edge.source == node.id)
                .map(|edge| edge.target.clone())
                .collect(),
        })
    }).collect();

    // 2. 注册并编译所有函数
    let function_map = match pipeline.register_functions().await {
        Ok(map) => map,
        Err(e) => { /* 发布错误并返回 */ }
    };

    // 3. 拓扑排序确定执行顺序
    let sorted_nodes = match topological_sort(&node_map) {
        Ok(sorted) => sorted,
        Err(e) => { /* 发布错误并返回 */ }
    };

    let source_node_id = sorted_nodes[0].to_owned();

    Ok(Self { id: pipeline.id.to_string(), name: pipeline.name.to_string(), 
              source_node_id, node_map, sorted_nodes, function_map })
}
```

#### 1.3.3 函数注册 (`register_functions`)

**位置**: `src/service/pipeline/batch_execution.rs:134-169`

```rust
async fn register_functions(&self) -> Result<HashMap<String, CompiledFunctionRuntime>> {
    let mut function_map = HashMap::new();
    for node in &self.nodes {
        if let NodeData::Function(func_params) = &node.data {
            let transform = get_transforms(&self.org, &func_params.name).await?;
            
            let compiled_runtime = if transform.is_js() {
                // 编译 JavaScript 函数 (仅 _meta 组织允许)
                let js_config = compile_js_function(&transform.function, &self.org)?;
                CompiledFunctionRuntime::JS(js_config, transform.is_result_array_js())
            } else {
                // 编译 VRL 函数
                let vrl_runtime_config = compile_vrl_function(&transform.function, &self.org)?;
                let registry = vrl_runtime_config.config
                    .get_custom::<vector_enrichment::TableRegistry>().unwrap();
                registry.finish_load();
                CompiledFunctionRuntime::VRL(
                    Box::new(VRLResultResolver {
                        program: vrl_runtime_config.program,
                        fields: vrl_runtime_config.fields,
                    }),
                    transform.is_result_array_vrl(),
                )
            };
            
            function_map.insert(node.get_node_id(), compiled_runtime);
        }
    }
    Ok(function_map)
}
```

#### 1.3.4 拓扑排序 (`topological_sort`)

**位置**: `src/service/pipeline/batch_execution.rs:1661-1682`

```rust
fn topological_sort(node_map: &HashMap<String, ExecutableNode>) -> Result<Vec<String>> {
    let mut result = Vec::new();
    let mut visited = HashSet::new();
    let mut temp = HashSet::new();

    let graph: HashMap<String, Vec<String>> = node_map
        .iter()
        .map(|(k, v)| (k.clone(), v.children.clone()))
        .collect();

    for node in node_map.keys() {
        dfs(node, &graph, &mut visited, &mut temp, &mut result)?;
    }

    result.reverse();
    Ok(result)
}

fn dfs(...) -> Result<()> {
    if temp.contains(current_node_id) {
        return Err(anyhow!("Cyclical pipeline detected."));  // 环检测
    }
    if visited.contains(current_node_id) {
        return Ok(());
    }
    temp.insert(current_node_id.to_string());
    // 递归访问子节点
    temp.remove(current_node_id);
    visited.insert(current_node_id.to_string());
    result.push(current_node_id.to_string());
    Ok(())
}
```

**执行计划构建总结**:
```
Pipeline 定义 → 节点映射构建 → 函数编译 → 拓扑排序 → ExecutablePipeline
     |              |                |            |
     └─ nodes       └─ id+data       └─ VRL/JS   └─ DFS + 环检测
     └─ edges       └─ children       └─ 缓存
```

---

## 二、ingest 阶段字段改写与错误处理

### 2.1 Ingest 流程总览

**位置**: `src/service/logs/ingest.rs:66-599`

```
HTTP/gRPC 请求 → 格式解析 → Pipeline 检查 → 数据转换 → 时间戳处理 → 写盘
                                 |
                                 └─ 有 Pipeline? → 批量执行 Pipeline → 结果处理
```

#### 2.1.1 Ingest 入口与 Pipeline 集成

**位置**: `src/service/logs/ingest.rs:108-115`

```rust
// 检索关联的 Pipeline
let stream_param = StreamParams::new(org_id, &stream_name, stream_type);
let executable_pipeline =
    crate::service::ingestion::get_stream_executable_pipeline(&stream_param).await;

// 准备 Pipeline 输入
let mut pipeline_inputs = Vec::with_capacity(stream_params.len());
let mut original_options = Vec::with_capacity(stream_params.len());
```

**位置**: `src/service/logs/ingest.rs:344-494`

```rust
// 批量执行 Pipeline
if let Some(exec_pl) = &executable_pipeline {
    let records_count = pipeline_inputs.len();
    match exec_pl.process_batch(org_id, pipeline_inputs, Some(stream_name.clone())).await {
        Err(e) => {
            // 整个批次失败，记录错误指标
            stream_status.status.failed += records_count as u32;
            metrics::INGEST_ERRORS.with_label_values(&[
                org_id, StreamType::Logs.as_str(), &stream_name, TRANSFORM_FAILED
            ]).inc();
        }
        Ok(pl_results) => {
            // 处理每个目标流的结果
            for (stream_params, stream_pl_results) in pl_results {
                // 时间戳处理、Schema 重构、原始数据保存等
            }
        }
    }
}
```

### 2.2 字段改写实现

#### 2.2.1 VRL 函数执行 (`apply_vrl_fn`)

**位置**: `src/service/ingestion/mod.rs:138-206`

```rust
pub fn apply_vrl_fn(
    runtime: &mut Runtime,
    vrl_runtime: &VRLResultResolver,
    row: Value,
    org_id: &str,
    stream_name: &[String],
) -> (Value, Option<String>) {
    // 1. 构建执行上下文
    let mut metadata = vrl::value::Value::from(BTreeMap::new());
    metadata.insert("org_id", vrl::value::Value::from(org_id.to_string()));
    metadata.insert("stream_name", vrl::value::Value::from(stream_name[0].clone()));
    
    let mut target = TargetValueRef {
        value: &mut vrl::value::Value::from(&row),  // 输入数据
        metadata: &mut metadata,                     // 元数据
        secrets: &mut vrl::value::Secrets::new(),    // 密钥存储
    };

    // 2. 执行编译后的 VRL 程序
    let timezone = vrl::compiler::TimeZone::Local;
    let result = match vrl::compiler::VrlRuntime::default() {
        vrl::compiler::VrlRuntime::Ast => {
            runtime.resolve(&mut target, &vrl_runtime.program, &timezone)
        }
    };

    // 3. 处理执行结果
    match result {
        Ok(res) => match res.try_into() {
            Ok(val) => (val, None),  // 成功：返回转换后的数据
            Err(err) => {
                // 结果转换失败：返回原始数据 + 错误信息
                metrics::INGEST_ERRORS.with_label_values(&[...]).inc();
                let clean_err = format!("{org_id}/{stream_name:?} vrl failed: {err:?}");
                (row, Some(clean_err))
            }
        },
        Err(err) => {
            // 执行失败：返回原始数据 + 错误信息
            metrics::INGEST_ERRORS.with_label_values(&[...]).inc();
            let clean_err = format!("{org_id}/{stream_name:?} vrl runtime error: {err:?}");
            (row, Some(clean_err))
        }
    }
}
```

**关键实现细节**:
1. **上下文注入**: `org_id` 和 `stream_name` 通过 metadata 注入，可在 VRL 中访问
2. **不可变输入**: 输入 `row` 不会被修改，失败时可安全返回
3. **AST 执行**: 使用 VRL 的 AST 解释器执行编译后的程序

#### 2.2.2 单记录 vs 批量处理模式

**位置**: `src/service/pipeline/batch_execution.rs:904-1014`

**单记录模式** (默认):
```rust
CompiledFunctionRuntime::VRL(vrl_resolver, false) => {
    record = match apply_vrl_fn(&mut vrl_runtime_state, vrl_resolver, 
                                record, &org_id, &[stream_name]) {
        (res, None) => res,
        (res, Some(error)) => { /* 处理错误 */ res }
    };
    send_to_children(&mut child_senders, PipelineItem { ... }, "FunctionNode").await;
}
```

**批量处理模式** (`#ResultArray#`):
```rust
CompiledFunctionRuntime::VRL(vrl_resolver, true) => {
    // 先收集所有记录
    result_array_records.push(record);
}

// 接收完成后批量执行
let result = match apply_vrl_fn(
    &mut vrl_runtime_state, vrl_resolver,
    json::Value::Array(result_array_records),  // 整个数组作为输入
    &org_id, &[stream_name]
) {
    (res, None) => res,
    (res, Some(error)) => { /* 处理错误 */ res }
};

// 展开结果数组
for record in result.as_array().unwrap().iter() {
    send_to_children(&mut child_senders, PipelineItem {
        idx: usize::MAX,  // 标记为无原始数据对应
        record: record.clone(),
        flattened: false,
    }, "FunctionNode").await;
}
```

**标记约定**:
- `idx: usize::MAX` 表示该记录由批量处理生成，无对应的原始输入记录
- 原始数据保存时会跳过这些记录

### 2.3 Pipeline 批量执行引擎

#### 2.3.1 并发执行模型 (`process_batch`)

**位置**: `src/service/pipeline/batch_execution.rs:288-520`

```rust
pub async fn process_batch(
    &self,
    org_id: &str,
    records: Vec<Value>,
    stream_name: Option<String>,
) -> Result<HashMap<StreamParams, Vec<(usize, Value)>>> {
    let batch_size = records.len();
    
    // 1. 创建通信通道
    let (result_sender, mut result_receiver) = channel(batch_size);  // 结果通道
    let (error_sender, mut error_receiver) = channel(batch_size);    // 错误通道
    
    let mut node_senders = HashMap::new();
    let mut node_receivers = HashMap::new();
    
    // 为每个节点创建独立的通道
    for node_id in &self.sorted_nodes {
        let (sender, receiver) = channel::<PipelineItem>(batch_size);
        node_senders.insert(node_id.to_string(), sender);
        node_receivers.insert(node_id.to_string(), receiver);
    }

    // 2. 为每个节点 spawn 独立任务
    let mut node_tasks = Vec::with_capacity(self.sorted_nodes.len());
    for (idx, node_id) in self.sorted_nodes.iter().enumerate() {
        let node = self.node_map.get(node_id).unwrap().clone();
        let node_receiver = node_receivers.remove(node_id).unwrap();
        let child_senders: Vec<_> = node.children.iter()
            .map(|child| node_senders.get(child).unwrap().clone())
            .collect();
        let result_sender_cp = node.children.is_empty().then_some(result_sender.clone());
        let function_runtime = self.function_map.get(node_id).cloned();
        
        let task = tokio::spawn(process_node(
            node, node_receiver, child_senders, function_runtime,
            result_sender_cp, error_sender.clone(), ...
        ));
        node_tasks.push(task);
    }

    // 3. 结果收集任务
    let result_task = tokio::spawn(async move {
        let mut results = HashMap::new();
        while let Some((idx, stream_params, record)) = result_receiver.recv().await {
            results.entry(stream_params)
                .or_insert(Vec::new())
                .push((idx, record));
        }
        results
    });

    // 4. 错误收集任务
    let error_task = tokio::spawn(async move {
        let mut pipeline_error = PipelineError::new(&self.id, &self.name);
        while let Some((node_id, node_type, error, fn_name)) = error_receiver.recv().await {
            pipeline_error.add_node_error(node_id, node_type, error, fn_name);
        }
        // 如果有错误，发布到 self_reporting 系统
        if count > 0 { publish_error(error_data).await; }
    });

    // 5. 发送数据到源节点启动处理
    let source_sender = node_senders.remove(&self.source_node_id).unwrap();
    for (idx, record) in records.into_iter().enumerate() {
        source_sender.send(PipelineItem { idx, record, flattened }).await?;
    }
    drop(source_sender);

    // 6. 等待所有任务完成
    try_join_all(node_tasks).await?;
    let pipeline_errors = error_task.await?;
    let results = result_task.await?;

    Ok(results)
}
```

**并发模型特点**:
- 每个节点独立 `tokio::spawn`，利用多核并行
- 节点间通过 `mpsc::channel` 传递数据，天然背压
- 结果和错误通过专用通道收集，解耦处理与报告
- 源节点发送完数据后 drop sender，触发所有节点优雅退出

#### 2.3.2 节点处理逻辑 (`process_node`)

**位置**: `src/service/pipeline/batch_execution.rs:622-1635`

```rust
async fn process_node(...) -> Result<()> {
    match &node.node_data {
        // Stream 节点 (源或目标)
        NodeData::Stream(stream_params) => {
            if node.children.is_empty() {
                // 目标节点：结果收集
                while let Some(pipeline_item) = receiver.recv().await {
                    // Flatten 处理
                    if !flattened && !record.is_null() && record.is_object() {
                        record = flatten::flatten_with_level(record, cfg.limit.ingest_flatten_level)?;
                    }
                    // 动态流名解析 (如 {field_name})
                    if destination_stream.stream_name.contains("{") {
                        destination_stream.stream_name = 
                            resolve_stream_name(&destination_stream.stream_name, &record)?;
                    }
                    // 跨类型处理 (如 Logs → Metrics)
                    if is_cross_type {
                        cross_type_records.push(record);  // 后台异步 ingest
                    } else {
                        result_sender.send((idx, destination_stream, record)).await?;
                    }
                }
            } else {
                // 源节点：广播到所有子节点
                while let Some(item) = receiver.recv().await {
                    send_to_children(&mut child_senders, item, "StreamNode").await;
                }
            }
        }
        
        // Condition 节点
        NodeData::Condition(condition_params) => {
            while let Some(pipeline_item) = receiver.recv().await {
                // 必须先 flatten 才能按字段判断
                if !flattened && !record.is_null() && record.is_object() {
                    record = flatten::flatten_with_level(record, cfg.limit.ingest_flatten_level)?;
                }
                // 评估条件
                let passes = match condition_params {
                    ConditionParams::V1 { conditions } => 
                        conditions.evaluate(record.as_object().unwrap()).await,
                    ConditionParams::V2 { conditions } => 
                        conditions.evaluate(record.as_object().unwrap()).await,
                };
                // 条件满足才发送到子节点
                if passes {
                    send_to_children(&mut child_senders, PipelineItem { ... }, "ConditionNode").await;
                }
            }
        }
        
        // Function 节点 (核心转换逻辑)
        NodeData::Function(func_params) => {
            let mut vrl_runtime_state = init_functions_runtime();
            while let Some(pipeline_item) = receiver.recv().await {
                // 根据 after_flatten 决定是否先 flatten
                if func_params.after_flatten && !flattened && !record.is_null() && record.is_object() {
                    record = flatten::flatten_with_level(record, cfg.limit.ingest_flatten_level)?;
                }
                // 执行 VRL/JS 函数
                match runtime {
                    CompiledFunctionRuntime::VRL(vrl_resolver, is_result_array) => {
                        if !is_result_array {
                            record = match apply_vrl_fn(...) {
                                (res, None) => res,
                                (res, Some(error)) => {
                                    error_sender.send((node.id, "function", err_msg, Some(func_name))).await;
                                    res  // 返回原始数据继续流动
                                }
                            };
                            flattened = false;  // VRL 可能生成嵌套结构
                            send_to_children(&mut child_senders, PipelineItem { ... }, "FunctionNode").await;
                        } else {
                            result_array_records.push(record);  // 收集用于批量处理
                        }
                    }
                    CompiledFunctionRuntime::JS(js_config, is_result_array) => {
                        // 类似 VRL 处理逻辑
                    }
                }
            }
            // 处理批量模式结果
            if !result_array_records.is_empty() {
                // 批量执行并展开结果
            }
        }
        
        // 其他节点类型...
    }
}
```

### 2.4 错误处理与副作用控制

#### 2.4.1 失败安全策略 (Fail-Safe)

**核心原则**: 函数执行失败时，**返回原始数据而非丢弃**。

**位置**: `src/service/ingestion/mod.rs:167-205`

```rust
match result {
    Ok(res) => match res.try_into() {
        Ok(val) => (val, None),  // 成功
        Err(err) => {
            metrics::INGEST_ERRORS.with_label_values(&[...]).inc();
            log::debug!("vrl failed at processing result {err:?} on record {row:?}. Returning original row.");
            let clean_err = format!("{org_id}/{stream_name:?} vrl failed: {err:?}");
            (row, Some(clean_err))  // 返回原始数据 + 错误
        }
    },
    Err(err) => {
        metrics::INGEST_ERRORS.with_label_values(&[...]).inc();
        log::debug!("vrl runtime failed at getting result {err:?} on record {row:?}. Returning original row.");
        let clean_err = format!("{org_id}/{stream_name:?} vrl runtime error: {err:?}");
        (row, Some(clean_err))  // 返回原始数据 + 错误
    }
}
```

**设计意图**:
- 确保数据不丢失：即使转换失败，原始数据仍可被索引
- 避免级联失败：单个函数失败不会导致整个 Pipeline 中断
- 可观测性：通过指标和日志追踪失败情况

#### 2.4.2 敏感信息脱敏

**位置**: `src/service/ingestion/mod.rs:184, 202`

```rust
// Log full error with record for debugging (内部调试日志)
log::debug!("{org_id}/{stream_name:?} vrl failed at processing result {err:?} on record {row:?}. Returning original row.");

// Return only error message without sensitive record data (返回给用户的错误)
let clean_err = format!("{org_id}/{stream_name:?} vrl failed: {err:?}");
```

**双层日志策略**:
- `log::debug!`: 包含完整记录数据，仅内部调试可用
- `clean_err`: 移除敏感数据，可安全返回给调用方

#### 2.4.3 错误指标与上报

**指标定义** (`src/service/logs/bulk.rs:45-48`):
```rust
pub const TRANSFORM_FAILED: &str = "document_failed_transform";
pub const TS_PARSE_FAILED: &str = "timestamp_parsing_failed";
pub const SCHEMA_CONFORMANCE_FAILED: &str = "schema_conformance_failed";
pub const PIPELINE_EXEC_FAILED: &str = "pipeline_execution_failed";
```

**指标上报**:
```rust
metrics::INGEST_ERRORS.with_label_values(&[
    org_id,
    StreamType::Logs.as_str(),
    &stream_name,
    TRANSFORM_FAILED,
]).inc();
```

**错误发布** (`publish_error`):
- Pipeline 执行错误通过 `self_reporting` 系统发布
- 包含详细的节点级错误信息
- 可在 UI 中查看 Pipeline 执行错误历史

#### 2.4.4 Flatten 时机控制

**Pipeline 验证规则** (`src/config/src/meta/pipeline/mod.rs:403-436`):
```rust
// 在同一分支中，如果前面的 FunctionNode 已经 flatten，后续节点也必须 flatten
if flattened && !func_params.after_flatten {
    return Err(anyhow!(
        "After Flatten must be checked if a previous FunctionNode already checked it in the same branch."
    ));
}
flattened |= func_params.after_flatten;
```

**设计目的**:
- 避免重复 flatten 操作，提升性能
- 确保数据一致性：一旦 flatten，后续所有节点都在 flatten 后的数据上操作

#### 2.4.5 跨类型流的副作用隔离

**位置**: `src/service/pipeline/batch_execution.rs:735-773`

```rust
if is_cross_type && !cross_type_records.is_empty() {
    let record_count = cross_type_records.len();
    tokio::spawn(async move {
        let req = cluster_rpc::IngestionRequest { ... };
        match crate::service::ingestion::ingestion_service::ingest(req).await {
            Ok(resp) if resp.status_code == 200 => {
                log::info!("cross-type ingestion successful ...");
            }
            Ok(resp) => {
                log::error!("cross-type ingestion failed (status={}): {}", resp.status_code, resp.message);
            }
            Err(e) => {
                log::error!("cross-type ingestion error: {e}");
            }
        }
    });
}
```

**隔离策略**:
- 跨类型目标流使用后台 `tokio::spawn` 异步执行
- 不阻塞主 Pipeline 流程
- 失败仅记录日志，不影响主流程
- 最终一致性模型：跨类型数据可能延迟或丢失，但不影响主流程

---

## 三、关键代码索引

| 功能模块 | 文件位置 | 关键行号 |
|---------|---------|---------|
| VRL 编译入口 | `src/service/ingestion/mod.rs` | 93-119 |
| VRL 函数执行 | `src/service/ingestion/mod.rs` | 138-206 |
| VRL 编译器配置 | `src/common/utils/functions.rs` | 45-89 |
| ExecutablePipeline 构建 | `src/service/pipeline/batch_execution.rs` | 217-286 |
| Pipeline 批量执行 | `src/service/pipeline/batch_execution.rs` | 288-520 |
| 节点处理逻辑 | `src/service/pipeline/batch_execution.rs` | 622-1635 |
| 拓扑排序 | `src/service/pipeline/batch_execution.rs` | 1661-1708 |
| 函数注册 | `src/service/pipeline/batch_execution.rs` | 134-169 |
| Pipeline 结构定义 | `src/config/src/meta/pipeline/mod.rs` | 43-60 |
| Transform 结构定义 | `src/config/src/meta/function.rs` | 37-52 |
| Ingest 主流程 | `src/service/logs/ingest.rs` | 66-599 |
| 错误常量定义 | `src/service/logs/bulk.rs` | 45-48 |
| Pipeline 验证 | `src/config/src/meta/pipeline/mod.rs` | 106-192 |

---

## 四、总结

### 4.1 架构亮点

1. **编译时优化**: VRL 表达式在 Pipeline 创建/更新时预编译为 AST，ingest 时直接执行
2. **失败安全**: 转换失败时返回原始数据，确保数据不丢失
3. **并发高效**: 每个节点独立 tokio 任务，节点间通道通信，天然背压
4. **灵活扩展**: 支持 VRL 和 JavaScript 两种函数类型，支持批量处理模式
5. **可观测性**: 完善的指标、日志、错误上报机制

### 4.2 设计权衡

| 决策 | 优点 | 缺点 |
|-----|-----|-----|
| AST 解释执行 | 编译快，无额外依赖 | 性能低于 JIT |
| 失败返回原始数据 | 数据不丢失 | 可能写入不符合预期的数据 |
| 跨类型后台异步 | 主流程低延迟 | 最终一致性，可能丢数 |
| 每个节点独立任务 | 多核利用充分 | 上下文切换开销 |

### 4.3 关键依赖

- `vrl` crate: Vector Remap Language 编译器和运行时
- `vector_enrichment` crate: 数据 enrichment 表支持
- `tokio`: 异步运行时和并发原语
- `serde_json`: JSON 数据处理

---

## 五、函数能力边界与脚本类型限制

### 5.1 双脚本体系与三级限制模型

OpenObserve 同时支持 VRL 和 JavaScript 两种脚本类型，但对二者施加了**差异化的三级限制模型**：

```
            ┌───────────────────────────────────────────────────────┐
            │                限制层级                                │
            ├───────────────────────────────────────────────────────┤
            │  L1: 组织级 ─ JS 仅 _meta 组织可用                    │
            │  L2: 管道级 ─ Pipeline 内禁止 JS 函数                 │
            │  L3: 函数级 ─ VRL/JS 各自的沙箱与安全限制              │
            └───────────────────────────────────────────────────────┘
```

#### 5.1.1 L1: 组织级限制 — JavaScript 仅限 `_meta` 组织

**位置**: `src/service/functions.rs:68-73`

```rust
// save_function 中的校验
if func.trans_type.unwrap_or(0) == 1 && org_id != "_meta" {
    return Ok(MetaHttpResponse::bad_request(
        "JavaScript functions are only allowed in the '_meta' organization. \
         Please use VRL functions for other organizations.",
    ));
}
```

**位置**: `src/service/functions.rs:140-144`

```rust
// test_run_function 中的校验
if trans_type == 1 && org_id != "_meta" {
    return Ok(MetaHttpResponse::bad_request(
        "JavaScript functions are only allowed in the '_meta' organization. \
         Please use VRL functions for other organizations.",
    ));
}
```

**影响范围**:
- `save_function`: 创建/更新函数时拦截
- `update_function`: 更新函数时同样拦截
- `test_run_function`: 测试运行时拦截
- JS 函数的**唯一合法用途**是 `_meta` 组织的 SSO claim 解析

#### 5.1.2 L2: 管道级限制 — Pipeline 内禁止 JavaScript

**位置**: `src/service/pipeline/mod.rs:42-63`

```rust
async fn validate_no_javascript_functions(pipeline: &Pipeline) -> Result<(), PipelineError> {
    for node in &pipeline.nodes {
        if let NodeData::Function(function_params) = &node.data {
            // 从 DB 加载函数定义以检查 trans_type
            let function = db_functions::get(&pipeline.org, &function_params.name)
                .await
                .map_err(|e| PipelineError::InvalidPipeline(format!(
                    "Failed to load function '{}': {}", function_params.name, e
                )))?;

            if function.is_js() {
                return Err(PipelineError::InvalidPipeline(format!(
                    "JavaScript functions cannot be used in pipelines. \
                     Function '{}' is a JavaScript function. \
                     Please use VRL functions instead.",
                    function_params.name
                )));
            }
        }
    }
    Ok(())
}
```

**调用时机**: 在 `save_pipeline` 和 `update_pipeline` 中均调用：

```rust
// save_pipeline (src/service/pipeline/mod.rs:89)
validate_no_javascript_functions(&pipeline).await?;

// update_pipeline (src/service/pipeline/mod.rs:137)
validate_no_javascript_functions(&pipeline).await?;
```

**注意**: 此验证是在 Pipeline 创建/更新时**实时查询 DB** 获取函数的 `trans_type`，而非仅依赖 Pipeline 定义中的节点元数据。这确保了即使函数后来被修改为 JS 类型，Pipeline 保存时也能捕获。

**前端联动**: 前端 PipelineEditor 同样过滤 JS 函数：

```javascript
// web/src/components/pipeline/PipelineEditor.vue:705-709
// JavaScript functions (trans_type === 1) cannot be used in pipelines
if (func.trans_type !== 1) {
    functions.value[func.name] = func;
    functionOptions.value.push(func.name);
}
```

#### 5.1.3 L3: 函数级沙箱与安全限制

**VRL 安全限制** (`src/service/ingestion/mod.rs:93-96`):

```rust
pub fn compile_vrl_function(func: &str, org_id: &str) -> Result<VRLRuntimeConfig, std::io::Error> {
    // 禁止 get_env_var — 防止环境变量泄露
    if func.contains("get_env_var") {
        return Err(std::io::Error::other("get_env_var is not supported"));
    }
    // ...
}
```

**VRL 能力边界**:
- 可用: `vrl::stdlib::all()` (VRL 标准库全部函数) + `vector_enrichment::vrl_functions()` (enrichment 函数)
- 禁止: `get_env_var` (环境变量读取)
- 上下文注入: `org_id` 和 `stream_name` 通过 `metadata` 注入，在 VRL 中以 `org_id` / `stream_name` 变量访问
- 执行模式: AST 解释执行 (`VrlRuntime::Ast`)

**JavaScript 安全限制** (`src/common/utils/js.rs:76-135`):

```rust
pub fn compile_js_function(func: &str, _org_id: &str) -> Result<JSRuntimeConfig, std::io::Error> {
    // 非空检查
    if func.trim().is_empty() {
        return Err(std::io::Error::other("JavaScript function cannot be empty"));
    }

    // 企业版: 使用集中式安全模式库
    #[cfg(feature = "enterprise")]
    {
        if let Err(pattern) = js_security::check_js_security(func) {
            return Err(std::io::Error::other(format!(
                "JavaScript function contains forbidden pattern: {}", pattern
            )));
        }
    }

    // 开源版: 内置危险模式列表
    #[cfg(not(feature = "enterprise"))]
    {
        const DANGEROUS_PATTERNS: &[&str] = &[
            "eval(", "Function(", "import(", "globalThis",
            "window.", "self.", "global.", "__proto__",
            "constructor.prototype", "constructor.constructor",
            "setTimeout", "setInterval", "setImmediate",
            "XMLHttpRequest", "fetch(", "WebSocket",
            "require(", "process.", "__dirname", "__filename",
            "module.", "exports.", "Reflect.", "Proxy(",
        ];
        // ...
    }
}
```

**JS 运行时沙箱** (`src/common/utils/js.rs:19-53`):

```rust
thread_local! {
    static JS_RUNTIME: Runtime = {
        let rt = Runtime::new().expect("Failed to create JS runtime");
        rt.set_memory_limit(10 * 1024 * 1024);     // 内存限制: 10MB
        rt.set_max_stack_size(512 * 1024);           // 栈大小限制: 512KB
        rt
    };
    static JS_CONTEXT: Context = JS_RUNTIME.with(|rt| {
        Context::full(rt).expect("Failed to create JS context")  // 使用完整上下文
    });
}
```

**JS 能力边界总结**:

| 维度 | 限制 | 值 |
|-----|------|---|
| 内存上限 | 单次执行 | 10 MB |
| 栈上限 | 递归深度 | 512 KB |
| 危险全局 | eval, Function | 编译时拦截 |
| 网络访问 | fetch, XMLHttpRequest, WebSocket | 编译时拦截 |
| 定时器 | setTimeout, setInterval, setImmediate | 编译时拦截 |
| 模块系统 | require, import, module, exports | 编译时拦截 |
| 原型链攻击 | __proto__, constructor.prototype | 编译时拦截 |
| 反射/代理 | Reflect, Proxy | 编译时拦截 |
| Node API | process, __dirname, __filename | 编译时拦截 |
| 全局逃逸 | globalThis, window, self, global | 编译时拦截 |
| 上下文注入 | orgId, streamName, inputJson | 运行时注入 |

### 5.2 校验入口与执行链路的连接方式

#### 5.2.1 完整校验-编译-缓存-执行链路

```
                        写入路径 (Pipeline 生命周期)
                        ═════════════════════════

 HTTP 请求
    │
    ▼
 save_pipeline / update_pipeline          ←── 校验入口
    │
    ├─ pipeline.validate()                ←── L0: 结构校验 (节点/边/环/leaf 类型)
    │   ├─ 非空名称
    │   ├─ 非空节点和边
    │   ├─ 首节点 = StreamNode | QueryNode
    │   ├─ ConditionNode 非空条件
    │   ├─ 足够边数连通
    │   ├─ DFS 遍历: leaf 必须是 StreamNode
    │   ├─ AfterFlatten 一致性检查
    │   └─ EnrichmentTables 仅 Scheduled
    │
    ├─ validate_no_javascript_functions()  ←── L2: Pipeline 级 JS 拦截
    │   └─ 遍历 FunctionNode → DB 查 trans_type → is_js() 则拒绝
    │
    ▼
 db::pipeline::set / update               ←── 持久化 + 触发缓存更新
    │
    ├─ infra_pipeline::put()              ←── 写入存储
    └─ update_cache()                     ←── 发送协调事件
         │
         ▼
    coordinator::emit_put_event()         ←── 集群广播
         │
         ▼
    db::pipeline::watch()                 ←── 各节点监听
         │
         ├─ ExecutablePipeline::new()     ←── 编译入口
         │   ├─ register_functions()       ←── 编译所有 FunctionNode
         │   │   └─ compile_vrl_function / compile_js_function  ←── L3: 函数级安全
         │   └─ topological_sort()         ←── 执行顺序
         │
         └─ STREAM_EXECUTABLE_PIPELINES   ←── 缓存 ExecutablePipeline
              .insert(stream_params, exec_pl)
```

```
                        读取路径 (Ingest 执行)
                        ══════════════════════

 Ingest 请求到达
    │
    ▼
 get_stream_executable_pipeline()         ←── 缓存查找
    │
    ▼
 STREAM_EXECUTABLE_PIPELINES.read()
    .get(stream_params)
    .cloned()                              ←── O(1) 缓存读取
    │
    ▼
 exec_pl.process_batch()                  ←── 执行入口
    │
    ├─ 每节点 spawn tokio 任务
    ├─ process_node(match node_data)
    │   ├─ FunctionNode → apply_vrl_fn / apply_js_fn  ←── 运行时
    │   ├─ ConditionNode → conditions.evaluate()
    │   └─ StreamNode → flatten + send
    │
    └─ 结果/错误收集
```

#### 5.2.2 关键衔接点分析

**衔接点 1: validate → compile 的延迟绑定**

校验时 (`save_pipeline`) 只检查 `is_js()`，**不执行编译**。编译发生在缓存构建时 (`ExecutablePipeline::new`)。这意味着：

1. 校验通过 ≠ 编译成功：函数语法错误在 `register_functions` 阶段才暴露
2. 编译失败时，Pipeline 不会被缓存，`log::error!` 记录但**不阻塞其他 Pipeline**
3. 缓存失败后，Ingest 走无 Pipeline 的直接写入路径

**位置**: `src/service/db/pipeline.rs:423-432`

```rust
match ExecutablePipeline::new(&pipeline).await {
    Err(e) => {
        log::error!(
            "[Pipeline::watch] {}/{}/{}: Error initializing pipeline \
             into ExecutablePipeline when updating cache: {}",
            pipeline.org, pipeline.name, pipeline.id, e
        );
        // 注意: 仅 log，不阻塞 watch 循环
    }
    Ok(exec_pl) => {
        stream_exec_pl.insert(stream_params.clone(), exec_pl);
    }
};
```

**衔接点 2: 函数更新对 Pipeline 缓存的影响**

**位置**: `src/service/functions.rs:400-419`

```rust
// update_function 中: 更新关联的 Pipeline
if let Ok(associated_pipelines) = db::pipeline::list_by_org(org_id).await {
    for pipeline in associated_pipelines {
        if pipeline.contains_function(&func.name)
            && let Err(e) = db::pipeline::update(&pipeline, None).await
        {
            // 更新失败 → 500 错误返回
            return Ok((http::StatusCode::INTERNAL_SERVER_ERROR, ...).into_response());
        }
    }
}
```

函数更新 → 遍历所有关联 Pipeline → 调用 `pipeline::update` → 触发 `update_cache` → 重新编译 `ExecutablePipeline`。若编译失败，Pipeline 从缓存中移除。

**衔接点 3: VRL `. ` 追加与编译的关系**

**位置**: `src/service/functions.rs:79-81`

```rust
// save_function 中: VRL 函数自动追加 "."
if func.trans_type.unwrap() == 0 && !func.function.ends_with('.') {
    func.function = format!("{} \n .", func.function);
}
```

VRL 语法要求程序最后一行是一个**点表达式**（表示返回当前值）。此追加发生在**编译前**，确保编译器能正确解析。在 Pipeline 的 `register_functions` 中则不再追加（因为 DB 中已存储追加点后的函数体）。

### 5.3 脚本限制对 Ingest 字段改写的影响

#### 5.3.1 VRL 限制对字段改写的影响

**`get_env_var` 禁止**:
- **影响**: VRL 函数无法读取服务端环境变量，无法将环境变量值注入到记录字段中
- **替代方案**: 使用 enrichment 表 或 VRL 的硬编码值
- **安全意义**: 防止通过 VRL 函数泄露服务端敏感配置（数据库密码、API Key 等）

**AST 解释执行的性能特征**:
- **影响**: 每次 `apply_vrl_fn` 调用都是完整的 AST 遍历，对高频小记录场景存在性能瓶颈
- **设计选择**: 牺牲执行速度换取编译速度和安全性（AST 模式无 JIT 注入风险）
- **对字段改写的影响**: VRL 函数中避免使用复杂正则、深层嵌套循环等操作，否则单条记录处理时间会显著增加

**metadata 注入的限制**:
- **可用**: `org_id`, `stream_name`
- **不可用**: 请求级信息（如 client IP、请求头、用户身份）
- **影响**: 字段改写逻辑无法基于请求来源做条件分支（如按 IP 段分流）

#### 5.3.2 JavaScript 限制对字段改写的影响 (仅 _meta 组织)

**Pipeline 内禁止 JS 的连锁效应**:
- **最关键的约束**: 即使 JS 函数存在于 `_meta` 组织，也**无法在 Pipeline 中使用**
- **JS 函数的合法使用场景**: 仅限传统的 Stream-Function 直接关联模式（`StreamOrder`，非 Pipeline）
- **遗留机制**: `StreamOrder.is_removed` + `StreamOrder.apply_before_flattening` 字段控制 JS 函数在流上的执行时机
- **Pipeline 取代**: 新的 Pipeline 架构仅支持 VRL，JS 函数是遗留兼容方案

**JS 运行时隔离对字段改写的影响**:

| 限制 | 对字段改写的影响 |
|-----|---------------|
| 10MB 内存 | 无法在单次执行中构建超大中间数组 (如: `rows.map(...)` 的 ResultArray 模式下大数组受限) |
| 512KB 栈 | 递归深度受限，无法处理深层嵌套 JSON 的递归改写 |
| 禁止 eval/Function | 无法动态生成字段名或动态构造改写逻辑 |
| 禁止网络访问 | 无法从外部 API 获取数据来丰富记录字段 |
| 禁止 require/import | 无法使用第三方库处理数据 |
| 编译时拦截 | 错误在保存函数时即暴露，不会延迟到 ingest |

#### 5.3.3 两类脚本的错误副作用控制差异

**VRL 错误处理**:

```rust
// apply_vrl_fn — 失败返回原始 row
match result {
    Ok(res) => match res.try_into() {
        Ok(val) => (val, None),
        Err(err) => (row, Some(clean_err))   // ← 原始数据回退
    },
    Err(err) => (row, Some(clean_err))       // ← 原始数据回退
}
```

- **副作用控制**: VRL 执行在独立 `TargetValueRef` 上操作，输入 `row` 不被修改
- **回退策略**: 失败 → 返回原始 `row`，错误记录但不阻断
- **度量**: `metrics::INGEST_ERRORS` 按 `[org, stream_type, stream_name, TRANSFORM_FAILED]` 上报

**JavaScript 错误处理**:

```rust
// apply_js_fn — 通过 JSON 序列化/反序列化隔离
let exec_code = format!(
    r#"(function() {{
        try {{
            var {var_name} = JSON.parse(inputJson);  // ← 反序列化隔离
            {func_for_execution}
            return JSON.stringify({{ success: true, data: {var_name} }});
        }} catch(e) {{
            return JSON.stringify({{
                success: false, error: e.name + ': ' + e.message,
                line: e.lineNumber || 'unknown',
                column: e.columnNumber || 'unknown'
            }});
        }}
    }})();"#,
    var_name, func_for_execution, var_name
);
```

- **副作用控制**: JS 通过 `JSON.parse(inputJson)` 创建**副本**，原始 `row` 不受 JS 执行影响
- **回退策略**: 与 VRL 相同 — 失败返回原始 `row`
- **额外信息**: JS 错误包含 `lineNumber` 和 `columnNumber`，方便定位
- **安全脱敏**: JS 错误消息不包含 `row` 数据内容，`log::error!` 中也仅记录错误消息

**关键差异对比**:

| 维度 | VRL | JavaScript |
|-----|-----|-----------|
| 输入隔离 | `vrl::value::Value::from(&row)` (引用) | `JSON.parse(inputJson)` (深拷贝) |
| 执行环境 | `Runtime::resolve` | `Context::eval` |
| 错误信息 | VRL 诊断 | JS 异常名+消息+行列号 |
| 内存控制 | 无显式限制 | 10MB / 512KB |
| Pipeline 可用 | ✅ | ❌ (L2 限制) |
| 组织限制 | 所有组织 | 仅 _meta (L1 限制) |
| 运行时状态 | `RuntimeState` (无副作用) | `thread_local!` 上下文 (隔离) |

### 5.4 限制体系的潜在风险与防御缺口

#### 5.4.1 编译失败 → 静默降级

**问题**: `ExecutablePipeline::new` 编译失败时，Pipeline 从缓存中移除，但**不产生用户可见的告警**。Ingest 请求会静默走无 Pipeline 路径，数据直接写入源流。

**场景**: 函数更新导致语法错误 → 关联 Pipeline 重新编译 → 失败 → Pipeline 从缓存消失 → 后续 ingest 不再执行任何转换。

**缓解**: 错误通过 `publish_error` 发布到 self_reporting 系统，但需要用户主动查看。

#### 5.4.2 L2 校验的 TOCTOU 窗口

**问题**: `validate_no_javascript_functions` 在保存时查询 DB 获取函数类型。若在保存 Pipeline 后、缓存构建前，函数被修改为 JS 类型，则：

- 校验通过时函数为 VRL
- 缓存构建时函数可能已变为 JS
- `register_functions` 会编译 JS 函数并放入 `CompiledFunctionRuntime::JS`

**实际风险**: 低。因为 `update_function` 会触发关联 Pipeline 的重新更新和缓存重建，最终一致性可保证。

#### 5.4.3 VRL `get_env_var` 的简单字符串匹配

**问题**: 使用 `func.contains("get_env_var")` 检测，可能被以下方式绕过：
- 字符串拼接: `get_" + "env_var"`
- 通过变量间接调用

**实际风险**: 低。VRL 编译器本身不支持字符串拼接调用函数，且 VRL 不支持反射。但更健壮的做法是在编译后检查 AST 中是否存在 `get_env_var` 函数调用节点。

#### 5.4.4 JS 危险模式的简单字符串匹配

**问题**: `DANGEROUS_PATTERNS` 使用 `contains()` 检测，可能被以下方式绕过：
- 注释中包含关键词: `// eval is not used` 会被误报
- 字符串值中包含关键词: `row.msg = "window.location"` 会被误报

**缓解**: 企业版使用 `o2_enterprise::enterprise::auth::js_security` 的集中式安全检查，可能有更精确的模式匹配。

### 5.5 限制体系关键代码索引

| 功能 | 文件位置 | 行号 |
|-----|---------|-----|
| L1: JS 组织限制 (save) | `src/service/functions.rs` | 68-73 |
| L1: JS 组织限制 (test) | `src/service/functions.rs` | 140-144 |
| L1: JS 组织限制 (update) | `src/service/functions.rs` | 360-364 |
| L2: Pipeline JS 拦截 | `src/service/pipeline/mod.rs` | 42-63 |
| L2: save_pipeline 调用 | `src/service/pipeline/mod.rs` | 89 |
| L2: update_pipeline 调用 | `src/service/pipeline/mod.rs` | 137 |
| L3: VRL get_env_var 禁止 | `src/service/ingestion/mod.rs` | 94-96 |
| L3: JS 危险模式列表 | `src/common/utils/js.rs` | 96-135 |
| L3: JS 运行时沙箱 | `src/common/utils/js.rs` | 19-53 |
| VRL 追加点号 | `src/service/functions.rs` | 79-81 |
| 编译失败静默降级 | `src/service/db/pipeline.rs` | 423-432 |
| 函数更新触发 Pipeline 重编 | `src/service/functions.rs` | 400-419 |
| 前端 JS 过滤 | `web/src/components/pipeline/PipelineEditor.vue` | 705-709 |
