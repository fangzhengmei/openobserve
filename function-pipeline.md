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
