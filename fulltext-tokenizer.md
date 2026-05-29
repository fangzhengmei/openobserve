# OpenObserve Full-Text Tokenizer 与索引字段协作分析

## 1. 概述

本文档深入分析 OpenObserve 中全文检索（Full-Text Search）相关的代码实现，重点关注三个核心方面：
- **Full-text fields 配置机制**：如何定义和管理需要进行全文索引的字段
- **Tokenizer 分词策略**：O2Tokenizer 的实现细节和分词逻辑
- **查询关键词定位 Parquet 分区**：如何利用倒排索引快速定位相关的 Parquet 文件

## 2. Full-Text Fields 配置机制

### 2.1 StreamSettings 结构体

在 `src/config/src/meta/stream.rs` 中定义了流的配置信息，其中 `full_text_search_keys` 字段用于存储需要进行全文索引的字段列表。

```rust
#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
pub struct StreamSettings {
    #[serde(default)]
    pub full_text_search_keys: Vec<String>,
    // 其他字段...
}
```

**关键点**：
- `full_text_search_keys` 是一个字符串向量，存储需要建立全文索引的字段名
- 当用户配置了全文索引字段后，这些字段的数据在写入时会被特殊处理
- 配置存储在元数据中，在查询和索引构建时都会用到

### 2.2 索引字段名称常量

系统定义了一个特殊的字段名 `INDEX_FIELD_NAME_FOR_ALL`，用于存储所有全文索引字段的合并内容，便于 `match_all()` 类型的查询。

## 3. Tokenizer 分词策略实现

### 3.1 O2Tokenizer 构建器

在 `src/config/src/utils/tantivy/tokenizer/mod.rs` 中定义了分词器的构建逻辑：

```rust
pub fn o2_tokenizer_build(collect_type: CollectType) -> TextAnalyzer {
    let cfg = get_config();
    let min_token_length =
        std::cmp::max(cfg.limit.inverted_index_min_token_length, MIN_TOKEN_LENGTH);
    let max_token_length =
        std::cmp::max(cfg.limit.inverted_index_max_token_length, MAX_TOKEN_LENGTH);
    tantivy::tokenizer::TextAnalyzer::builder(O2Tokenizer::new(collect_type))
        .filter(RemoveShortFilter::limit(min_token_length))
        .filter(tantivy::tokenizer::RemoveLongFilter::limit(max_token_length))
        .filter(tantivy::tokenizer::LowerCaser)
        .build()
}
```

**分词器流水线**：
1. **O2Tokenizer**：核心分词逻辑，处理驼峰命名、非ASCII字符等
2. **RemoveShortFilter**：移除过短的 token（默认最小长度 2）
3. **RemoveLongFilter**：移除过长的 token（默认最大长度 64）
4. **LowerCaser**：将所有 token 转换为小写

### 3.2 O2Tokenizer 核心实现

在 `src/config/src/utils/tantivy/tokenizer/o2_tokenizer.rs` 中实现了自定义分词逻辑。

#### 3.2.1 CollectType 枚举

```rust
pub enum CollectType {
    #[default]
    Ingest,
    Search,
}
```

- **Ingest 模式**：数据写入时的分词模式，会同时保留完整的驼峰词和拆分后的词
- **Search 模式**：查询时的分词模式，只使用拆分后的词进行搜索

#### 3.2.2 驼峰命名拆分

```rust
fn split_camel_case(input: &str) -> Vec<&str> {
    if looks_like_base64(input) {
        return vec![input];
    }
    let mut words = Vec::new();
    let mut start = 0;
    let mut prev_was_upper = false;
    for (i, c) in input.char_indices() {
        if c.is_uppercase() {
            if i != 0 && (!prev_was_upper || (i + 1 < input.len() && 
                input[i + 1..].chars().next().map(|c| c.is_lowercase()).unwrap_or(false))) {
                words.push(&input[start..i]);
                start = i;
            }
            prev_was_upper = true;
        } else {
            prev_was_upper = false;
        }
    }
    words.push(&input[start..]);
    words
}
```

**拆分规则**：
- Base64 编码的字符串不进行拆分
- "CamelCase" 拆分为 ["Camel", "Case"]
- "HTTPServer" 拆分为 ["HTTP", "Server"]
- "HelloWorld" 拆分为 ["Hello", "World"]

#### 3.2.3 分词流程

1. 扫描文本，跳过标点符号和空白字符
2. 遇到 ASCII 字母数字时，找到 token 结束位置（下一个非字母数字字符）
3. 检查是否包含驼峰命名（首字符后有大写字母）
4. 如果是驼峰命名：
   - Ingest 模式：先输出完整词，再输出拆分后的词
   - Search 模式：只输出拆分后的词
5. 非 ASCII 字符（如中文）单独作为一个 token

#### 3.2.4 搜索 token 收集

```rust
pub fn o2_collect_search_tokens(text: &str) -> Vec<String> {
    let mut a = o2_tokenizer_build(CollectType::Search);
    let mut token_stream = a.token_stream(text);
    let mut tokens: Vec<String> = Vec::new();
    let mut add_token = |token: &Token| {
        tokens.push(token.text.to_lowercase());
    };
    token_stream.process(&mut add_token);
    tokens
}
```

### 3.3 分词器注册

在索引打开时，需要将 O2Tokenizer 注册到 Tantivy：

```rust
// src/service/search/grpc/storage.rs:770-773
let index = tantivy::Index::open(reader_directory)?;
index
    .tokenizers()
    .register(O2_TOKENIZER, o2_tokenizer_build(CollectType::Search));
```

## 4. 查询处理流程

### 4.1 IndexCondition 与 Condition

在 `src/service/search/index.rs` 中定义了查询条件的抽象表示。

#### 4.1.1 Condition 枚举

```rust
pub enum Condition {
    Equal(String, String),           // 字段等于值
    NotEqual(String, String),        // 字段不等于值
    StrMatch(String, String, bool),  // 字符串匹配（字段、值、大小写敏感）
    In(String, Vec<String>, bool),   // 字段在值列表中
    Regex(String, String),           // 正则匹配
    MatchAll(String),                // 匹配所有全文字段
    FuzzyMatchAll(String, u8),       // 模糊匹配所有全文字段
    All(),                           // 匹配所有文档
    Or(Box<Condition>, Box<Condition>),
    And(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
}
```

#### 4.1.2 IndexCondition 结构体

```rust
pub struct IndexCondition {
    pub conditions: Vec<Condition>,  // 多个条件通过 AND 连接
}
```

**关键方法**：
- `to_tantivy_query()`：将条件转换为 Tantivy 查询
- `to_physical_expr()`：转换为 DataFusion 物理表达式（用于需要回退过滤的情况）
- `can_remove_filter()`：检查是否可以安全地移除 DataFusion 过滤器

### 4.2 从物理表达式提取条件

```rust
pub fn from_physical_expr(expr: &Arc<dyn PhysicalExpr>) -> Self {
    if let Some(expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
        // 处理比较运算符
    } else if let Some(expr) = expr.as_any().downcast_ref::<InListExpr>() {
        // 处理 IN 表达式
    } else if let Some(expr) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
        let name = expr.name();
        match name {
            MATCH_ALL_UDF_NAME => Condition::MatchAll(get_physical_value(&expr.args()[0])),
            STR_MATCH_UDF_NAME => Condition::StrMatch(field, value, true),
            // ... 其他 UDF
        }
    }
    // ...
}
```

### 4.3 转换为 Tantivy 查询

```rust
pub fn to_tantivy_query(
    &self,
    schema: &Schema,
    default_field: Option<Field>,
) -> anyhow::Result<Box<dyn Query>> {
    Ok(match self {
        Condition::Equal(field, value) => {
            let field = schema.get_field(field)?;
            let term = Term::from_field_text(field, value);
            Box::new(TermQuery::new(term, IndexRecordOption::Basic))
        }
        Condition::StrMatch(field, value, case_sensitive) => {
            let field = schema.get_field(field)?;
            Box::new(ContainsQuery::new(value, field, *case_sensitive)?)
        }
        Condition::MatchAll(value) => {
            let default_field = default_field.ok_or_else(|| {
                anyhow::anyhow!("There's no FullTextSearch field for match_all() function")
            })?;
            // 使用 o2_collect_search_tokens 分词后构建查询
            let mut tokens = o2_collect_search_tokens(value);
            // 处理通配符、构建多个 TermQuery 的布尔组合
        }
        // ... 其他条件类型
    })
}
```

## 5. 物理优化器：IndexRule

在 `src/service/search/datafusion/optimizer/physical_optimizer/index.rs` 中实现了基于索引的查询优化规则。

### 5.1 IndexRule 结构体

```rust
pub struct IndexRule {
    index_fields: HashSet<String>,           // 可用于索引的字段
    index_condition: Arc<Mutex<Option<IndexCondition>>>,  // 提取出的索引条件
    pub can_optimize: Arc<AtomicBool>,       // 是否可以优化
}
```

### 5.2 优化流程

1. 遍历执行计划，找到 FilterExec 节点
2. 将过滤谓词拆分为多个合取项
3. 检查每个项是否可以使用索引优化
4. 将可优化的条件提取为 IndexCondition
5. 如果所有条件都可优化且启用了移除过滤器功能，则移除 FilterExec
6. 否则保留原始过滤器（用于精确匹配）

```rust
fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
    if let Some(filter) = node.as_any().downcast_ref::<FilterExec>() {
        self.has_filter = true;
        let mut index_conditions = IndexCondition::new();
        let mut other_conditions = Vec::new();
        for expr in split_conjunction(filter.predicate()) {
            if is_expr_valid_for_index(expr, &self.index_fields) {
                let condition = Condition::from_physical_expr(expr);
                index_conditions.add_condition(condition);
            } else {
                other_conditions.push(expr.clone());
            }
        }
        // 检查是否可以移除过滤器
        let is_remove_filter = self.is_remove_filter || index_conditions.can_remove_filter();
        // 保存索引条件供后续使用
        if !index_conditions.is_empty() {
            *self.index_condition.lock() = Some(index_conditions);
        }
        // 根据 is_remove_filter 决定是否保留过滤器
    }
}
```

## 6. 查询关键词定位 Parquet 分区实现

### 6.1 整体流程

```
用户查询
    ↓
SQL 解析 → 逻辑计划 → 物理计划
    ↓
IndexRule 优化器提取 IndexCondition
    ↓
tantivy_search() 使用倒排索引过滤文件
    ↓
search_tantivy_index() 逐个文件搜索
    ↓
返回匹配的行号位图（BitVec）或直接返回聚合结果
    ↓
create_tables_from_files() 只扫描过滤后的文件和行
    ↓
DataFusion 执行查询（如果需要，应用回退过滤器）
```

### 6.2 tantivy_search 函数

在 `src/service/search/grpc/storage.rs:420` 中实现了使用倒排索引过滤 Parquet 文件列表的逻辑。

```rust
pub async fn tantivy_search(
    query: Arc<super::QueryParams>,
    file_list: &mut Vec<FileKey>,
    index_condition: Option<IndexCondition>,
    idx_optimize_mode: Option<IndexOptimizeMode>,
) -> Result<(usize, bool, TantivyMultiResult), Error> {
    // 1. 缓存索引文件
    let index_file_names = file_list_map
        .iter()
        .filter_map(|(_, f)| {
            if f.meta.index_size > 0 {
                convert_parquet_file_name_to_tantivy_file(&f.key)
                    .map(|ttv_file| (ttv_file, f.clone()))
            } else {
                None
            }
        })
        .collect_vec();
    
    // 2. 按时间范围分区文件，便于并行处理
    let (index_parquet_files, query_limit) =
        partition_tantivy_files(index_parquet_files, &idx_optimize_mode, target_partitions);
    
    // 3. 并行搜索每个文件组
    for file_group in index_parquet_files {
        let mut tasks = Vec::new();
        for file in file_group {
            let task = tokio::task::spawn(async move {
                search_tantivy_index(
                    &trace_id,
                    time_range,
                    index_condition_clone,
                    idx_optimize_rule_clone,
                    &file,
                )
                .await
            });
            tasks.push(task)
        }
        
        // 4. 处理搜索结果
        while let Some(result) = tasks.try_next().await {
            match result {
                Ok((file_name, result, has_skipped_conditions)) => {
                    match result {
                        TantivyResult::RowIdsBitVec(num_rows, bitvec) => {
                            if num_rows == 0 {
                                // 没有匹配，从文件列表中移除
                                file_list_map.remove(&file_name);
                            } else {
                                // 保存行号位图，后续只扫描这些行
                                let file = file_list_map.get_mut(&file_name).unwrap();
                                file.with_segment_ids(bitvec);
                            }
                        }
                        TantivyResult::Count(count) => {
                            // 聚合查询，直接返回计数，无需扫描 Parquet
                            tantivy_result_builder.add_row_nums(count as u64);
                            file_list_map.remove(&file_name);
                        }
                        // ... 其他结果类型
                    }
                }
            }
        }
    }
}
```

**关键优化点**：
- 跳过没有索引的文件（`index_size == 0`）
- 按时间范围分组文件，控制并行度
- 如果索引搜索返回的行数过多（超过阈值比例），回退到完整扫描
- 支持多种优化模式（Count、Histogram、TopN、Distinct）直接从索引返回结果

### 6.3 search_tantivy_index 函数

在 `src/service/search/grpc/storage.rs:724` 中实现了单个 Tantivy 索引文件的搜索。

```rust
async fn search_tantivy_index(
    trace_id: &str,
    time_range: (i64, i64),
    index_condition: Option<IndexCondition>,
    idx_optimize_rule: Option<IndexOptimizeMode>,
    parquet_file: &FileKey,
) -> anyhow::Result<(String, TantivyResult, bool)> {
    // 1. 检查结果缓存
    if cfg.common.inverted_index_result_cache_enabled {
        cache_key = generate_cache_key(&index_condition, &idx_optimize_rule, parquet_file);
        if let Some(result) = tantivy_result_cache::GLOBAL_CACHE.get(&cache_key) {
            return Ok((parquet_file.key.to_string(), result, false));
        }
    }
    
    // 2. 打开 Tantivy 索引
    let puffin_dir = Arc::new(
        get_tantivy_directory(trace_id, &file_account, &ttv_file_name, parquet_file.meta.index_size)
            .await?,
    );
    let index = tantivy::Index::open(reader_directory)?;
    index.tokenizers().register(O2_TOKENIZER, o2_tokenizer_build(CollectType::Search));
    let reader = index.reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .num_warming_threads(0)
        .try_into()?;
    let searcher = tantivy_reader.searcher();
    
    // 3. 构建 Tantivy 查询
    let (mut query, has_skipped_conditions) =
        condition.to_tantivy_query(trace_id, tantivy_schema.clone(), fts_field)?;
    
    // 4. 添加时间范围过滤（如果文件不完全在时间范围内）
    if !file_in_range && let Ok(ts_field) = tantivy_schema.get_field(TIMESTAMP_COL_NAME) {
        let ts_range = RangeQuery::new(
            Bound::Included(Term::from_field_i64(ts_field, start_time)),
            Bound::Excluded(Term::from_field_i64(ts_field, end_time)),
        );
        query = Box::new(BooleanQuery::new(vec![
            (Occur::Must, query),
            (Occur::Must, Box::new(ts_range)),
        ]));
    }
    
    // 5. 预热查询词项（提高搜索性能）
    warm_up_terms(&searcher, &warm_terms, need_all_term_fields, need_fast_field).await?;
    
    // 6. 执行搜索，根据优化模式返回不同结果
    let res = tokio::task::spawn_blocking(move || match idx_optimize_rule {
        None => TantivyResult::handle_matched_docs(&searcher, query),
        Some(IndexOptimizeMode::SimpleCount) => {
            TantivyResult::handle_simple_count(&searcher, query)
        }
        Some(IndexOptimizeMode::SimpleHistogram(..)) => {
            TantivyResult::handle_simple_histogram(...)
        }
        Some(IndexOptimizeMode::SimpleTopN(..)) => {
            TantivyResult::handle_simple_top_n(...)
        }
        Some(IndexOptimizeMode::SimpleDistinct(..)) => {
            TantivyResult::handle_simple_distinct(...)
        }
    })
    .await??;
    
    // 7. 结果后处理
    let result = match res {
        TantivyResult::RowIds(row_ids) => {
            // 检查匹配行数是否过多
            let row_ids_percent = row_ids.len() as f64 / parquet_file.meta.records as f64 * 100.0;
            if skip_threshold > 0 && row_ids_percent > skip_threshold as f64 {
                // 匹配过多，回退到完整扫描
                return Ok(("".to_string(), TantivyResult::RowIdsBitVec(...), true));
            }
            // 转换为 BitVec 存储
            let mut res = BitVec::repeat(false, parquet_file.meta.records as usize);
            for id in row_ids {
                res.set(id as usize, true);
            }
            TantivyResult::RowIdsBitVec(num_rows, res)
        }
        // ... 其他结果类型
    };
    
    // 8. 缓存结果
    if cfg.common.inverted_index_result_cache_enabled && !has_skipped_conditions {
        tantivy_result_cache::GLOBAL_CACHE.put(cache_key, entry);
    }
    
    Ok((key, result, has_skipped_conditions))
}
```

**关键技术点**：
- **结果缓存**：相同查询条件的结果可以直接从缓存返回
- **时间范围过滤**：如果 Parquet 文件不完全在查询时间范围内，需要额外的时间过滤
- **词项预热**：提前加载查询词项到内存，提高搜索速度
- **智能回退**：如果索引搜索结果不够精确（有跳过的条件）或匹配行数过多，回退到 DataFusion 完整扫描
- **BitVec 压缩**：使用位向量存储匹配的行号，节省内存

### 6.4 索引优化模式

`IndexOptimizeMode` 定义了可以直接从索引返回的聚合类型：

```rust
pub enum IndexOptimizeMode {
    SimpleSelect(limit, ascend),     // 简单选择，返回行号
    SimpleCount,                     // 计数查询，直接返回 count
    SimpleHistogram(min, width, num),// 直方图查询
    SimpleTopN(field, limit, ascend),// TopN 查询
    SimpleDistinct(field, limit, ascend), // 去重查询
}
```

这些优化模式允许某些查询完全跳过 Parquet 文件扫描，直接从倒排索引返回结果。

## 7. 组件协作关系图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        用户查询处理流程                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌──────────────────────┐    │
│  │  SQL 解析器 │───▶│  逻辑计划   │───▶│  物理计划生成器       │    │
│  └─────────────┘    └─────────────┘    └─────────┬────────────┘    │
│                                                  │                 │
│                        ┌─────────────────────────┘                 │
│                        ▼                                           │
│           ┌───────────────────────────────┐                         │
│           │      IndexRule 优化器         │                         │
│           │  ┌─────────────────────────┐  │                         │
│           │  │ 1. 识别可索引字段       │  │                         │
│           │  │ 2. 提取 IndexCondition  │  │                         │
│           │  │ 3. 决定是否移除过滤器   │  │                         │
│           │  └─────────────────────────┘  │                         │
│           └─────────────┬─────────────────┘                         │
│                         ▼                                           │
│           ┌───────────────────────────────┐                         │
│           │    tantivy_search()           │                         │
│           │  ┌─────────────────────────┐  │                         │
│           │  │ 1. 过滤有索引的文件     │  │                         │
│           │  │ 2. 按时间分区分组       │  │                         │
│           │  │ 3. 并行搜索索引文件     │  │                         │
│           │  └─────────────────────────┘  │                         │
│           └─────────────┬─────────────────┘                         │
│                         ▼                                           │
│           ┌───────────────────────────────┐                         │
│           │  search_tantivy_index()       │                         │
│           │  ┌─────────────────────────┐  │                         │
│           │  │ 1. 打开 Tantivy 索引    │  │                         │
│           │  │ 2. 注册 O2Tokenizer     │  │                         │
│           │  │ 3. 构建查询 + 时间过滤  │  │                         │
│           │  │ 4. 执行搜索              │  │                         │
│           │  │ 5. 返回 BitVec/聚合结果 │  │                         │
│           │  └─────────────────────────┘  │                         │
│           └─────────────┬─────────────────┘                         │
│                         ▼                                           │
│           ┌───────────────────────────────┐                         │
│           │ create_tables_from_files()    │                         │
│           │  只扫描过滤后的文件和行        │                         │
│           └─────────────┬─────────────────┘                         │
│                         ▼                                           │
│           ┌───────────────────────────────┐                         │
│           │   DataFusion 执行引擎         │                         │
│           │   (如需要，应用回退过滤器)     │                         │
│           └─────────────┬─────────────────┘                         │
│                         ▼                                           │
│                  查询结果返回用户                                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      O2Tokenizer 分词流程                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  输入文本: "HelloWorld from OpenObserve 2024"                        │
│                                                                     │
│  ┌───────────────────────────────────────────────────────┐          │
│  │ Ingest 模式（数据写入）                                │          │
│  ├───────────────────────────────────────────────────────┤          │
│  │ 1. "HelloWorld" → ["HelloWorld", "Hello", "World"]    │          │
│  │ 2. "from" → ["from"]                                  │          │
│  │ 3. "OpenObserve" → ["OpenObserve", "Open", "Observe"] │          │
│  │ 4. "2024" → ["2024"]                                  │          │
│  └───────────────────────────────────────────────────────┘          │
│                                                                     │
│  ┌───────────────────────────────────────────────────────┐          │
│  │ Search 模式（查询时）                                  │          │
│  ├───────────────────────────────────────────────────────┤          │
│  │ 1. "HelloWorld" → ["Hello", "World"]                  │          │
│  │ 2. "from" → ["from"]                                  │          │
│  │ 3. "OpenObserve" → ["Open", "Observe"]                │          │
│  │ 4. "2024" → ["2024"]                                  │          │
│  └───────────────────────────────────────────────────────┘          │
│                                                                     │
│  后续过滤器: RemoveShort → RemoveLong → LowerCaser                   │
│  最终: ["helloworld", "hello", "world", "from", "openobserve",      │
│         "open", "observe", "2024"]  (Ingest)                        │
│  最终: ["hello", "world", "from", "open", "observe", "2024"]        │
│         (Search)                                                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 8. 关键文件汇总

| 文件路径 | 核心功能 |
|---------|---------|
| `src/config/src/meta/stream.rs` | StreamSettings 定义，full_text_search_keys 配置 |
| `src/config/src/utils/tantivy/tokenizer/mod.rs` | O2Tokenizer 构建器，分词流水线 |
| `src/config/src/utils/tantivy/tokenizer/o2_tokenizer.rs` | O2Tokenizer 核心实现，驼峰拆分逻辑 |
| `src/service/search/index.rs` | IndexCondition 和 Condition 定义，查询转换 |
| `src/service/search/datafusion/optimizer/physical_optimizer/index.rs` | IndexRule 物理优化器 |
| `src/service/search/grpc/storage.rs` | tantivy_search 和 search_tantivy_index 实现 |
| `src/service/tantivy/mod.rs` | Tantivy 索引创建，文档添加 |
| `src/service/search/datafusion/optimizer/physical_optimizer/rewrite_match.rs` | match_all UDF 重写为 LIKE 表达式 |

## 9. 设计亮点

1. **智能分词策略**：O2Tokenizer 支持驼峰命名拆分，Ingest/Search 双模式，兼顾索引完整性和查询效率

2. **多层过滤机制**：
   - 第一层：倒排索引快速过滤不相关的 Parquet 文件
   - 第二层：BitVec 行号位图只扫描匹配的行
   - 第三层：DataFusion 过滤器确保精确匹配（需要时）

3. **结果缓存**：索引搜索结果可缓存，重复查询性能提升显著

4. **智能回退**：
   - 索引不存在或过旧时，自动回退到完整扫描
   - 匹配行数过多时，回退避免位图过大
   - 条件无法完全用索引表达时，保留 DataFusion 过滤器

5. **聚合优化**：Count、Histogram、TopN、Distinct 等聚合查询可直接从索引返回结果，完全跳过 Parquet 扫描

6. **时间分区**：利用时间范围分区，减少不必要的索引搜索

## 10. 性能优化建议

1. **合理配置全文索引字段**：只对真正需要全文搜索的字段建立索引，减少索引大小

2. **调整分词参数**：根据实际数据特点调整 `inverted_index_min_token_length` 和 `inverted_index_max_token_length`

3. **启用结果缓存**：对于重复查询较多的场景，启用 `inverted_index_result_cache_enabled`

4. **合理设置跳过阈值**：`inverted_index_skip_threshold` 控制何时回退到完整扫描，需要在索引效率和扫描效率之间平衡

5. **利用时间分区**：查询时尽量指定时间范围，减少需要搜索的索引文件数量
