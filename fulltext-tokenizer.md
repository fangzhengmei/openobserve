# OpenObserve Full-Text Tokenizer 与索引字段协作分析

## 1. 概述

本文档深入分析 OpenObserve 中全文检索（Full-Text Search）相关的代码实现，重点关注三个核心方面：
- **Full-text fields 配置机制**：如何定义和管理需要进行全文索引的字段
- **Tokenizer 分词策略**：O2Tokenizer 的实现细节和分词逻辑
- **查询关键词定位 Parquet 分区**：如何利用倒排索引快速定位相关的 Parquet 文件

**特别补充**：配置值从 stream settings 进入倒排索引写入、查询阶段如何读取并匹配这些字段，以及 tokenizer 在整条路径里的协作关系。

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

### 2.2 获取全文搜索字段的函数

在 `src/infra/src/schema/mod.rs:348` 中定义了获取完整全文搜索字段的函数：

```rust
pub fn get_stream_setting_fts_fields(settings: &Option<StreamSettings>) -> Vec<String> {
    let default_fields = SQL_FULL_TEXT_SEARCH_FIELDS.clone();
    match settings {
        Some(settings) => {
            let mut fields = settings.full_text_search_keys.clone();
            fields.extend(default_fields);
            if settings.index_original_data {
                fields.push(ORIGINAL_DATA_COL_NAME.to_string());
            }
            if settings.index_all_values {
                fields.push(ALL_VALUES_COL_NAME.to_string());
            }
            fields.sort();
            fields.dedup();
            fields
        }
        None => default_fields,
    }
}
```

**字段组成逻辑**：
1. **用户配置字段**：`settings.full_text_search_keys` 中用户显式配置的字段
2. **默认字段**：`SQL_FULL_TEXT_SEARCH_FIELDS` 环境变量配置的默认字段
3. **原始数据字段**：如果 `index_original_data` 为 true，添加 `_original` 字段
4. **所有值字段**：如果 `index_all_values` 为 true，添加 `_all_values` 字段
5. **去重排序**：对所有字段进行排序和去重

### 2.3 索引字段名称常量

系统定义了一个特殊的字段名 `INDEX_FIELD_NAME_FOR_ALL`，用于存储所有全文索引字段的合并内容，便于 `match_all()` 类型的查询。

## 3. 数据写入与索引创建链路

### 3.1 完整链路概览

```
用户配置 StreamSettings.full_text_search_keys
    ↓
get_stream_setting_fts_fields() 组合完整字段列表
    ↓
数据写入时传递给 create_tantivy_index()
    ↓
generate_tantivy_index() 创建 Tantivy 索引
    ↓
所有全文字段数据合并到 INDEX_FIELD_NAME_FOR_ALL 字段
    ↓
使用 O2Tokenizer (Ingest 模式) 分词后写入索引
```

### 3.2 调用链分析

#### 3.2.1 数据写入阶段

在 `src/job/files/parquet.rs:709-712` 中获取字段配置：

```rust
let stream_settings = infra::schema::unwrap_stream_settings(&latest_schema);
let full_text_search_fields = get_stream_setting_fts_fields(&stream_settings);
let index_fields = get_stream_setting_index_fields(&stream_settings);
```

然后在 `src/job/files/parquet.rs:924-933` 中调用索引创建：

```rust
let index_size = create_tantivy_index(
    "INGESTER",
    &org_id,
    &new_file_key,
    &full_text_search_fields,
    &index_fields,
    latest_schema.clone(),
    reader,
)
.await
.map_err(|e| anyhow::anyhow!("generate_tantivy_index_on_ingester error: {e}"))?;
```

#### 3.2.2 索引创建阶段

在 `src/service/tantivy/mod.rs:77-134` 的 `create_tantivy_index` 函数中：

```rust
pub(crate) async fn create_tantivy_index(
    caller: &str,
    org_id: &str,
    parquet_file_name: &str,
    full_text_search_fields: &[String],  // 全文搜索字段列表
    index_fields: &[String],              // 二级索引字段列表
    schema: Arc<Schema>,
    reader: RecordBatchStream,
) -> Result<usize, anyhow::Error> {
    let dir = PuffinDirWriter::new();
    let index = generate_tantivy_index(
        dir.clone(),
        reader,
        full_text_search_fields,
        index_fields,
        schema,
    )
    .await?;
    // ... 写入索引文件
}
```

### 3.3 Tantivy 索引 Schema 构建

在 `src/service/tantivy/mod.rs:137-204` 的 `generate_tantivy_index` 函数中：

#### 3.3.1 字段过滤与合并

```rust
// filter out fields that are not in schema & not of type Utf8
let fts_fields = full_text_search_fields
    .iter()
    .filter(|f| {
        schema_fields
            .get(f)
            .map(|v| v.data_type() == &DataType::Utf8 || v.data_type() == &DataType::LargeUtf8)
            .is_some()
    })
    .map(String::from)
    .collect::<HashSet<_>>();

let index_fields = index_fields
    .iter()
    .filter(|f| schema_fields.contains_key(f))
    .map(String::from)
    .collect::<HashSet<_>>();

let tantivy_fields = fts_fields
    .union(&index_fields)
    .cloned()
    .collect::<HashSet<_>>();
```

#### 3.3.2 创建特殊的全文字段

**关键设计**：所有全文搜索字段的数据都会被写入到同一个 `INDEX_FIELD_NAME_FOR_ALL` 字段中！

```rust
// add fields to tantivy schema
if !full_text_search_fields.is_empty() {
    let fts_opts = tantivy::schema::TextOptions::default().set_indexing_options(
        tantivy::schema::TextFieldIndexing::default()
            .set_index_option(tantivy::schema::IndexRecordOption::Basic)
            .set_tokenizer(O2_TOKENIZER)        // 使用 O2Tokenizer 分词
            .set_fieldnorms(false),
    );
    tantivy_schema_builder.add_text_field(INDEX_FIELD_NAME_FOR_ALL, fts_opts);
}
```

**注意**：
- 全文搜索字段在 Tantivy Schema 中**不创建独立字段**
- 只创建一个 `INDEX_FIELD_NAME_FOR_ALL` 字段，使用 `O2_TOKENIZER` 分词器
- 二级索引字段（`index_fields`）会创建独立的字段，使用 `raw` 分词器

### 3.4 文档写入与字段合并

在 `src/service/tantivy/mod.rs:233-267` 中处理每个字段的数据：

```rust
// process full text search fields
let mut docs = vec![tantivy::doc!(); num_rows];
for column_name in tantivy_fields.iter() {
    // get field
    let field = match tantivy_schema.get_field(column_name) {
        Ok(f) => f,
        Err(_) => fts_field.unwrap(),  // 关键：如果字段不在 schema 中，使用 INDEX_FIELD_NAME_FOR_ALL
    };

    // get column data and convert to strings for indexing
    if let Some(data) = inverted_idx_batch.column_by_name(column_name) {
        // handle string types directly
        process_string_array!(data, StringViewArray, docs, field);
        process_string_array!(data, StringArray, docs, field);
        process_string_array!(data, LargeStringArray, docs, field);
        // ... handle numeric types
    } else {
        // column not found, add empty string
        for doc in docs.iter_mut() {
            doc.add_text(field, "");
        }
    }
}
```

**核心逻辑解释**：

1. `tantivy_fields` 包含 `fts_fields` ∪ `index_fields`
2. 对于 `index_fields` 中的字段：
   - 在 Tantivy Schema 中存在独立的字段（使用 `raw` 分词器）
   - 数据写入对应字段
3. 对于 `fts_fields` 中的字段：
   - 在 Tantivy Schema 中**不存在**独立字段
   - 匹配 `Err(_)` 分支，使用 `fts_field.unwrap()` 即 `INDEX_FIELD_NAME_FOR_ALL`
   - **所有全文字段的数据都被追加到同一个字段中！**

**示例**：
如果配置了 `full_text_search_keys = ["message", "service_name"]`：
- 文档的 `message` 字段内容 → 写入 `INDEX_FIELD_NAME_FOR_ALL`
- 文档的 `service_name` 字段内容 → 也写入 `INDEX_FIELD_NAME_FOR_ALL`
- 结果：`INDEX_FIELD_NAME_FOR_ALL` 包含所有全文字段的内容拼接

### 3.5 分词器注册（Ingest 模式）

在 `src/service/tantivy/mod.rs:206-207` 中：

```rust
let tokenizer_manager = tantivy::tokenizer::TokenizerManager::default();
tokenizer_manager.register(O2_TOKENIZER, o2_tokenizer_build(CollectType::Ingest));
```

**Ingest 模式特点**：
- 驼峰命名会同时保留完整词和拆分后的词
- 例如："HelloWorld" → ["HelloWorld", "Hello", "World"]
- 索引更完整，但占用更多存储空间

## 4. Tokenizer 分词策略实现

### 4.1 O2Tokenizer 构建器

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

### 4.2 O2Tokenizer 核心实现

在 `src/config/src/utils/tantivy/tokenizer/o2_tokenizer.rs` 中实现了自定义分词逻辑。

#### 4.2.1 CollectType 枚举

```rust
pub enum CollectType {
    #[default]
    Ingest,
    Search,
}
```

- **Ingest 模式**：数据写入时的分词模式，会同时保留完整的驼峰词和拆分后的词
- **Search 模式**：查询时的分词模式，只使用拆分后的词进行搜索

#### 4.2.2 驼峰命名拆分

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

#### 4.2.3 分词流程

1. 扫描文本，跳过标点符号和空白字符
2. 遇到 ASCII 字母数字时，找到 token 结束位置（下一个非字母数字字符）
3. 检查是否包含驼峰命名（首字符后有大写字母）
4. 如果是驼峰命名：
   - Ingest 模式：先输出完整词，再输出拆分后的词
   - Search 模式：只输出拆分后的词
5. 非 ASCII 字符（如中文）单独作为一个 token

#### 4.2.4 搜索 token 收集

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

### 4.3 分词器注册时机

| 阶段 | 位置 | 模式 | 用途 |
|------|------|------|------|
| 索引创建 | `src/service/tantivy/mod.rs:206-207` | Ingest | 写入文档时分词 |
| 查询搜索 | `src/service/search/grpc/storage.rs:770-773` | Search | 搜索索引时分词 |

## 5. 查询处理流程

### 5.1 完整查询链路

```
用户 SQL 包含 match_all('keyword')
    ↓
SQL 解析 → MatchVisitor 检查流是否有全文搜索字段
    ↓
逻辑计划 → 物理计划
    ↓
IndexRule 优化器提取 IndexCondition (Condition::MatchAll)
    ↓
tantivy_search() 开始处理
    ↓
search_tantivy_index() 打开索引并注册 O2Tokenizer (Search 模式)
    ↓
Condition::MatchAll → to_tantivy_query() 调用 o2_collect_search_tokens
    ↓
每个 token 在 INDEX_FIELD_NAME_FOR_ALL 字段中搜索
    ↓
返回匹配的行号或聚合结果
```

### 5.2 IndexCondition 与 Condition

在 `src/service/search/index.rs` 中定义了查询条件的抽象表示。

#### 5.2.1 Condition 枚举

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

#### 5.2.2 IndexCondition 结构体

```rust
pub struct IndexCondition {
    pub conditions: Vec<Condition>,  // 多个条件通过 AND 连接
}
```

**关键方法**：
- `to_tantivy_query()`：将条件转换为 Tantivy 查询
- `to_physical_expr()`：转换为 DataFusion 物理表达式（用于需要回退过滤的情况）
- `can_remove_filter()`：检查是否可以安全地移除 DataFusion 过滤器

### 5.3 MatchAll 转换为 Tantivy 查询

在 `src/service/search/index.rs:370-422` 中，`MatchAll` 条件的转换是全文搜索的核心：

```rust
Condition::MatchAll(value) => {
    let default_field = default_field.ok_or_else(|| {
        anyhow::anyhow!("There's no FullTextSearch field for match_all() function")
    })?;
    if value.is_empty() || value == "*" {
        Box::new(AllQuery {})
    } else {
        // 关键：使用 Search 模式对查询关键词分词
        let mut tokens = o2_collect_search_tokens(value);
        let contains_search =
            tokens.len() == 1 && value.starts_with("*") && value.ends_with("*");
        let first_prefix = if value.starts_with("*") && !tokens.is_empty() {
            Some(tokens.remove(0))
        } else {
            None
        };
        let last_prefix = if value.ends_with("*") {
            tokens.pop()
        } else {
            None
        };
        // 每个分词结果构建一个 TermQuery
        let mut terms: Vec<Box<dyn Query>> = tokens
            .into_iter()
            .map(|value| {
                let term = Term::from_field_text(default_field, &value);
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as _
            })
            .collect();
        // 处理通配符前缀
        if let Some(value) = first_prefix {
            terms.push(if contains_search {
                Box::new(ContainsQuery::new_case_insensitive(&value, default_field)?)
            } else {
                let value = format!(".*{value}");
                Box::new(RegexQuery::from_pattern(&value, default_field)?)
            });
        }
        if let Some(value) = last_prefix {
            terms.push(Box::new(PhrasePrefixQuery::new_with_offset(vec![(
                0,
                Term::from_field_text(default_field, &value),
            )])));
        }
        // 所有 token 必须同时匹配（AND 关系）
        if !terms.is_empty() {
            if terms.len() > 1 {
                Box::new(BooleanQuery::intersection(terms))
            } else {
                terms.remove(0)
            }
        } else {
            return Err(anyhow::anyhow!(
                "The value of match_all() function can't be empty"
            ));
        }
    }
}
```

**关键逻辑**：
1. 使用 `o2_collect_search_tokens(value)` 对查询关键词进行分词（Search 模式）
2. 每个分词结果在 `default_field`（即 `INDEX_FIELD_NAME_FOR_ALL`）中搜索
3. 多个分词结果使用 `BooleanQuery::intersection`（AND 关系）
4. 支持通配符前缀和后缀

### 5.4 物理优化器：IndexRule

在 `src/service/search/datafusion/optimizer/physical_optimizer/index.rs` 中实现了基于索引的查询优化规则。

#### 5.4.1 IndexRule 结构体

```rust
pub struct IndexRule {
    index_fields: HashSet<String>,           // 可用于索引的字段
    index_condition: Arc<Mutex<Option<IndexCondition>>>,  // 提取出的索引条件
    pub can_optimize: Arc<AtomicBool>,       // 是否可以优化
}
```

#### 5.4.2 优化流程

1. 遍历执行计划，找到 FilterExec 节点
2. 将过滤谓词拆分为多个合取项
3. 检查每个项是否可以使用索引优化
4. 将可优化的条件提取为 IndexCondition
5. 如果所有条件都可优化且启用了移除过滤器功能，则移除 FilterExec
6. 否则保留原始过滤器（用于精确匹配）

### 5.5 查询时的分词器注册

在 `src/service/search/grpc/storage.rs:770-773` 中：

```rust
let index = tantivy::Index::open(reader_directory)?;
index
    .tokenizers()
    .register(O2_TOKENIZER, o2_tokenizer_build(CollectType::Search));
```

**Search 模式特点**：
- 驼峰命名只输出拆分后的词
- 例如："HelloWorld" → ["Hello", "World"]
- 索引更高效，匹配更准确

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

## 7. 完整链路与组件协作

### 7.1 配置 → 索引写入链路

```
┌─────────────────────────────────────────────────────────────────────┐
│                   Full-Text Fields 配置流                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 1. 用户配置 StreamSettings                                    │  │
│  │    full_text_search_keys: ["message", "service"]             │  │
│  └─────────────────────────────┬────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 2. get_stream_setting_fts_fields()                           │  │
│  │    + 用户配置字段                                             │  │
│  │    + SQL_FULL_TEXT_SEARCH_FIELDS (默认)                       │  │
│  │    + _original (如果 index_original_data)                     │  │
│  │    + _all_values (如果 index_all_values)                      │  │
│  └─────────────────────────────┬────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 3. create_tantivy_index()                                   │  │
│  │    接收 full_text_search_fields 参数                         │  │
│  └─────────────────────────────┬────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 4. generate_tantivy_index()                                 │  │
│  │    ├─ 创建 INDEX_FIELD_NAME_FOR_ALL 字段                      │  │
│  │    │   (使用 O2_TOKENIZER, Ingest 模式)                       │  │
│  │    └─ 所有 fts_fields 数据写入该字段                           │  │
│  └─────────────────────────────┬────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 5. 索引写入完成                                               │  │
│  │    Tantivy 索引文件存储到对象存储                               │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 7.2 查询 → 匹配链路

```
┌─────────────────────────────────────────────────────────────────────┐
│                   Full-Text Query 匹配流                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 1. 用户 SQL 查询                                              │  │
│  │    SELECT * FROM logs WHERE match_all('error')                │  │
│  └─────────────────────────────┬────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 2. MatchVisitor 检查                                          │  │
│  │    确认流有全文搜索字段配置                                     │  │
│  └─────────────────────────────┬────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 3. IndexRule 优化器                                           │  │
│  │    提取 Condition::MatchAll("error")                          │  │
│  └─────────────────────────────┬────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 4. search_tantivy_index()                                    │  │
│  │    注册 O2Tokenizer (Search 模式)                             │  │
│  └─────────────────────────────┬────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 5. to_tantivy_query()                                        │  │
│  │    o2_collect_search_tokens("error")                          │  │
│  │    → ["error"]                                               │  │
│  │    在 INDEX_FIELD_NAME_FOR_ALL 字段中搜索                      │  │
│  └─────────────────────────────┬────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 6. 返回结果                                                   │  │
│  │    匹配的行号 BitVec 或聚合结果                                │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 7.3 Tokenizer 在整条路径中的协作

```
┌─────────────────────────────────────────────────────────────────────┐
│                  O2Tokenizer 协作关系图                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────────┐            ┌──────────────────────┐      │
│  │   数据写入阶段        │            │    查询阶段           │      │
│  │   (Ingest 模式)       │            │   (Search 模式)      │      │
│  └──────────┬───────────┘            └──────────┬───────────┘      │
│             │                                   │                  │
│             ▼                                   ▼                  │
│  ┌──────────────────────┐            ┌──────────────────────┐      │
│  │ O2Tokenizer::new     │            │ o2_collect_search    │      │
│  │   (CollectType::Ingest)│            │   _tokens()         │      │
│  └──────────┬───────────┘            └──────────┬───────────┘      │
│             │                                   │                  │
│             ▼                                   ▼                  │
│  ┌──────────────────────┐            ┌──────────────────────┐      │
│  │ 分词流程:             │            │ 分词流程:            │      │
│  │ 1. 跳过标点空白       │            │ 1. 跳过标点空白      │      │
│  │ 2. 识别 ASCII token   │            │ 2. 识别 ASCII token  │      │
│  │ 3. 驼峰拆分检测       │            │ 3. 驼峰拆分检测      │      │
│  │ 4. 输出完整词 + 拆分词│            │ 4. 只输出拆分词     │      │
│  │ 5. 非 ASCII 单独输出  │            │ 5. 非 ASCII 单独输出 │      │
│  └──────────┬───────────┘            └──────────┬───────────┘      │
│             │                                   │                  │
│             ▼                                   ▼                  │
│  ┌──────────────────────────┐        ┌──────────────────────┐      │
│  │ RemoveShortFilter        │        │ RemoveShortFilter    │      │
│  │ RemoveLongFilter         │        │ RemoveLongFilter     │      │
│  │ LowerCaser               │        │ LowerCaser           │      │
│  └──────────┬───────────────┘        └──────────┬───────────┘      │
│             │                                   │                  │
│             ▼                                   ▼                  │
│  ┌──────────────────────────┐        ┌──────────────────────┐      │
│  │ 写入 Tantivy 索引        │        │ 构建 TermQuery      │      │
│  │ INDEX_FIELD_NAME_FOR_ALL │        │ 在 INDEX_FIELD_NAME_ │      │
│  │ 字段                     │        │ FOR_ALL 中搜索       │      │
│  └──────────────────────────┘        └──────────────────────┘      │
│                                                                     │
│  示例: "HelloWorld"                                                 │
│    Ingest:  ["HelloWorld", "Hello", "World"] → 索引                │
│    Search:  ["Hello", "World"] → 查询                               │
│    匹配:   "Hello" AND "World" 在合并字段中找到                      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 7.4 组件协作关系图

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
| `src/infra/src/schema/mod.rs` | get_stream_setting_fts_fields 函数，获取完整字段列表 |
| `src/config/src/utils/tantivy/tokenizer/mod.rs` | O2Tokenizer 构建器，分词流水线 |
| `src/config/src/utils/tantivy/tokenizer/o2_tokenizer.rs` | O2Tokenizer 核心实现，驼峰拆分逻辑 |
| `src/service/tantivy/mod.rs` | Tantivy 索引创建，全文字段合并到 INDEX_FIELD_NAME_FOR_ALL |
| `src/job/files/parquet.rs` | 数据写入时调用 create_tantivy_index |
| `src/service/search/index.rs` | IndexCondition 和 Condition 定义，查询转换 |
| `src/service/search/sql/visitor/match_all.rs` | MatchVisitor 检查全文搜索字段 |
| `src/service/search/datafusion/optimizer/physical_optimizer/index.rs` | IndexRule 物理优化器 |
| `src/service/search/grpc/storage.rs` | tantivy_search 和 search_tantivy_index 实现 |
| `src/service/search/datafusion/optimizer/physical_optimizer/rewrite_match.rs` | match_all UDF 重写为 LIKE 表达式 |

## 9. 设计亮点

1. **全文字段合并设计**：所有全文搜索字段的数据合并到单个 `INDEX_FIELD_NAME_FOR_ALL` 字段
   - 简化 `match_all()` 查询实现
   - 减少索引字段数量，提高查询效率

2. **智能分词策略**：O2Tokenizer 支持驼峰命名拆分，Ingest/Search 双模式
   - Ingest 模式：完整词 + 拆分词，索引更完整
   - Search 模式：只使用拆分词，查询更高效

3. **多层过滤机制**：
   - 第一层：倒排索引快速过滤不相关的 Parquet 文件
   - 第二层：BitVec 行号位图只扫描匹配的行
   - 第三层：DataFusion 过滤器确保精确匹配（需要时）

4. **结果缓存**：索引搜索结果可缓存，重复查询性能提升显著

5. **智能回退**：
   - 索引不存在或过旧时，自动回退到完整扫描
   - 匹配行数过多时，回退避免位图过大
   - 条件无法完全用索引表达时，保留 DataFusion 过滤器

6. **聚合优化**：Count、Histogram、TopN、Distinct 等聚合查询可直接从索引返回结果，完全跳过 Parquet 扫描

7. **时间分区**：利用时间范围分区，减少不必要的索引搜索

## 10. 性能优化建议

1. **合理配置全文索引字段**：只对真正需要全文搜索的字段建立索引，减少索引大小
   - 避免将所有字段都加入全文搜索
   - 优先选择高基数字段（内容丰富的字段）

2. **调整分词参数**：根据实际数据特点调整 `inverted_index_min_token_length` 和 `inverted_index_max_token_length`
   - 中文日志可能需要更小的 min_token_length
   - 英文日志可以适当增大

3. **启用结果缓存**：对于重复查询较多的场景，启用 `inverted_index_result_cache_enabled`

4. **合理设置跳过阈值**：`inverted_index_skip_threshold` 控制何时回退到完整扫描，需要在索引效率和扫描效率之间平衡
   - 值太小：频繁回退，索引利用率低
   - 值太大：位图过大，内存占用高

5. **利用时间分区**：查询时尽量指定时间范围，减少需要搜索的索引文件

6. **理解分词差异**：
   - 查询 "HelloWorld" 会匹配包含 "hello" 和 "world" 的文档
   - 驼峰命名的拆分词会被分别搜索
   - 考虑数据特点调整查询关键词
