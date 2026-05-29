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

## 11. 容易误解的三个关键机制

### 11.1 full_text_search_keys 与 index_fields 的互斥校验

#### 11.1.1 为什么需要互斥

在 Tantivy 索引中，`full_text_search_keys`（全文索引字段）和 `index_fields`（二级索引字段）使用不同的分词策略和索引结构：

| 属性 | 全文索引字段 (FTS) | 二级索引字段 (Index) |
|------|-------------------|---------------------|
| Tantivy 字段 | 合并到 `INDEX_FIELD_NAME_FOR_ALL` | 创建独立字段 |
| 分词器 | `O2_TOKENIZER`（驼峰拆分 + 小写） | `raw`（不分词，原样存储） |
| 查询方式 | `match_all()` 在合并字段中搜索 | `Equal`/`In` 等精确匹配 |
| 索引粒度 | 词项级别 | 原始值级别 |

**如果同一字段同时出现在两种索引中**：
- 全文索引会将其分词后存入合并字段（如 "HelloWorld" → "hello", "world"）
- 二级索引会原样存入独立字段（如 "HelloWorld" → "HelloWorld"）
- 查询语义产生歧义：`Equal("HelloWorld")` 在二级索引中能匹配，但在全文索引中分词后无法精确匹配

#### 11.1.2 校验实现

在 `src/service/stream.rs:1365-1414` 中：

```rust
fn validate_index_field_conflicts(
    current_settings: &config::meta::stream::StreamSettings,
    new_settings: &config::meta::stream::UpdateStreamSettings,
) -> Result<(), String> {
    // Get the actual FTS and Index fields including defaults
    let current_fts_with_defaults =
        infra::schema::get_stream_setting_fts_fields(&Some(current_settings.clone()));
    let current_index_with_defaults =
        infra::schema::get_stream_setting_index_fields(&Some(current_settings.clone()));

    // Simulate the final state after applying the update
    let mut final_fts_fields: HashSet<String> = current_fts_with_defaults.iter().cloned().collect();
    let mut final_index_fields: HashSet<String> =
        current_index_with_defaults.iter().cloned().collect();

    // Apply removes
    for field in &new_settings.full_text_search_keys.remove {
        final_fts_fields.remove(field);
    }
    for field in &new_settings.index_fields.remove {
        final_index_fields.remove(field);
    }

    // Apply adds
    for field in &new_settings.full_text_search_keys.add {
        final_fts_fields.insert(field.clone());
    }
    for field in &new_settings.index_fields.add {
        final_index_fields.insert(field.clone());
    }

    // Find fields that would exist in both FTS and Secondary Index
    let conflicting_fields: Vec<String> = final_fts_fields
        .intersection(&final_index_fields)
        .cloned()
        .collect();

    if !conflicting_fields.is_empty() {
        let field_names: Vec<String> = conflicting_fields
            .iter()
            .map(|s| format!("'{s}'"))
            .collect();
        return Err(format!(
            "Field(s) {} cannot have both Full Text Search and Secondary Index. \
             Please choose only one index type per field.",
            field_names.join(", ")
        ));
    }
    Ok(())
}
```

#### 11.1.3 校验要点

1. **模拟最终状态**：校验不是简单检查新增字段，而是模拟 `当前配置 + 移除 + 新增` 的最终状态
2. **包含默认字段**：使用 `get_stream_setting_fts_fields()` 和 `get_stream_setting_index_fields()` 获取包含默认字段的完整列表
3. **Bloom Filter 独立**：Bloom Filter 字段不受此互斥约束，可以与 FTS 或二级索引共存
4. **校验时机**：在写入数据库之前执行，防止产生不一致状态

#### 11.1.4 索引创建时的字段分流

在 `src/service/tantivy/mod.rs` 中，互斥的字段走不同的索引路径：

```
用户配置字段列表
    ↓
fts_fields (只含 Utf8/LargeUtf8 类型)    index_fields (含所有类型)
    ↓                                         ↓
全部写入 INDEX_FIELD_NAME_FOR_ALL          各自创建独立字段
使用 O2_TOKENIZER 分词                      使用 raw 分词器
    ↓                                         ↓
match_all() 查询                             Equal/In 查询
```

关键代码（`src/service/tantivy/mod.rs:237-240`）：

```rust
for column_name in tantivy_fields.iter() {
    let field = match tantivy_schema.get_field(column_name) {
        Ok(f) => f,                        // index_fields: 在 schema 中有独立字段
        Err(_) => fts_field.unwrap(),      // fts_fields: 回退到合并字段
    };
}
```

### 11.2 索引条件部分跳过时的 DataFusion 回退过滤与结果缓存限制

#### 11.2.1 条件跳过的根因

在 `to_tantivy_query` 中（`src/service/search/index.rs:101-129`），当某个条件无法转换为 Tantivy 查询时，该条件会被**跳过**而非报错：

```rust
pub fn to_tantivy_query(
    &self,
    trace_id: &str,
    schema: Schema,
    default_field: Option<Field>,
) -> anyhow::Result<(Box<dyn Query>, bool)> {
    let mut has_skipped = false;
    let mut queries: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(self.conditions.len());
    for condition in &self.conditions {
        match condition.to_tantivy_query(&schema, default_field) {
            Ok(query) => {
                queries.push((Occur::Must, query));
            }
            Err(e) => {
                // 条件被跳过！
                log::info!(
                    "[trace_id {trace_id}] to_tantivy_query: skipping condition due to error: {e}"
                );
                has_skipped = true;
            }
        }
    }
    // ...
    Ok((Box::new(BooleanQuery::from(queries)), has_skipped))
}
```

**跳过发生的典型场景**：
1. **新增索引字段无历史数据**：用户新添加了一个二级索引字段，但旧的 Tantivy 索引文件中没有这个字段（`schema.get_field(field)` 返回 Err）
2. **MatchAll 但无全文搜索字段**：`default_field` 为 None，`MatchAll` 条件无法生成查询
3. **字段不在当前索引 schema 中**：某些 Parquet 文件的索引可能是在字段配置变更之前创建的

#### 11.2.2 回退过滤的完整链路

当 `has_skipped_conditions = true` 时，系统必须保留 DataFusion 过滤器以确保查询正确性：

```
search_tantivy_index() 返回 (file_name, result, has_skipped_conditions=true)
    ↓
tantivy_search() 设置 is_add_filter_back = true
    ↓
search() 检查 is_add_filter_back
    ├── true:  保留 index_condition 和 fst_fields，传给 create_tables_from_files()
    └── false: 清空 index_condition = None, fst_fields = vec![]
    ↓
create_tables_from_files() 使用 index_condition 构建回退过滤器
    ↓
DataFusion 扫描 Parquet 时应用回退过滤器，精确匹配被跳过的条件
```

**代码路径**（`src/service/search/grpc/storage.rs:138-142`）：

```rust
// set index_condition to None, means we do not need to add filter back
if !is_add_filter_back {
    index_condition = None;
    fst_fields = vec![];
}
```

#### 11.2.3 回退过滤器的构建

`IndexCondition.to_physical_expr()` 将索引条件转换回 DataFusion 物理表达式（`src/service/search/index.rs:543-672`）：

```rust
// MatchAll 回退为 LIKE 表达式
Condition::MatchAll(value) => {
    let value = value
        .trim_start_matches("re:")
        .trim_start_matches('*')
        .trim_end_matches('*')
        .to_string();
    // 为每个 fst_field 创建 LIKE '%value%' 表达式
    let term = Arc::new(Literal::new(ScalarValue::Utf8(Some(format!("%{value}%")))));
    let mut expr_list: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(fst_fields.len());
    for field in fst_fields.iter() {
        expr_list.push(create_like_expr_with_not_null(field, term, schema));
    }
    Ok(disjunction(expr_list))  // 所有 FTS 字段 OR 组合
}
```

#### 11.2.4 结果缓存限制

**关键约束**：当条件被跳过时，索引搜索结果是不完整的，因此**不允许缓存**：

```rust
// src/service/search/grpc/storage.rs:939-949
// Do not cache when conditions were skipped — the result is incomplete.
if cfg.common.inverted_index_result_cache_enabled
    && !cache_key.is_empty()
    && !has_skipped_conditions        // ← 关键：跳过条件时禁止缓存
    && (result.get_memory_size() < cfg.limit.inverted_index_result_cache_max_entry_size
        || percent < 1.0)
{
    let entry = get_cache_entry(result.clone(), percent, parquet_file.meta.records as usize);
    tantivy_result_cache::GLOBAL_CACHE.put(cache_key, entry);
}
```

**缓存读取时的特殊处理**：缓存命中时返回 `has_skipped_conditions = false`，因为缓存的结果是之前完整搜索的结果：

```rust
if let Some(result) = tantivy_result_cache::GLOBAL_CACHE.get(&cache_key) {
    return Ok((parquet_file.key.to_string(), result, false));  // false = 无跳过条件
}
```

#### 11.2.5 多重回退触发条件汇总

| 触发条件 | 代码位置 | 回退行为 |
|---------|---------|---------|
| `has_skipped_conditions = true` | `storage.rs:602-603` | `is_add_filter_back = true` |
| 文件无索引（`file_name.is_empty()`） | `storage.rs:605-620` | `is_add_filter_back = true`，保留文件 |
| 匹配行数过多（超过 `inverted_index_skip_threshold`） | `storage.rs:904-917` | 返回空文件名 + `has_skipped = true` |
| 搜索出错 | `storage.rs:655-663` | `is_add_filter_back = true`，保留文件 |
| 多个文件匹配行数过多 | `storage.rs:610-618` | 完全跳过索引搜索，保留所有文件 |

### 11.3 match_all 重写与 tokenizer 的协作边界

#### 11.3.1 两条独立的查询路径

`match_all()` 在查询执行中有两条完全独立的路径，**它们之间不共享 tokenizer 处理**：

```
┌─────────────────────────────────────────────────────────────────┐
│                    match_all() 的两条路径                         │
├───────────────────────┬─────────────────────────────────────────┤
│   路径 A: 倒排索引    │   路径 B: DataFusion 回退过滤           │
│   (Tantivy 搜索)      │   (RewriteMatch / to_physical_expr)     │
├───────────────────────┼─────────────────────────────────────────┤
│                       │                                         │
│  Condition::MatchAll  │  RewriteMatchPhysical 优化器             │
│       ↓               │       ↓                                 │
│  o2_collect_search    │  直接使用原始字符串                      │
│  _tokens("error")     │  trim * 后构造 LIKE '%error%'           │
│       ↓               │       ↓                                 │
│  ["error"]            │  "%error%"                              │
│       ↓               │       ↓                                 │
│  TermQuery 在         │  LIKE 在 Parquet 行数据上               │
│  INDEX_FIELD_NAME_    │  子串匹配                               │
│  FOR_ALL 中搜索       │       ↓                                 │
│       ↓               │  DataFusion 自身的字符串匹配            │
│  O2Tokenizer          │  (不经过 O2Tokenizer)                   │
│  Search 模式分词      │                                         │
│                       │                                         │
└───────────────────────┴─────────────────────────────────────────┘
```

#### 11.3.2 路径 A：倒排索引中的 tokenizer 协作

在 `Condition::MatchAll.to_tantivy_query()` 中（`src/service/search/index.rs:370-422`）：

1. **分词处理**：`o2_collect_search_tokens(value)` 使用 Search 模式分词
2. **查询构建**：每个 token 构建 `TermQuery`，在 `INDEX_FIELD_NAME_FOR_ALL` 中搜索
3. **AND 组合**：多个 token 使用 `BooleanQuery::intersection`
4. **通配符处理**：
   - `*keyword*` → `ContainsQuery`（子串搜索）
   - `keyword*` → `PhrasePrefixQuery`（前缀搜索）
   - `*keyword` → `RegexQuery`（后缀搜索，转为正则 `.*keyword`）

**关键**：这一路径中，查询关键词经过 O2Tokenizer Search 模式分词，与索引写入时的 Ingest 模式分词结果匹配。

#### 11.3.3 路径 B：DataFusion 回退中的 LIKE 重写

**RewriteMatchPhysical 优化器**（`src/service/search/datafusion/optimizer/physical_optimizer/rewrite_match.rs`）：

```rust
fn rewrite_match_all_physical(
    expr: &Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
    fields: &[(String, DataType)],
) -> Result<Arc<dyn PhysicalExpr>> {
    if name == MATCH_ALL_UDF_NAME {
        let item = extract_string_literal(item_expr)?;
        let item = item
            .trim_start_matches("re:")  // 去掉 re: 前缀
            .trim_start_matches('*')    // 去掉前导 *
            .trim_end_matches('*')      // 去掉尾部 *
            .to_string();

        // 为每个 FTS 字段创建 LIKE '%item%' 表达式
        for (field, data_type) in fields.iter() {
            let term = Arc::new(Literal::new(ScalarValue::Utf8(Some(format!("%{item}%")))));
            let new_expr = create_like_expr_with_not_null_physical(schema, field, term);
            expr_list.push(new_expr);
        }
        Ok(disjunction(expr_list))  // 所有 FTS 字段 OR 组合
    }
}
```

**IndexCondition.to_physical_expr()** 中的 MatchAll 回退（`src/service/search/index.rs:582-616`）：

```rust
Condition::MatchAll(value) => {
    let value = value
        .trim_start_matches("re:")
        .trim_start_matches('*')
        .trim_end_matches('*')
        .to_string();
    // 直接构造 LIKE '%value%' 表达式
    let term = Arc::new(Literal::new(ScalarValue::Utf8(Some(format!("%{value}%")))));
    for field in fst_fields.iter() {
        expr_list.push(create_like_expr_with_not_null(field, term, schema));
    }
    Ok(disjunction(expr_list))
}
```

#### 11.3.4 协作边界的关键差异

| 维度 | 路径 A (Tantivy) | 路径 B (DataFusion LIKE) |
|------|-----------------|------------------------|
| **分词** | O2Tokenizer Search 模式 | 无分词，直接使用原始字符串 |
| **匹配方式** | 词项精确匹配 (TermQuery) | 子串匹配 (LIKE '%...%') |
| **大小写** | LowerCaser 转小写后匹配 | `LikeExpr::new(false, true, ...)` 大小写不敏感 |
| **驼峰处理** | "HelloWorld" → ["hello", "world"] 各自匹配 | "HelloWorld" 作为整体子串匹配 |
| **通配符语义** | `*keyword*` → ContainsQuery | `*keyword*` → trim 后 LIKE '%keyword%' |
| **多词查询** | "hello world" → AND 组合 | "hello world" → LIKE '%hello world%' |
| **精度** | 词项级精确（可能有假阳性） | 子串级精确（保证真阳性） |

#### 11.3.5 can_remove_filter 的边界

`can_remove_filter()` 决定是否可以安全地移除 DataFusion 过滤器（即只用 Tantivy 索引结果）：

```rust
pub fn can_remove_filter(&self) -> bool {
    match self {
        Condition::Equal(..) => true,
        Condition::NotEqual(..) => true,
        Condition::StrMatch(..) => true,
        Condition::In(..) => true,
        Condition::Regex(..) => false,              // 正则查询不精确
        Condition::MatchAll(v) => is_alphanumeric(v), // 仅纯字母数字可移除
        Condition::FuzzyMatchAll(..) => false,       // 模糊查询不精确
        Condition::All() => true,
        Condition::Or(left, right) => left.can_remove_filter() && right.can_remove_filter(),
        Condition::And(left, right) => left.can_remove_filter() && right.can_remove_filter(),
        Condition::Not(condition) => condition.can_remove_filter(),
    }
}

fn is_alphanumeric(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_alphanumeric())
}
```

**`MatchAll` 为什么需要 `is_alphanumeric` 检查**：

- **可移除** (`"error"`)：纯字母数字，O2Tokenizer 分词后 TermQuery 精确匹配，无异义
- **不可移除** (`"error log"` 或 `"re:err.*"` 或 `"*error*"`)：
  - 包含空格：分词为多个 token AND 组合，但 LIKE 回退是整体子串匹配，语义不同
  - 包含通配符：Tantivy 使用 ContainsQuery/RegexQuery，可能有假阳性
  - 包含正则前缀 `re:`：正则匹配精度低于精确匹配

**移除过滤器的两种触发方式**：
1. **配置开关**：`feature_query_remove_filter_with_index = true` 全局强制移除
2. **条件判断**：`index_conditions.can_remove_filter()` 每个条件都允许移除时才移除

#### 11.3.6 完整的决策流程图

```
match_all('keyword')
    ↓
┌───────────────────────────────────────────────────────┐
│ IndexRule 提取 Condition::MatchAll("keyword")         │
└───────────────────────┬───────────────────────────────┘
                        ↓
┌───────────────────────────────────────────────────────┐
│ can_remove_filter()?                                   │
│  ├─ keyword 是纯字母数字 → true                       │
│  │    → 可移除 DataFusion 过滤器                       │
│  └─ keyword 含空格/特殊字符 → false                   │
│       → 必须保留 DataFusion 过滤器                     │
└───────────────────────┬───────────────────────────────┘
                        ↓
┌───────────────────────────────────────────────────────┐
│ Tantivy 索引搜索 (路径 A)                              │
│  ├─ o2_collect_search_tokens("keyword")               │
│  │    → 分词后在 INDEX_FIELD_NAME_FOR_ALL 中搜索       │
│  └─ 返回 BitVec 行号位图                               │
└───────────────────────┬───────────────────────────────┘
                        ↓
┌───────────────────────────────────────────────────────┐
│ is_add_filter_back?                                    │
│  ├─ true (条件跳过/匹配过多/无索引)                    │
│  │    → 保留 index_condition                           │
│  │    → DataFusion 扫描时应用 LIKE 回退过滤 (路径 B)  │
│  └─ false (索引完整且精确)                             │
│       → 清空 index_condition                           │
│       → 仅扫描 BitVec 标记的行                         │
└───────────────────────┬───────────────────────────────┘
                        ↓
┌───────────────────────────────────────────────────────┐
│ RewriteMatchPhysical 优化器 (仅路径 B 触发)            │
│  → match_all('keyword') 重写为                         │
│    field1 LIKE '%keyword%' OR field2 LIKE '%keyword%' │
│  → 注意：这里不经过 O2Tokenizer                        │
│  → LIKE 是子串匹配，比 TermQuery 更宽松               │
└───────────────────────────────────────────────────────┘
```

#### 11.3.7 常见误解澄清

**误解 1**："match_all 查询关键词总是经过 O2Tokenizer 分词"

**事实**：仅在 Tantivy 索引搜索路径（路径 A）中分词。DataFusion 回退过滤路径（路径 B）直接使用 LIKE 子串匹配，不经过任何分词。

**误解 2**："索引搜索结果总是精确的，不需要回退过滤"

**事实**：当条件被跳过（`has_skipped_conditions = true`）时，Tantivy 只搜索了部分条件，结果可能包含假阳性，必须由 DataFusion 回退过滤进一步筛选。

**误解 3**："MatchAll 的 can_remove_filter 只看查询类型"

**事实**：`MatchAll` 还检查查询值是否为纯字母数字。`match_all('error log')` 因为包含空格而不能移除过滤器——因为 Tantivy 路径会分词为 `"error" AND "log"`，而 LIKE 回退路径是 `LIKE '%error log%'` 整体匹配，两者的匹配域不同。

**误解 4**："缓存的结果总是可以信任的"

**事实**：当 `has_skipped_conditions = true` 时，结果不会被缓存，因为它是基于部分条件的不完整结果。缓存命中时返回 `has_skipped_conditions = false` 是因为缓存的是之前完整的搜索结果。但如果有新的索引字段被添加，旧的缓存结果可能遗漏了新字段的过滤，这是需要关注的边界情况。
