# OpenObserve 日志数据全链路概念级分析报告

## 1. 概述

本报告梳理 OpenObserve 中日志数据从接收端写入、列式落盘到查询索引可见的完整链路，重点分析批量缓冲、压缩策略与查询路径的衔接关系。所有参数默认值均来自代码实际配置。

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  接收写入   │───▶│  内存缓冲   │───▶│  列式落盘   │───▶│  查询索引   │
│  (Ingest)   │    │ (WAL+Mem)  │    │  (Parquet)  │    │  (Search)   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

---

## 2. 接收端写入路径

### 2.1 核心组件

| 组件 | 职责 | 关键特性 |
|------|------|----------|
| Writer | 写入入口管理器 | 按线程+组织分流，支持共享内存表 |
| WAL | 预写日志 | 磁盘持久化，防止数据丢失 |
| Memtable | 内存表 | Arrow 列式内存存储，支持实时查询 |
| ProcessedBatch | 预处理批 | JSON→Arrow 转换前置，解耦 CPU 与 IO |

### 2.2 写入流程

```
HTTP/gRPC 请求
     │
     ▼
  Writer.get_writer()  # 按 org_id + stream_type 获取写入器
     │
     ▼
  ProcessedBatch 预处理
     ├─ 序列化为 bytes 供 WAL 写入
     ├─ 转换为 Arrow RecordBatch 供 Memtable 写入
     └─ 计算总大小用于旋转检查
     │
     ▼
  写入队列（可选异步）
     │
     ▼
  双写操作
     ├─ WAL 写入：磁盘顺序写，保证持久性
     └─ Memtable 写入：内存列式存储，提供实时查询
```

### 2.3 批量缓冲机制

**入队缓冲** (`config.rs:1541, 3089-3091`)
- 队列大小：`ZO_WAL_WRITE_QUEUE_SIZE`（默认 **10000**）
- 两种模式：
  - 队列满时阻塞（默认）
  - 队列满时拒绝（`ZO_WAL_WRITE_QUEUE_FULL_REJECT=true`）

**内存缓冲**
- Memtable 阈值：`ZO_MAX_FILE_SIZE_IN_MEMORY`（默认 **256MB**）
- WAL 阈值：`ZO_MAX_FILE_SIZE_ON_DISK`（默认 **256MB**）
- 时间阈值：`ZO_MAX_FILE_RETENTION_TIME`（默认 **600 秒/10 分钟**）

### 2.4 旋转（Rotate）机制

当满足以下任一条件时触发旋转 (`writer.rs:600-760`)：
1. WAL 压缩后大小 + 新数据 > 磁盘阈值
2. WAL 原始大小 + 新数据 > 磁盘阈值
3. Memtable JSON 大小 + 新数据 > 内存阈值
4. Memtable Arrow 大小 + 新数据 > 内存阈值
5. 文件存活时间超过最大保留时间（默认 600 秒）

旋转操作：
1. 同步当前 WAL 到磁盘
2. 创建新的 WAL 和 Memtable
3. 旧 Memtable 转为 Immutable 等待持久化

### 2.5 容错机制

- **崩溃恢复**：启动时扫描未完成的 .lock 文件，重放 WAL
- **幂等处理**：重复旋转检查避免竞态
- **熔断保护**：内存/磁盘使用率超过阈值时拒绝写入

---

## 3. 列式落盘机制

### 3.1 持久化流程

```
Memtable（活跃）
     │
     ▼  旋转触发
Immutable（不可变内存表）
     │
     ▼  后台线程持久化
Partition 分区处理
     ├─ 按小时分区
     ├─ Schema 合并演进
     └─ 分块写入（PARQUET_FILE_CHUNK_SIZE = 100k 行）
     │
     ▼
Parquet 写入（.par 临时文件）
     │
     ▼  原子切换
1. 创建 .lock 文件
2. 删除 .wal 文件
3. .par → .parquet 重命名
4. 删除 .lock 文件
     │
     ▼
本地 WAL 目录存储
```

### 3.2 Parquet 格式设计

**列式存储结构** (`parquet.rs:46-96`)
- 按列组织数据，相同类型数据连续存储
- 支持谓词下推和列裁剪
- 行组（Row Group）作为读取单位

**元数据嵌入**
```rust
// parquet.rs:75-83
KeyValue::new("min_ts", ...)         // 最小时间戳
KeyValue::new("max_ts", ...)         // 最大时间戳
KeyValue::new("records", ...)        // 记录数
KeyValue::new("original_size", ...)  // 原始大小
```

### 3.3 压缩策略

**全局压缩配置** (`config.rs:971-972`)
- 配置项：`ZO_PARQUET_COMPRESSION`
- 默认值：`zstd`

**支持的压缩算法** (`config.rs:3424-3433`)

| 算法 | 配置值 | 适用场景 | 压缩比 | 速度 |
|------|--------|----------|--------|------|
| ZSTD | `zstd` | 默认通用 | 高 | 快 |
| SNAPPY | `snappy` | 低延迟场景 | 中 | 极快 |
| GZIP | `gzip` | 冷存储 | 很高 | 慢 |
| LZ4 | `lz4` | 高速缓存 | 中 | 极快 |
| BROTLI | `brotli` | 静态资源 | 很高 | 慢 |
| 无压缩 | `none` | 调试/特殊 | - | 最快 |

**特殊列优化** (`parquet.rs:62-73`)
```rust
// 时间戳列禁用字典编码，使用增量编码
.set_column_dictionary_enabled("_timestamp", false)
.set_column_encoding("_timestamp", Encoding::DELTA_BINARY_PACKED)

// 可配置禁用时间戳压缩（加速点查）
// ZO_TIMESTAMP_COMPRESSION_DISABLED
.set_column_compression("_timestamp", Compression::UNCOMPRESSED)
```

**Ingester 阶段压缩**
- 可通过 `ZO_FEATURE_INGESTER_NONE_COMPRESSION=true` 临时禁用
- 仅在 WAL 阶段使用，最终上传前合并时会重新压缩

### 3.4 布隆过滤器

**启用条件** (`parquet.rs:92-102`)
- 全局开关：`ZO_BLOOM_FILTER_ENABLED`（默认 **true**）
- 默认字段：`log_file`, `service_name`, `trace_id`, `span_id`
- 可配置字段：流设置中的 `bloom_filter_fields`

**参数配置**
- NDV（唯一值数）估算：`min(records, PARQUET_MAX_ROW_GROUP_SIZE) / NDV_RATIO`
- 假阳性率：`DEFAULT_BLOOM_FILTER_FPP`（默认 **0.01**）
- 按行组存储，减少内存占用

### 3.5 行组大小

**配置** (`config.rs:70`)
```rust
pub const PARQUET_MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;  // 1,048,576 行
```
- 注释明确说明：this can't be change, it will cause segment matching error

---

## 4. 文件合并与索引生成

### 4.1 小文件合并

**触发条件** (`job/files/parquet.rs:469-506`)
1. 累计原始大小达到 `ZO_MAX_FILE_SIZE_ON_DISK`
2. 文件数量超过阈值
3. 文件存活时间超过 `ZO_MAX_FILE_RETENTION_TIME`

**合并流程**
```
扫描本地 WAL 目录中的 parquet 文件
     │
     ▼
按前缀（org/stream/date/hour）分组
     │
     ▼
读取所有文件的 Schema，取并集
     │
     ▼
DataFusion 执行合并查询
     │
     ▼
生成合并后的大 Parquet 文件
     │
     ▼
上传到对象存储（S3/OSS 等）
     │
     ▼
写入 file_list 元数据到数据库
     │
     ▼
删除本地小文件（或加入待删队列）
```

**文件推送间隔** (`config.rs:2650-2651`)
- 配置项：`ZO_FILE_PUSH_INTERVAL`
- 默认值：**10 秒**（embedded 模式下的默认值）

### 4.2 倒排索引生成

**触发时机** (`job/files/parquet.rs:902-919`)
- 全局开关：`ZO_ENABLE_INVERTED_INDEX`（默认 **true**）
- 流类型支持：日志、指标、追踪（需 `support_index()`）
- 有配置的全文搜索字段或索引字段

**索引结构**
- 引擎：Tantivy 全文检索库
- 存储格式：Puffin（Parquet 附属文件格式）
- 与 Parquet 文件一一对应：`file.parquet` → `file.tantivy`

**索引字段类型**
1. **全文搜索字段（FTS）**：支持 `match_all()`, `str_match()`
2. **索引字段**：支持精确匹配 `=`, `IN`, 范围查询

---

## 5. 查询与索引可见性

### 5.1 数据可见性分层

查询时数据来自三个层级，按优先级合并：

```
┌─────────────────────────────────────────────────┐
│  查询层（Search）                               │
└─────────┬─────────────────┬─────────────────────┘
          │                 │
          ▼                 ▼
┌─────────────────┐ ┌────────────────────────────┐
│ Memtable（热）  │ │ Immutable（温）            │
│  最近写入数据   │ │  已旋转但未持久化的内存表  │
│  < 600s         │ │  < 数分钟                 │
└─────────────────┘ └────────────────────────────┘
          │                 │
          └─────────────────┬─────────────────────┘
                            ▼
                   ┌─────────────────┐
                   │ Parquet（冷）   │
                   │  对象存储数据   │
                   │  已建立索引     │
                   └─────────────────┘
```

### 5.2 查询执行路径

```
SQL 查询请求
     │
     ▼
SQL 解析与重写（Sql::new）
     ├─ 识别流名、时间范围
     ├─ Schema 解析
     └─ 检查索引适用性
     │
     ▼
分区规划（search_partition）
     ├─ 查询 file_list 获取候选文件
     ├─ 按时间分片划分任务
     └─ 估计数据量决定并行度
     │
     ▼
分布式执行（cluster::http::search）
     ├─ 分发到 Querier 节点
     └─ 本地执行或远程 gRPC
     │
     ▼
DataFusion 执行
     ├─ 选择文件列表
     │   ├─ 时间范围过滤（min_ts/max_ts）
     │   ├─ 分区键过滤
     │   └─ 布隆过滤器（如适用）
     ├─ 索引扫描（如使用倒排索引）
     │   ├─ Tantivy 倒排索引查询
     │   └─ 获取匹配行号
     ├─ Parquet 读取
     │   ├─ 列裁剪（只读取需要的列）
     │   ├─ 谓词下推到 Row Group
     │   └─ 解压所需列块
     └─ 结果聚合与排序
     │
     ▼
结果返回
```

### 5.3 索引使用路径

**索引条件提取** (`index.rs:66-89`)
```
SQL WHERE 子句
     │
     ▼
split_conjunction 拆分 AND 条件
     │
     ▼
is_expr_valid_for_index 检查
     ├─ 字段必须在索引列表中
     ├─ 支持操作符：=, !=, IN, NOT IN
     └─ 支持函数：match_all, str_match, fuzzy_match_all
     │
     ▼
IndexCondition 生成
     ├─ 可用于 Tantivy 的查询条件
     └─ 剩余的 DataFusion 过滤条件
```

**索引执行流程**
1. 先通过 Tantivy 查询倒排索引，获取匹配的文档 ID
2. 用文档 ID 作为 Row Group 内的过滤条件
3. 剩余条件由 DataFusion 在读取后过滤

---

## 6. 压缩策略与查询路径的衔接

### 6.1 压缩对查询的影响

**有利影响**
1. **减少 IO 量**：高压缩比减少磁盘/网络读取量
2. **列式局部性**：同列数据连续存储，缓存命中率高
3. **谓词下推**：压缩不影响 Parquet 统计信息（min/max）

**不利影响**
1. **解压开销**：查询时需要解压列数据
2. **随机读成本**：高压缩算法（Gzip/Brotli）解压慢

### 6.2 时间戳列特殊处理的设计考量

```
查询模式               压缩策略选择
─────────────────────────────────────────────
范围扫描（>、<）      DELTA_BINARY_PACKED + 无压缩
                       → 顺序读取，CPU 开销小
─────────────────────────────────────────────
精确点查（=）         无压缩 + 增量编码
                       → 直接定位，无需解压整列
─────────────────────────────────────────────
聚合查询（min/max）    统计信息元数据
                       → 无需读取实际数据
```

### 6.3 压缩算法与查询性能的权衡

| 算法 | 压缩比 | 写入速度 | 读取速度 | 适用查询场景 |
|------|--------|----------|----------|-------------|
| **ZSTD** | 高 | 快 | 快 | 通用场景，平衡性能 |
| **LZ4** | 中 | 极快 | 极快 | 热数据、高并发查询 |
| **SNAPPY** | 中 | 极快 | 极快 | 低延迟查询 |
| **GZIP** | 很高 | 慢 | 慢 | 冷存储、低频访问 |
| **无压缩** | - | 最快 | 最快 | 时间戳列、索引列 |

### 6.4 行组大小与查询性能

**配置**：`PARQUET_MAX_ROW_GROUP_SIZE` = **1,048,576 行**（1024 * 1024）

**设计权衡**
- 大行组：压缩比更高，但扫描单组成本高
- 小行组：谓词下推更精确，但元数据开销大
- 布隆过滤器按行组存储，行组大小影响过滤器精度
- 代码注释明确说明该值不可修改，否则会导致 segment matching error

---

## 7. 关键参数汇总（全部经过代码核对）

| 参数 | 默认值 | 说明 | 代码位置 |
|------|--------|------|----------|
| `ZO_WAL_WRITE_QUEUE_SIZE` | `10000` | 写入队列大小 | config.rs:3090 |
| `ZO_MAX_FILE_SIZE_ON_DISK` | `256` (MB) | WAL/Parquet 文件大小阈值 | config.rs:1500 |
| `ZO_MAX_FILE_SIZE_IN_MEMORY` | `256` (MB) | Memtable 大小阈值 | config.rs:1503 |
| `ZO_MAX_FILE_RETENTION_TIME` | `600` (秒) | 文件最大保留时间 | config.rs:1497 |
| `ZO_PARQUET_COMPRESSION` | `zstd` | Parquet 压缩算法 | config.rs:971 |
| `PARQUET_MAX_ROW_GROUP_SIZE` | `1048576` (行) | Parquet 行组大小 | config.rs:70 |
| `PARQUET_FILE_CHUNK_SIZE` | `102400` (行) | 分块写入大小 | config.rs:71 |
| `ZO_BLOOM_FILTER_ENABLED` | `true` | 布隆过滤器开关 | config.rs:1089 |
| `DEFAULT_BLOOM_FILTER_FPP` | `0.01` | 布隆过滤器假阳性率 | config.rs:72 |
| `ZO_ENABLE_INVERTED_INDEX` | `true` | 倒排索引开关 | config.rs:1258 |
| `ZO_TIMESTAMP_COMPRESSION_DISABLED` | `false` | 禁用时间戳压缩 | config.rs:974 |
| `ZO_FILE_PUSH_INTERVAL` | `10` (秒) | 本地文件上传间隔 | config.rs:2651 |
| `ZO_WAL_WRITE_QUEUE_FULL_REJECT` | `false` | 队列满时拒绝 | config.rs:1116 |

---

## 8. 总结

### 8.1 核心设计思想

1. **LSM 树思想**：Memtable → Immutable → SSTable（Parquet）的分层架构
2. **读写分离**：写入路径只做顺序 IO，查询路径优化随机读
3. **列式存储**：Parquet 列式布局适配分析型查询
4. **三级索引**：分区裁剪 → 布隆过滤器 → 倒排索引
5. **成本权衡**：压缩算法、行组大小、索引粒度均可配置

### 8.2 性能关键点

- 写入瓶颈在 WAL 磁盘 IO 和 Memtable 锁竞争
- 查询瓶颈在 Parquet 解压和网络传输
- 索引能显著减少扫描数据量，但增加写入开销
- 时间戳列的特殊优化对范围查询性能至关重要
- 行组大小为 1,048,576 行，代码明确注释不可修改

### 8.3 可观测性

系统提供以下关键指标用于监控链路健康：
- `INGEST_WAL_LOCK_TIME`：WAL 锁等待时间
- `INGEST_MEMTABLE_LOCK_TIME`：Memtable 锁等待时间
- `INGEST_WAL_USED_BYTES`：WAL 磁盘使用量
- `INGEST_MEMTABLE_BYTES`：Memtable 内存使用量
- 文件级统计通过 `file_list` 元数据追踪
