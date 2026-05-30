# OpenObserve 集群队列分发与消费流程分析

## 1. 概述

OpenObserve 使用 NATS JetStream 作为分布式消息队列的底层实现，通过封装的 `Queue` trait 提供统一的消息队列操作接口。整个系统围绕 **消息入列**、**节点选举** 和 **确认回执** 三个核心环节构建可靠的分布式协作机制。

## 2. 核心架构组件

### 2.1 队列抽象层 (`src/infra/src/queue/mod.rs`)

系统定义了统一的队列接口 `Queue` trait：

```rust
#[async_trait]
pub trait Queue: Sync + Send + 'static {
    async fn create(&self, topic: &str) -> Result<()>;
    async fn create_with_config(&self, topic: &str, config: QueueConfig) -> Result<()>;
    async fn publish(&self, topic: &str, value: Bytes) -> Result<()>;
    async fn consume(&self, topic: &str, deliver_policy: Option<DeliverPolicy>) -> Result<Arc<mpsc::Receiver<Message>>>;
    async fn purge(&self, topic: &str, sequence: usize) -> Result<()>;
}
```

**关键配置选项：**
- `RetentionPolicy`: `Interest`（兴趣保留）或 `Limits`（限制保留）
- `StorageType`: `File`（文件存储）或 `Memory`（内存存储）
- `DeliverPolicy`: `All`（全部重放）、`Last`（最新开始）、`New`（仅新消息）

### 2.2 NATS 队列实现 (`src/infra/src/queue/nats.rs`)

`NatsQueue` 是实际的队列实现，核心特性：

1. **主题命名规范**：通过 `format_key()` 函数格式化主题名，确保符合 NATS 规范
2. **消费者配置**：支持持久化消费者（durable consumer）
3. **消息传递**：使用 `tokio::sync::mpsc` 通道异步传递消息

## 3. 消息入列流程

### 3.1 发布消息流程

```
调用方 → Queue::publish() → NATS JetStream → 等待发布确认
```

**核心代码 (`src/infra/src/queue/nats.rs:117-126`)：**
```rust
async fn publish(&self, topic: &str, value: Bytes) -> Result<()> {
    let client = get_nats_client().await.clone();
    let jetstream = jetstream::new(client);
    let topic_name = format!("{}{}", self.prefix, format_key(topic));
    let ack = jetstream.publish(topic_name, value).await?;
    ack.await?;  // 等待 NATS 服务端确认
    Ok(())
}
```

**关键点：**
- 发布操作是**同步等待确认**的，确保消息已被 NATS 接收
- 主题名会经过格式化处理，确保安全性

### 3.2 典型发布场景

**1. Coordinator 事件发布 (`src/infra/src/coordinator/events.rs`)**
- 用于元数据变更的集群广播
- 支持 `Put` 和 `Delete` 两种操作类型

**2. 试用配额高可用同步 (`src/service/trial_quota.rs`)**
- 发布增量扣费消息
- 携带 `source_node` 信息避免自消费

**3. 超级集群队列 (`src/super_cluster_queue/mod.rs`)**
- 跨集群元数据同步
- 支持多种消息类型（元数据、告警、仪表盘等）

## 4. 节点选举机制

### 4.1 分布式锁 (`src/infra/src/dist_lock.rs`)

系统使用 NATS 实现分布式锁，用于节点注册等关键操作：

```rust
pub async fn lock(key: &str, wait_ttl: u64) -> Result<Option<Locker>> {
    let mut lock = nats::Locker::new(key);
    lock.lock(wait_ttl).await?;
    Ok(Some(Locker(LockerStore::Nats(lock))))
}
```

### 4.2 节点注册流程 (`src/common/infra/cluster/nats.rs:86-201`)

```
1. 获取分布式锁 /nodes/register
2. 初始化 Coordinator 事件流
3. 启动节点列表监听 (watch_node_list)
4. 获取现有节点列表
5. 生成唯一节点 ID (generate_node_id)
6. 将节点信息写入 /nodes/{uuid}
7. 释放分布式锁
8. 启动心跳保持协程
```

**节点 ID 生成算法 (`src/infra/src/cluster/mod.rs:591-606`)：**
```rust
pub fn generate_node_id(mut ids: Vec<i32>) -> i32 {
    ids.sort();
    ids.dedup();
    let mut new_node_id = 1;
    for id in ids {
        if id == new_node_id {
            new_node_id += 1;
        } else {
            break;
        }
    }
    new_node_id
}
```

### 4.3 一致性哈希节点分配

系统使用一致性哈希算法进行任务分配，支持多种角色：

- `QUERIER_INTERACTIVE_CONSISTENT_HASH`: 交互式查询节点
- `QUERIER_BACKGROUND_CONSISTENT_HASH`: 后台查询节点  
- `COMPACTOR_CONSISTENT_HASH`: 压缩节点
- `FLATTEN_COMPACTOR_CONSISTENT_HASH`: 扁平化压缩节点

**哈希环操作 (`src/infra/src/cluster/mod.rs:61-97`)：**
```rust
pub async fn add_node_to_consistent_hash(node: &Node, role: &Role, group: Option<RoleGroup>) {
    // 每个节点创建多个虚拟节点 (vnodes)
    for i in 0..get_config().limit.consistent_hash_vnodes {
        let key = format!("{}:{}:{}", CONSISTENT_HASH_PRIME, node.name, i);
        let hash = h.sum64(&key);
        nodes.insert(hash, node.name.clone());
    }
}
```

**节点选择算法：**
1. 计算键的哈希值
2. 在哈希环上顺时针查找第一个节点
3. 如果到达环尾则从头开始

## 5. 消息消费与确认回执

### 5.1 消费流程

```
NATS JetStream → 消费者协程 → mpsc::channel → 业务处理 → 消息确认(ACK)
```

**核心消费逻辑 (`src/infra/src/queue/nats.rs:128-202`)：**
```rust
async fn consume(&self, topic: &str, deliver_policy: Option<DeliverPolicy>) -> Result<Arc<mpsc::Receiver<Message>>> {
    let (tx, rx) = mpsc::channel(1024);
    
    tokio::task::spawn(async move {
        // 创建或获取消费者
        let consumer = stream.get_or_create_consumer(&consumer_name, config).await?;
        
        // 持续消费消息
        loop {
            let message = messages.try_next().await?;
            let message = super::Message::Nats(message);
            tx.send(message).await?;
        }
    });
    
    Ok(Arc::new(rx))
}
```

### 5.2 确认回执 (ACK) 机制

**消息确认接口 (`src/infra/src/queue/mod.rs:147-155`)：**
```rust
impl Message {
    pub async fn ack(&self) -> Result<()> {
        match self {
            Message::Nats(msg) => msg.ack().await.map_err(|e| Error::Message(format!("ack error:{e}")))?,
        }
        Ok(())
    }
}
```

### 5.3 典型消费场景

**场景 1: Coordinator 事件消费 (`src/infra/src/coordinator/events.rs:142-215`)**

```rust
async fn subscribe(tx: mpsc::Sender<CoordinatorEvent>) -> Result<()> {
    loop {
        match receiver.recv().await {
            Some(message) => {
                // 1. 反序列化消息
                let event: CoordinatorEvent = serde_json::from_slice(&message.payload)?;
                
                // 2. 发送到业务处理通道
                tx.send(event).await?;
                
                // 3. 确认消息（仅在成功处理后）
                message.ack().await?;
            }
            None => { /* 重连逻辑 */ }
        }
    }
}
```

**ACK 策略：**
- ✅ 消息成功处理后才发送 ACK
- ❌ 通道发送失败时不发送 ACK（消息将被重新投递）

**场景 2: 试用配额 HA 队列消费 (`src/service/trial_quota.rs:405-490`)**

```rust
while let Some(msg) = rx.recv().await {
    let ha_msg: TrialQuotaHaMsg = json::from_slice(payload)?;
    
    // 跳过自己发布的消息
    if ha_msg.source_node.eq(&LOCAL_NODE) {
        msg.ack().await?;  // 确认后跳过
        continue;
    }
    
    // 跳过过期消息（已在数据库快照中）
    if ha_msg.timestamp <= watermark {
        msg.ack().await?;  // 确认后跳过
        continue;
    }
    
    // 应用增量更新
    add_to_org_counter(&ha_msg.org_id, ha_msg.cost);
    
    msg.ack().await?;  // 处理完成后确认
}
```

**ACK 策略要点：**
1. **消息过滤后确认**：即使消息被过滤（自消费、过期），也需要 ACK 避免重复投递
2. **处理后确认**：业务逻辑执行成功后才发送 ACK
3. **异常处理**：反序列化失败也需要 ACK，避免坏消息阻塞队列

## 6. 三者关联关系

### 6.1 完整数据流图

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  消息发布   │────▶│  NATS JetStream │────▶│  消息消费   │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  发布确认   │     │  持久化存储  │     │  处理确认   │
│  (Publish  │     │  (Stream)    │     │  (ACK)      │
│   ACK)      │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
                            │
                            ▼
                      ┌─────────────┐
                      │  节点选举   │
                      │  (一致性哈希)│
                      └─────────────┘
                            │
                            ▼
                      ┌─────────────┐
                      │  任务分配   │
                      └─────────────┘
```

### 6.2 节点注册与队列消费的时序关系

```
节点启动
    │
    ▼
1. 获取分布式锁
    │
    ▼
2. 注册节点信息到 NATS KV
    │
    ▼
3. 启动节点列表 watcher
    │
    ▼
4. 构建一致性哈希环
    │
    ▼
5. 释放分布式锁
    │
    ▼
6. 启动心跳协程 (set_online)
    │
    ▼
7. 初始化队列消费者
    │  ├─ 创建队列主题
    │  ├─ 创建消费者
    │  └─ 开始消费循环
    │
    ▼
8. 正常运行
    ├─ 发布消息到队列
    ├─ 消费并处理消息
    └─ 发送 ACK 确认
```

### 6.3 ACK 对消息投递的影响

| 状态 | 行为 | 结果 |
|------|------|------|
| 发送 ACK | NATS 删除消息 | ✓ 消息处理完成 |
| 不发送 ACK | NATS 等待超时 | ⟳ 消息重新投递 |
| 消费者断开 | NATS 检测到离线 | ⟳ 消息重新投递给其他消费者 |

## 7. 关键设计决策

### 7.1 交付策略选择

- **`DeliverPolicy::New`**: 正常运行时使用，只消费新消息
- **`DeliverPolicy::All`**: 重连或初始化时使用，确保不丢失任何消息
- **`DeliverPolicy::Last`**: 从最新消息开始，适用于快速启动场景

### 7.2 持久化消费者 vs 临时消费者

```rust
let config = jetstream::consumer::pull::Config {
    name: Some(consumer_name.to_string()),
    durable_name: if is_durable {
        Some(consumer_name.to_string())  // 持久化：记录消费进度
    } else {
        None  // 临时：断开后丢失进度
    },
    ..
};
```

**超级集群队列使用持久化消费者** (`src/infra/src/queue/nats.rs:47-49`)：
```rust
pub fn super_cluster() -> Self {
    Self::new("super_cluster_queue_").with_consumer_name(get_cluster_name(), true)
}
```

### 7.3 消息幂等性保证

由于消息可能重复投递（未 ACK 断开），消费端需要保证幂等性：
1. **试用配额**：通过 `timestamp` 和 `watermark` 过滤重复消息
2. **元数据同步**：通过版本号或时间戳判断是否需要应用更新
3. **节点注册**：通过 UUID 唯一标识，重复注册会被覆盖

## 8. 异常处理机制

### 8.1 消费端重连

`src/infra/src/coordinator/events.rs` 中的重连逻辑：
```rust
loop {
    // 消费异常断开
    if config::cluster::is_offline() { break; }
    
    // 重连时使用 DeliverPolicy::All 回放所有消息
    let deliver_policy = if reconnect {
        queue::DeliverPolicy::All
    } else {
        queue::DeliverPolicy::New
    };
    
    // 重新订阅
    let receiver = q.consume(COORDINATOR_STREAM, Some(deliver_policy)).await?;
    
    // 继续消费...
}
```

### 8.2 节点健康检查

`src/infra/src/cluster/mod.rs:406-481` 中的健康检查：
- 定期 HTTP 健康检查 (`/healthz`)
- 连续失败达到阈值后从哈希环移除
- 从本地缓存中删除失效节点

## 9. 节点职责边界分析

### 9.1 节点角色 (Role) 职责范围

系统定义了 8 种节点角色，每种角色有明确的职责边界 (`src/config/src/meta/cluster.rs:201-210`)：

| 角色 | 职责范围 | 队列消费职责 |
|------|---------|------------|
| **All** | 全能节点，承担所有角色职责 | 消费所有类型队列消息 |
| **Ingester** | 数据写入、WAL 管理、数据压缩触发 | 消费文件列表广播、元数据变更事件 |
| **Querier** | 查询执行、缓存管理、结果聚合 | 消费查询任务、文件列表变更 |
| **Compactor** | 文件压缩、数据保留策略执行 | 消费压缩任务队列 |
| **Router** | 请求路由、负载均衡 | 不消费业务队列 |
| **AlertManager** | 告警评估、通知发送、告警调度 | 消费告警事件队列 |
| **FlattenCompactor** | 扁平化数据压缩 | 消费扁平化压缩任务 |
| **ActionServer** | 脚本执行、动作处理 | 消费动作执行队列 |

**角色判定逻辑** (`src/config/src/meta/cluster.rs:103-138`)：
```rust
pub fn is_querier(&self) -> bool {
    self.role.contains(&Role::Querier) || self.role.contains(&Role::All)
}
```

**关键设计**：
- **单角色节点**：只执行特定职责，资源隔离性好
- **多角色节点**：节省资源但存在资源竞争
- **Router 节点**：仅做路由，不消费业务队列

### 9.2 角色组 (RoleGroup) 职责划分

角色组用于对 Querier 节点的细粒度任务优先级划分：

| 角色组 | 职责范围 | 典型任务类型 |
|--------|---------|-----------|
| **None** | 承担所有查询任务 | 通用查询节点 |
| **Interactive** | 高优先级交互式查询 | UI查询、仪表盘、值查询、RUM、下载 |
| **Background** | 低优先级后台任务 | 报表、告警、派生流、搜索作业 |

**角色组映射关系** (`src/config/src/meta/cluster.rs:269-279`)：
```rust
impl From<SearchEventType> for RoleGroup {
    fn from(value: SearchEventType) -> Self {
        match value {
            SearchEventType::Reports | SearchEventType::Alerts 
            | SearchEventType::DerivedStream 
            | SearchEventType::SearchJob => RoleGroup::Background,
            _ => RoleGroup::Interactive,
        }
    }
}
```

### 9.3 节点注册的职责边界

**节点注册阶段各组件的职责划分**：

| 组件 | 职责范围 | 关键操作 |
|------|---------|---------|
| **分布式锁** | 保证节点注册互斥 | `/nodes/register` 锁保护 |
| **NATS KV** | 存储节点元数据 | `/nodes/{uuid}` 存储节点信息 |
| **节点列表 Watcher** | 监听节点变更 | `watch_node_list` 实时更新 |
| **一致性哈希** | 构建任务分配环 | `add_node_to_consistent_hash` |
| **心跳协程** | 维持节点在线状态 | 定期更新 TTL |

**注册时序中的职责隔离** (`src/common/infra/cluster/nats.rs:86-201`)：
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  分布式锁模块   │───▶│   NATS KV 存储   │───▶│  一致性哈希模块  │
│  (互斥保护)     │    │  (元数据持久化 │    │  (任务分配)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │  心跳协程     │
                        │  (保活机制)   │
                        └─────────────────┘
```

### 9.4 主从决策触发机制

系统采用**无中心的主从决策模式**，不同场景采用不同的选举策略：

**策略 1: ID 最小节点选举** (`src/job/mod.rs:83-95`)
- **适用场景**：一次性迁移任务、数据补丁执行
- **选举逻辑**：
  ```rust
  let is_leader = infra::cluster::get_cached_online_nodes()
      .await
      .and_then(|mut nodes| {
          nodes.sort_by_key(|n| n.id);
          nodes.into_iter().next()
      })
      .map(|first| first.id == LOCAL_NODE.id)
      .unwrap_or(true);
  ```
- **特点**：稳定可预测，节点重启后 leader 不变

**策略 2: UUID 排序选举** (`src/job/mod.rs:826-832`)
- **适用场景**：定期清理任务
- **选举逻辑**：
  ```rust
  let is_leader = match infra::cluster::get_cached_online_ingester_nodes().await {
      Some(mut nodes) if !nodes.is_empty() => {
          nodes.sort_by(|a, b| a.uuid.cmp(&b.uuid));
          nodes[0].uuid == LOCAL_NODE.uuid
      }
      _ => true,
  };
  ```
- **特点**：随机性好，负载均衡

**策略 3: 特定角色节点执行** (`src/job/mod.rs:64-66`)
- **适用场景**：告警管理、计量计费
- **判定逻辑**：
  ```rust
  if LOCAL_NODE.is_alert_manager() {
      // 只有 AlertManager 角色才执行
  }
  ```

### 9.5 一致性哈希分配的职责范围

**文件广播中的一致性哈希路由** (`src/service/db/file_list/broadcast.rs:42-120`)：

```
文件列表变更事件
        │
        ▼
┌─────────────────────────────────────────┐
│  按文件 ID 计算哈希值            │
└─────────────────────────────────────────┘
        │
        ├───────────────────────────┐
        ▼                           ▼
┌─────────────────────┐     ┌─────────────────────┐
│ Interactive 角色组    │     │ Background 角色组    │
│ 哈希环查询       │     │ 哈希环查询       │
└─────────────────────┘     └─────────────────────┘
        │                           │
        ▼                           ▼
┌─────────────────────┐     ┌─────────────────────┐
│ 目标节点 A        │     │ 目标节点 B        │
│ (可能相同)       │     │ (可能不同)       │
└─────────────────────┘     └─────────────────────┘
```

**企业版增强路由策略** (`src/service/db/file_list/broadcast.rs:59-93`)：
- **策略 A（默认）**：全局哈希环，所有节点参与
- **策略 B（企业版）**：按插槽选择，仅选择部分节点

```rust
// OSS / strategy=all: use the global ring unchanged.
if let Some(node_name) = cluster::get_node_from_consistent_hash(
    &item.id.to_string(),
    &Role::Querier,
    Some(RoleGroup::Interactive),
)
    && node_name.eq(&node.name)
{
    node_items.push(item.clone());
}
```

## 10. 确认回执失败完整流程

### 10.1 消费者配置与默认值分析（基于 NATS 官方文档 + async-nats 0.47.0 源码验证）

**关键发现：所有重投相关参数使用 NATS 服务器默认值** (`src/infra/src/queue/nats.rs:144-153`)

```rust
let config = jetstream::consumer::pull::Config {
    name: Some(consumer_name.to_string()),
    durable_name: if is_durable {
        Some(consumer_name.to_string())
    } else {
        None
    },
    deliver_policy: get_deliver_policy(deliver_policy),
    ..Default::default()  // ⚠️ 所有其他参数使用 async-nats 0.47.0 的 Default::default()
};
```

**NATS JetStream 官方默认值验证**（来源：[NATS 官方文档](https://docs.nats.io/nats-concepts/jetstream/consumers#configuration) + async-nats 0.47.0 源码）：

| 参数 | 配置状态 | NATS 默认值 | 行为 |
|------|---------|-----------|------|
| `ack_wait` | 未配置 | **30秒** | 消息 30 秒未 ACK 则触发重投 |
| `max_deliver` | 未配置 | **-1（无限）** | **消息无限重投，永不丢弃** |
| `ack_policy` | 未配置 | **AckPolicy::Explicit** | 必须显式 ACK 每条消息 |
| `max_ack_pending` | 未配置 | **1000** | 最多 1000 条待 ACK 消息（流控） |
| 死信队列 | 未配置 | **无** | **NATS 原生无死信队列机制** |
| `inactive_threshold` | 未配置 | **0（永不清理）** | 消费者永久保留（持久化时） |

> ⚠️ **重要修正**：OpenObserve 代码中**没有配置重投次数限制**（`max_deliver` 保持默认 -1），也**没有实现死信队列**。失败的消息会被**无限次重投**，除非业务代码主动 ACK 或消息被 Stream 清理。
>
> **NATS 服务器行为说明**：即使 `max_deliver` 设置为有限次数，消息达到最大投递次数后**仍然留在 Stream 中**，只是不再投递给该消费者。这与传统"死信队列"概念不同。

### 10.1.1 结束重投的真实条件

**基于代码分析：消息停止重投只有三种可能**（OpenObserve 未配置 `max_deliver` 限制）：

| 结束条件 | 触发方式 | 代码证据位置 | 说明 |
|---------|---------|------------|------|
| **业务代码主动 ACK** | 消费成功后调用 `message.ack()` | 多处，如 `src/infra/src/coordinator/events.rs:204` | 正常路径，NATS 服务器删除消息 |
| **业务代码主动 ACK（丢弃）** | 即使处理失败也调用 `message.ack()` | `src/service/trial_quota.rs:439-442` | 保护性丢弃，防止坏消息阻塞队列 |
| **Stream 保留策略清理** | 消息超过 `max_age` 或 `max_bytes` | `src/infra/src/queue/nats.rs:92-111` | 被 Stream 整体清理，非消费端控制 |

**详细代码分析**：

1. **正常结束：业务 ACK**
   - 消费成功 → 调用 `msg.ack().await` → NATS 服务器删除消息
   - 典型实现：`src/infra/src/coordinator/events.rs:204`
   ```rust
   // 处理成功后才 ACK
   tx.send(event).await?;
   message.ack().await?;
   ```

2. **保护性结束：主动丢弃坏消息**
   - 场景：反序列化失败、格式错误等无法恢复的错误
   - 实现位置：`src/service/trial_quota.rs:439-442`
   ```rust
   let ha_msg: TrialQuotaHaMsg = match json::from_slice(payload) {
       Ok(m) => m,
       Err(e) => {
           log::error!("[TRIAL_QUOTA] Failed to deserialize HA message: {e}");
           if let Err(e) = msg.ack().await {  // ✅ 主动 ACK 丢弃
               log::error!("[TRIAL_QUOTA] Failed to ack HA message: {e}");
           }
           continue;
       }
   };
   ```
   - 设计权衡：可能丢失数据，但保证队列可用性

3. **被动结束：Stream 保留策略清理**
   - 配置位置：`src/infra/src/queue/nats.rs:92-111`
   - 默认 `max_age = 60 天`（`ZO_NATS_QUEUE_MAX_AGE`，`src/config/src/config.rs:2102`）
   - 消息在 Stream 中存活超过 60 天后被自动清理
   - 这是**最后的安全网**，业务逻辑不应该依赖此机制

> ⚠️ **关键结论**：
> 1. **没有"超过重投次数自动丢弃"的机制**！OpenObserve 使用 NATS 默认值 `max_deliver = -1`，意味着只要消息还在 Stream 中，就会一直尝试重投。
> 2. **没有死信队列（DLQ）机制**！NATS 原生不支持 DLQ，OpenObserve 也未实现。
> 3. **max_deliver 的真实含义**：即使配置了有限次数，达到次数后消息**仍然留在 Stream 中**，只是不再投递给该消费者，而非被删除。

### 10.1.2 关于 `max_deliver` 参数的深入说明

**重要概念澄清**：`max_deliver` 参数经常被误解为"最大投递次数，超过则丢弃"，但这是不准确的。

**NATS 官方定义**（来源：[NATS 官方文档](https://docs.nats.io/nats-concepts/jetstream/consumers#configuration)）：

> **MaxDeliver**: The maximum number of times a specific message delivery will be attempted. Applies to any message that is re-sent due to acknowledgment policy (i.e., due to a negative acknowledgment or no acknowledgment sent by the client). **The default is -1 (redeliver until acknowledged)**. **Messages that have reached the maximum delivery count will stay in the stream**.

**关键理解**：

| 误解 | 事实 |
|-----|------|
| "达到 max_deliver 后消息被删除" | ❌ 错误 |
| "达到 max_deliver 后进入死信队列" | ❌ 错误 |
| "达到 max_deliver 后不再投递给该消费者" | ✅ 正确 |
| "达到 max_deliver 后消息仍保留在 Stream 中" | ✅ 正确 |

**OpenObserve 中的实际情况**：

```rust
// src/infra/src/queue/nats.rs:144-153
let config = jetstream::consumer::pull::Config {
    name: Some(consumer_name.to_string()),
    durable_name: if is_durable {
        Some(consumer_name.to_string())
    } else {
        None
    },
    deliver_policy: get_deliver_policy(deliver_policy),
    ..Default::default()  // max_deliver = -1（NATS 默认）
};
```

**结论**：由于 `max_deliver = -1`（默认值），OpenObserve 中的消息**永远不会因为投递次数过多而停止重投**。

### 10.2 两种不同的 ACK 策略对比

系统中存在两种截然不同的 ACK 处理策略，由业务代码自行决定：

**策略 A：失败不 ACK（默认行为）**

**实现位置**：`src/infra/src/coordinator/events.rs:178-186`

```rust
let event: CoordinatorEvent = match serde_json::from_slice(&message.payload) {
    Ok(event) => event,
    Err(e) => {
        log::error!("[COORDINATOR::EVENTS] failed to deserialize coordinator event: {e}");
        continue;  // ❌ 不发送 ACK！消息会无限重投
    }
};
```

**适用场景**：Coordinator Events 元数据同步
**行为**：反序列化失败 → 不 ACK → 30秒后重投 → 无限循环
**风险**：坏消息会永远阻塞队列，持续占用资源

**策略 B：失败主动 ACK（保护性丢弃）**

**实现位置**：`src/service/trial_quota.rs:444-452`

```rust
let ha_msg: TrialQuotaHaMsg = match json::from_slice(payload) {
    Ok(m) => m,
    Err(e) => {
        log::error!("[TRIAL_QUOTA] Failed to deserialize HA message: {e}");
        if let Err(e) = msg.ack().await {  // ✅ 主动发送 ACK！
            log::error!("[TRIAL_QUOTA] Failed to ack HA message: {e}");
        }
        continue;  // 丢弃坏消息
    }
};
```

**适用场景**：试用配额 HA 同步
**行为**：反序列化失败 → 主动 ACK → 消息删除 → 防止队列阻塞
**代价**：可能丢失部分数据，但保证系统可用性

### 10.3 ACK 超时与消息重投的真实流程

**NATS JetStream 重投触发条件**：

1. **消费者离线重投**：消费者断开连接，消息未 ACK
2. **ACK 超时重投**：消费者在线但处理超时（30秒）未 ACK
3. **NAK 主动重投**：消费者发送 NAK 要求重投（代码中未使用）

**重投后的节点变化流程** (`src/infra/src/queue/nats.rs:128-202`)：

```
消费者 A 接收消息
      │
      ▼
开始业务处理
      │
      ├──────────────────────┐
      │ 成功                  │ 失败/超时
      ▼                        ▼
   发送 ACK             不发送 ACK
      │                        │
      ▼                        │
   消息删除               │
                               │
                               ▼
                       等待 30 秒 (ack_wait 默认值)
                               │
                               ▼
                       消息变为待投递状态
                               │
                               ▼
                       NATS 选择消费者
                               │
                               ├──────────────────────────┐
                               │ 消费者 A 在线           │ 消费者 A 离线
                               ▼                           ▼
                         重新投递到消费者 A         投递到其他在线消费者
                         (同节点重投)            (节点切换)
                               │
                               ▼
                         无限循环直到成功
                         (无 max_deliver 限制)
```

> **关键区别**：
> - **同节点重投**：消费者 A 仍在线 → 继续投递给 A（可能持续失败）
> - **节点切换**：消费者 A 离线 → NATS 选择其他在线消费者 B 投递

### 10.4 节点在线状态与消息投递的关联

**节点状态对消息投递的影响链**：

```
节点状态变化
      │
      ├──────────────────────────────────────┐
      │ 节点 Online                         │ 节点 Offline
      ▼                                        ▼
┌─────────────────────┐             ┌─────────────────────┐
│ 消费者保持连接      │             │ 消费者断开连接      │
│ (NATS 认为可用)     │             │ (NATS 认为不可用)   │
└──────────┬──────────┘             └──────────┬──────────┘
           │                                     │
           ▼                                     ▼
┌─────────────────────┐             ┌─────────────────────┐
│ 消息继续投递给本节点 │             │ NATS 触发重平衡     │
│ (即使处理失败)       │             │ 消息重新分配        │
└─────────────────────┘             └──────────┬──────────┘
                                                 │
                                                 ▼
                                       ┌─────────────────────┐
                                       │ 选择其他在线节点    │
                                       │ 投递未 ACK 消息     │
                                       └─────────────────────┘
```

**超级集群队列的节点约束** (`src/super_cluster_queue/mod.rs:139-150`)：

```rust
if LOCAL_NODE.is_compactor() {  // ⚠️ 只有 Compactor 角色的节点才消费！
    tokio::task::spawn(async move {
        loop {
            if is_offline() {
                break;
            }
            if let Err(e) = queue.subscribe().await {
                log::error!("[SUPER_CLUSTER:sync] failed to subscribe: {e}");
            }
        }
    });
}
```

**职责边界**：
- ✅ **发布**：任何节点都可以发布消息到超级集群队列
- ✅ **消费**：只有 Compactor 角色的节点才能消费超级集群队列
- ⚠️ **故障转移**：Compactor 节点离线 → 其他 Compactor 节点接管

### 10.5 节点离线后的责任转移完整流程

**阶段 1：节点离线检测** (`src/infra/src/cluster/mod.rs:406-481`)

```
节点正常运行
      │
      ▼
心跳协程定期更新 TTL
(默认 15 秒心跳，TTL 60 秒)
      │
      ▼
节点崩溃/网络中断
      │
      ▼
停止心跳更新
      │
      ▼
NATS KV TTL 过期 (60 秒后)
      │
      ▼
节点状态从 KV 中消失
      │
      ▼
节点列表 Watcher 触发变更事件
```

**阶段 2：一致性哈希重平衡**

```
节点下线事件触发
      │
      ▼
从本地缓存移除节点
      │
      ▼
从一致性哈希环移除节点
(删除该节点的所有虚拟节点)
      │
      ▼
哈希环空间重新分配
      │
      ▼
原节点的哈希区间
分配给相邻节点
```

**阶段 3：消息消费责任转移**

```
原消费者节点离线
      │
      ▼
NATS JetStream 检测到消费者断开
      │
      ▼
消费者"待处理消息"变为未确认
      │
      ▼
等待 ack_wait (30 秒)
      │
      ▼
NATS 触发重投
      │
      ▼
选择其他在线 Compactor 节点
      │
      ▼
消息投递给新节点
      │
      ▼
新节点处理并 ACK
(如果成功)
```

**文件列表广播中的节点变化处理** (`src/service/db/file_list/broadcast.rs:193-210`)：

```rust
loop {
    // 等待节点恢复在线
    loop {
        match cluster::get_node_by_uuid(&node.uuid).await {
            None => {
                EVENTS.write().await.remove(&node.uuid);
                log::error!("[broadcast] node[{}] leaved cluster, dropping events",
                    &node.grpc_addr,
                );
                return Ok(());  // ⚠️ 节点永久离开：丢弃积压事件
            }
            Some(v) => {
                if v.status == NodeStatus::Online {
                    break;  // 节点恢复：继续发送
                }
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
```

> **设计权衡**：文件列表广播采用"节点离开则丢弃事件"策略，而不是等待重投。这是基于实时性优先的设计选择。

### 10.6 消息重投的真实生命周期

**阶段 1: 消息入列与初始投递**
- 消息发布到 NATS JetStream
- 持久化存储（File 存储类型）
- 推送到匹配的消费者

**阶段 2: 处理与 ACK 决策**
- 消费者接收消息
- 业务逻辑处理
- 成功 → 发送 ACK → 消息删除
- 失败 → 两种策略：
  - **策略 A**：不发送 ACK → 等待 30 秒超时
  - **策略 B**：主动发送 ACK → 消息删除（保护性丢弃）

**阶段 3: 重投决策**
- NATS 检测未 ACK 消息（30 秒后）
- 检查消费者在线状态
- 选择目标消费者：
  - 消费者在线 → 继续投递给同一消费者
  - 消费者离线 → 投递到其他在线消费者（节点切换）

**阶段 4: 无限循环**
- 重复阶段 2-3，直到：
  - 消息处理成功并 ACK
  - 业务代码主动 ACK 丢弃
  - 消息超过 `max_age`（默认 60 天）被 Stream 清理

> ⚠️ **没有"超过最大投递次数"的终止条件！** 消息会一直重投直到成功或过期。

### 10.7 重试与退避策略

**文件广播中的应用层重试** (`src/service/db/file_list/broadcast.rs:261-294`)：

```rust
let mut wait_ttl = 1;
let mut retry_ttl = 0;
loop {
    if retry_ttl >= 1800 {
        log::error!("[broadcast] to node[{}] timeout, dropping event, already retried for 30 minutes",
            &node.grpc_addr
        );
        break;  // ⚠️ 应用层超时：主动放弃
    }
    match client.send_file_list(request).await {
        Ok(_) => break,
        Err(e) => {
            log::error!("[broadcast] send event to node[{}] failed: {}, retrying...",
                &node.grpc_addr,
                e
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(wait_ttl)).await;
            retry_ttl += wait_ttl;
            if wait_ttl < 60 {
                wait_ttl *= 2  // 指数退避: 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s...
            };
        }
    }
}
```

**退避策略特点**：
- 初始等待：1 秒
- 指数增长：每次重试等待时间翻倍
- 最大等待：60 秒
- 总超时：30 分钟（1800 秒）后主动放弃

> **关键区别**：这是**应用层**的重试逻辑，与 NATS JetStream 的消息重投是两个独立的机制。

## 11. 总结：关键发现与修正

### 11.1 核心修正（基于 NATS 官方文档 + 真实代码分析）

| 之前的错误结论 | 真实代码行为 | 证据位置 |
|---------------|-------------|---------|
| 重投有上限配置 | **无重投上限，无限重投** | `src/infra/src/queue/nats.rs:152` 使用 `Default::default()`，NATS 默认 `max_deliver = -1` |
| 有死信队列机制 | **无死信队列** | NATS 原生不支持 DLQ，OpenObserve 也未实现 |
| 超过次数丢弃消息 | **永不丢弃，直到成功或被 Stream 清理** | `max_deliver = -1` 无限重投，`max_age = 60` 天（`ZO_NATS_QUEUE_MAX_AGE`） |
| ack_wait 可配置 | **固定 30 秒（NATS 默认）** | 代码中未配置，使用 NATS 服务器默认值 |
| max_ack_pending = 1024 | **max_ack_pending = 1000** | NATS 官方文档确认默认值为 1000 |

### 11.2 两种 ACK 策略的权衡

| 策略 | 实现位置 | 行为 | 适用场景 | 风险 |
|------|---------|------|---------|------|
| **失败不 ACK** | `src/infra/src/coordinator/events.rs` | 不发送 ACK，无限重投 | 元数据同步（必须保证不丢失） | 坏消息永久阻塞队列 |
| **失败主动 ACK** | `src/service/trial_quota.rs` | 主动 ACK，丢弃坏消息 | 统计数据（可用性优先） | 可能丢失部分数据 |

### 11.3 节点状态与消息投递的关联

```
节点状态变化
      │
      ├──────────────────────────────────────────────┐
      │ Online                                     │ Offline
      ▼                                              ▼
┌─────────────────────────┐               ┌─────────────────────────┐
│ 消费者保持连接          │               │ 消费者断开连接          │
│ NATS 认为可用           │               │ NATS 触发重平衡         │
└─────────────┬───────────┘               └─────────────┬───────────┘
              │                                         │
              ▼                                         ▼
┌─────────────────────────┐               ┌─────────────────────────┐
│ 消息继续投递给本节点    │               │ 30 秒后重投到其他节点   │
│ (即使持续处理失败)      │               │ (节点切换)               │
└─────────────────────────┘               └─────────────────────────┘
```

**超级集群队列的特殊约束** (`src/super_cluster_queue/mod.rs:139`)：
- ✅ **发布**：任意节点可发布
- ✅ **消费**：仅 Compactor 角色节点可消费
- ⚠️ **故障转移**：Compactor 离线 → 其他 Compactor 接管

### 11.4 OpenObserve 集群队列系统的核心设计原则

1. **可靠性优先但有边界**：通过发布确认保证消息入列，但消费端 ACK 策略由业务决定
2. **最终一致性依赖幂等**：消息可能重复投递，消费端必须保证幂等处理
3. **高可用性由 NATS 提供**：持久化消费者 + Stream 复制 + 自动重平衡
4. **可扩展性通过角色隔离**：一致性哈希 + 角色分组实现任务细粒度分配
5. **无死信是设计选择**：宁愿消息无限重投，也不轻易丢弃（元数据场景）

### 11.5 三个核心环节的协作关系

| 环节 | 核心职责 | 与其他环节的关联 |
|------|---------|----------------|
| **消息入列** | 数据入口，发布确认保证 | 与节点状态无关，任何节点可发布 |
| **节点选举/角色** | 决定谁能消费 | 角色限制决定消费权限，哈希环决定任务分配 |
| **确认回执** | 驱动消息生命周期 | ACK 策略决定消息是否重投，节点状态决定重投目标 |

### 11.6 节点职责边界总结

| 组件 | 核心职责 | 边界 |
|------|---------|------|
| **节点角色** | 执行特定类型任务 | 按 Role/RoleGroup 严格划分消费权限 |
| **一致性哈希** | 任务分配路由 | 仅负责哈希映射，不负责消息投递本身 |
| **分布式锁** | 关键操作互斥 | 仅保护原子性，不参与消息队列逻辑 |
| **NATS JetStream** | 消息可靠投递 | 仅负责传输存储，不关心业务处理结果 |
| **心跳机制** | 维持节点在线状态 | 影响哈希环和消费者状态，但不直接处理消息 |
| **业务代码** | ACK 策略决策 | 最终决定消息是确认丢弃还是等待重投 |
