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

## 9. 总结

OpenObserve 集群队列系统的核心设计原则：

1. **可靠性优先**：通过两层 ACK（发布确认 + 消费确认）保证消息不丢失
2. **最终一致性**：通过消息重放和幂等处理保证集群状态最终一致
3. **高可用性**：持久化消费者 + 自动重连 + 健康检查保障系统容错能力
4. **可扩展性**：一致性哈希算法支持动态节点扩缩容，任务自动重平衡

三个核心环节的协作关系：
- **消息入列**是数据入口，提供基础的可靠性保证
- **节点选举**决定了消息的消费主体和任务分配
- **确认回执**是消息状态流转的关键，驱动消息投递生命周期
