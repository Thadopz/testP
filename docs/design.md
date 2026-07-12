# 系统设计

本文记录项目从单机匹配模型演进为分布式订单处理链路时的设计目标、核心对象、不变量和实现思路。

## 目录

- [1. 设计目标](#1-设计目标)
- [2. 核心对象与不变量](#2-核心对象与不变量)
- [3. 实现思路](#3-实现思路)

## 1. 设计目标

把原来的单机匹配程序扩展为一条分布式订单处理链路：订单和骑手的状态变化被包装为事件，不同 logical `node` 按 `shard` 分担消费任务，并且通过 Kafka、PostgreSQL Outbox/Inbox 和 etcd ownership 协同工作。

初步规划中主要解决以下问题：

1. 任务拆分：将订单映射到 `shard`，每个 `node` 只处理自己拥有的 `shard`，避免所有节点竞争同一份数据。
2. 故障转移：`node` 失效后，`controller` 将其 `shard` 迁移给存活节点；旧 `node` 恢复后不能继续写入已经失去 ownership 的 `shard`。
3. 持久化：为每个 shard 保存 checkpoint，使新 owner 能从上次处理位置继续消费。
4. 控制器需要保持高可用：允许启动多个 controller，通过 etcd election 保证同一时间只有一个 controller 修改 ownership。
5. 负载均衡：用一致性哈希规划 shard 归属，并在节点变化时逐步再平衡。
6. 跨资源一致性：学习中发现PostgreSQL 与 Kafka 不能组成天然事务，因此业务更新只在 PostgreSQL 本地事务中完成，跨 Kafka 的发布通过 Outbox、租约重试和 Inbox 幂等保证最终投递。

## 2. 核心对象与不变量

核心对象包括：

- `Order`：待匹配的订单；
- `Rider`：可参与匹配的骑手；
- `Event`：订单或骑手状态变化；
- `Shard`：事件和处理责任的分区；
- `Ownership`：当前 shard 的合法处理者；
- `Checkpoint`：记录该 shard 下一条待处理事件的位置；
- `Membership`：描述 node 是否仍然存活。

系统希望维持以下不变量：

- 同一时刻一个 `shard` 只有一个合法 `owner`；
- `shard ownership` 每次变更都增加 `epoch`；
- 同一个 `shard` 内事件按 `offset` 顺序处理；
- 事件成功应用后才能推进 `checkpoint`；
- 旧 `epoch` 的 `node` 不允许继续写入；
- 同一订单事件重复消费时，结果应保持幂等。

## 3. 实现思路

### 3.1 从单机匹配转向事件驱动

项目最初只有单机匹配器和 benchmark，之后将订单、骑手的变化统一建模为 `Event`。事件包括订单创建、取消、重试、匹配成功、匹配失败，以及骑手上线、移动和下线。

`EventLog` 抽象出三个动作：`Append` 追加事件，`TailFrom` 从指定 offset 持续读取日志，`EndOffset` 获取 shard 的日志末尾位置。一开始使用内存和文件实现验证流程，当前分布式运行时主要使用 Kafka。业务 shard 与 Kafka partition 按编号对应，从而保留同一 shard 内的事件顺序。

引入 PostgreSQL 后，订单状态、Inbox、Outbox 和 checkpoint 被收敛到同一个本地事务里；Kafka 发布不直接进入业务事务，而是由 Outbox Publisher 异步发布。

### 3.2 划分控制面与数据面

- `controller` 属于控制面，负责 leader election、读取 membership、初始化 ownership，以及执行 failover 和 rebalance。
- `cmd/node` 属于订单消费侧，也可以理解为 Order Worker，负责消费 `order-events`、应用订单状态、写入匹配请求 Outbox 和推进 checkpoint。
- `cmd/matcher-worker` 属于匹配消费侧，负责消费 `match-requests`、筛选候选骑手、在 PostgreSQL 事务中预约骑手，并写入匹配结果 Outbox。
- `cmd/node` 与 `cmd/matcher-worker` 是独立进程，但当前共享同一套 logical node-id 与 shard ownership。`node-id=1` 的两个 worker 都只处理 ownership 属于 logical node 1 的 shard。

多个 controller 可以同时运行，但只有 etcd 选出的 leader 可以修改 ownership。worker 定期通过带 TTL 的 membership 续租；心跳过期后，controller 会把故障 logical node 的 shard 迁移给其他存活节点。

### 3.3 解码与应用事件

`Codec` 只负责事件的编码和解码。`Applier` 负责把订单事件转换成 PostgreSQL 事务内的业务动作。例如，`order_created` 会写入订单状态、Inbox、`match_requested` Outbox 和 checkpoint，随后由 Outbox Publisher 推送给 Matcher Worker。

Matcher Worker 收到匹配请求后，也会在自己的事务中完成 Inbox 去重、订单锁定、骑手预约、结果 Outbox 和 checkpoint。

事务只包含 PostgreSQL 内能够原子提交的动作，Kafka 发布通过 Outbox/Inbox 保证至少一次投递和幂等消费。

### 3.4 Ownership、Checkpoint 与 Fencing

`Ownership` 是一个三元组 `(shard, node, epoch)`。etcd 使用 CAS 方式更新 ownership，每次分配都会增加 epoch。shard worker 启动时保存自己取得的 epoch，并在应用事件前后与 etcd 中的当前 ownership 比较；如果 node 或 epoch 已发生变化，则旧 worker 停止处理。

### 3.5 分片匹配与负载均衡

订单首先根据坐标映射到 home shard。当前匹配流程已经从 `cmd/node` 中拆出：Order Worker 只写入 `match_requested` Outbox，Matcher Worker 消费 `match-requests` 后使用内存候选索引筛选 TopK，并通过 PostgreSQL 条件更新预约骑手。

内存索引只负责候选计算，最终是否能占用骑手由数据库事务裁决。这样避免把内存中的匹配状态和 SQL 事务混在一起，也允许 Publisher 失败后通过 Outbox 重试、消费者通过 Inbox 去重。

当前 Matcher Worker 仍然是简化实现：候选索引由本进程按 shard 初始化，跨节点候选查询还没有做成独立 RPC。后续如果继续业务化，可以让各 shard owner 提供候选查询和条件预约接口：先并行收集 TopK 候选，再按顺序尝试数据库侧原子预约。

shard 默认通过一致性哈希分配给存活节点，每个 node 在哈希环上使用 32 个虚拟节点。当前只有 64 个 shard，节点数量较少时分布仍可能不够均匀。

### 3.6 实现演进与当前边界

项目按以下顺序逐步演进：

1. 单机匹配器与 benchmark。
2. 事件日志与回放，加入 checkpoint 恢复。
3. 拆分 controller 与 node，加入 membership、ownership epoch 和 fencing。
4. 引入 Kafka、etcd、Prometheus 和 Grafana。
5. 引入 PostgreSQL，用本地事务统一提交 Inbox、业务状态、Outbox 和 checkpoint。
6. 移除旧 direct engine 路径，把匹配请求改为 `order-events -> Outbox -> match-requests -> Matcher Worker`。
7. 为 Producer 和 Outbox Publisher 补充批量 Kafka I/O，并增加端到端压测脚本。
8. 增加 `retry-producer`，验证订单从 `missed` 重新进入匹配流程时的状态转换和 Inbox 幂等。
9. 增加骑手容量压测，验证不同骑手、订单规模下的吞吐和容量上限。


