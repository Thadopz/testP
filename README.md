# Program Design Test: Order Matching Distribution

这是一个用 Go 编写的订单与骑手匹配模拟项目，用于验证在不同骑手规模、订单规模和运行时长约束下，系统能否持续生成订单并完成分配。

项目从先前的单机订单匹配 benchmark 出发，在分布式方向上进行了拓展：事件日志、分片拥有权、持久化、节点故障切换和可观测指标等功能，还有一些一致性与幂等问题还在完善中。

## 设计目标

把原来的单机匹配程序扩展为一条分布式订单处理链路：订单和骑手的状态变化被包装为事件，不同`node`按`shard`分担消费任务，并且通过共享的事件日志和协调存储协同工作。

初步规划中主要解决以下问题：

1. 任务的拆分：将订单映射到`shard`，每个 `node` 只处理自己拥有的 `shard`，避免所有节点竞争同一份数据。
2. 故障处理以及转移：`node` 失效后，`controller` 将其` shard `迁移给存活节点；旧`node`恢复后不能继续写入已经失去拥有权（ownership）的`shard`。
3. 进度恢复：为每个 shard 保存 checkpoint，使新 owner 能从上次处理位置继续消费。
4. 控制器高可用：允许启动多个 controller，但通过raft的leader election保证同一时间只有一个`controller`修改ownership。
5. 负载均衡：使用一致性哈希规划shard归属，并在节点变化时逐步再平衡。

## 核心对象与不变量

核心对象包括：

- `Order`：待匹配的订单；
- `Rider`：可参与匹配的骑手；
- `Event`：订单或骑手状态变化；
- `Shard`：事件和处理责任的分区；
- `Ownership`：当前 shard 的合法处理者；
- `Checkpoint`：记录该 shard 下一条待处理事件的位置；
- `Membership`：描述 node 是否仍然存活。

系统希望维持的不变量：

- 同一时刻一个 `shard` 只有一个合法 `owner`；
- `shard ownership` 每次变更都增加 `epoch`；
- 同一个 `shard` 内事件按 `offset` 顺序处理；
- 事件成功应用后才能推进 `checkpoint`；
- 旧 `epoch` 的 `node` 不允许继续写入；
- 同一订单事件重复消费时，结果应尽量保持幂等。

## 实现思路

### 1. 单机匹配要转移到事件驱动

项目最初只有单机匹配器和benchmark，之后将订单、骑手的变化统一建模为 `Event`。事件包括订单创建、取消、重试、匹配成功、匹配失败，以及骑手上线、移动和下线。

`EventLog` 抽象出三个动作：`Append` 追加事件，`TailFrom` 从指定 offset 持续读取日志，`EndOffset` 用来获取 shard 的日志末尾位置。一开始是使用内存和文件实现验证流程，分布式运行时再调Kafka实现。业务shard与Kafka partition按编号对应，从而保留同一shard内的事件顺序。

### 2. 划分控制面与数据面

- `controller` 属于控制面，负责leader election、读取membership、初始化ownership，以及执行故障转移failover和rebalance。
- `node` 属于数据面，负责发送心跳、读取自己拥有的shard、加载checkpoint，并为每个shard启动事件消费任务（后续被shardworker整体封装）。

多个 controller 可以同时运行，但只有etcd选出的leader可以修改ownership。node定期通过带 TTL 的membership续租；心跳过期后，controller 会把故障节点的 shard 迁移给其他存活节点。

### 3. 解码与应用事件

`Codec`只负责事件的编码和解码，`Applier` 负责把事件转换成业务动作。例如`order_created` 会被转换为 `engine.SubmitBatch()`，订单结果则更新到order state。拆分这两个职责后，事件格式、日志实现和匹配逻辑就可以分别测试了。

### 4. Ownership、checkpoint 与 fencing

`Ownership` 是一个三元组 `(shard, node, epoch)`。etcd 使用 CAS 方式更新 ownership，每次分配都会增加 epoch。shard worker 启动时保存自己取得的 epoch，并在应用事件前后与 etcd 中的当前 ownership 比较；如果 node 或 epoch 已发生变化，则旧 worker 停止处理。

事件应用成功后，node 才会推进 checkpoint。节点重启或 shard 迁移时，新 owner 从 checkpoint 记录的 offset 继续消费。因此当前处理语义接近 at-least-once：如果 node 在 apply 之后、checkpoint 保存之前宕机，同一事件可能会被再次执行，这一点需要DB提供事务锁，但是引入DB又要处理与消息队列之前天然不能构成事务的矛盾，同时对性能也有一定的影响。

前后两次fencing校验可以发现ownership已经变化，但不能撤销检查之间已经发生的副作用。order state、匹配动作和 checkpoint 目前也不在同一个事务中，所以 `LastEventID` 只能提供有限的重复识别能力，还是要落到幂等表做乐观锁。

### 5. 分片匹配与负载均衡

订单首先在坐标对应的 home shard 中查找骑手；如果没有合适候选者，matching engine 会访问预先计算的邻居 shard，并逐步扩大搜索半径，综合距离与骑手负载选择结果。

当前跨 shard 搜索发生在同一 node 进程内，打算是做跨节点 RPC，后续可以让各shard owner提供候选查询和条件预约接口：先并行收集 TopK 候选，再按顺序尝试原子预约，骑手设置一个订单上限，类似一个带缓冲区的channel吧，满了就阻塞。

shard 默认通过一致性哈希分配给存活节点，每个 node 在哈希环上使用 32 个虚拟节点。当前只有 64 个 shard，节点数量较少时分布仍可能不够均匀，导致效果有点平庸。

### 6. 实现演进与当前边界

项目是逐步演进的：

单机匹配器
做分片匹配引擎
事件日志与回放
checkpoint 恢复
node/controller 职责拆分
membership 与故障转移
ownership epoch 与 fencing
Kafka、etcd 和可观测性
改进单机匹配器，与分布式适配（主要是先前是直接在单机内生成骑手的，导致每一个engine里都有固定的骑手）

当前仍有很多重要边界：order state 暂时存储在 etcd 中；骑手和匹配状态主要位于 node 内存；Kafka 消费重试、无法解码事件和死信队列还没想好怎么写；controller shard 数、matching engine shard 数与 Kafka partition 数也需要由使用者保持一致，有一些地方图省事写了些魔法数字，以及硬编码了一致性哈希的初始份数。

核心匹配逻辑以及核心的对象最初由手打实现，后续借助 AI 完成了部分重构、测试、指标收集和可观测性配置，完成docker compose以及相关yaml的编写，在思路上也有一定的纠正与指导。

## 当前能力

- 使用 Kafka 作为订单事件日志。
- 使用 etcd 保存节点 membership、shard ownership、controller election、checkpoint 和 order state。
- node 按当前 ownership 动态消费自己负责的 shard。
- controller 通过 leader election 保证同一时间只有一个 active controller 做 ownership 维护。
- node 心跳失效后，controller 会把 dead node 的 shard 迁移给存活节点。
- ownership 带 epoch，applier 会做 fencing 校验，避免旧 owner 在恢复后继续写入过期状态。
- checkpoint 在事件成功 apply 后推进，整体语义接近 at-least-once。
- 暴露 Prometheus 指标，并提供 Grafana dashboard。

## 核心流程

```mermaid
flowchart LR
    Producer["Producer<br/>生成订单事件"] -->|"写入 order_created"| Kafka[("Kafka<br/>事件日志")]

    subgraph ControlPlane["控制面"]
        Controller["Controller<br/>选主与分片调度"]
        Etcd[("etcd<br/>membership / ownership / epoch")]
        Controller <-->|"选主、检查存活、分配 shard"| Etcd
    end

    subgraph DataPlane["数据面"]
        Node["Node<br/>消费所拥有的 shard"]
        Applier["Event Applier<br/>fencing 校验与事件应用"]
        Engine["Matching Engine<br/>订单与骑手匹配"]
        OrderState[("Order State<br/>订单处理状态")]
        Checkpoint[("Checkpoint<br/>消费进度")]

        Node --> Applier --> Engine
        Applier --> OrderState
        Node --> Checkpoint
    end

    Node -->|"心跳、读取 ownership"| Etcd
    Kafka -->|"按 shard 从 checkpoint 继续消费"| Node
    Engine -->|"写入 matched / missed 事件"| Kafka
    OrderState -.->|"当前实现存储于"| Etcd
    Checkpoint -.->|"当前实现存储于"| Etcd
```

1. `cmd/producer` 生成 `order_created` 事件并写入 Kafka。
2. `cmd/controller` 竞选 leader，读取 etcd 中的节点 membership，并维护 shard 到 node 的 ownership。
3. `cmd/node` 定期写入心跳，读取自己持有的 shard ownership。
4. node 从 Kafka 按 shard 读取事件，从 etcd shard checkpoint 继续处理。
5. event applier 在 apply 前校验 ownership epoch，过期 owner 会被拒绝。
6. apply 成功后，node 写入 order state，并推进该 shard 的 checkpoint。
7. 如果某个 node 心跳过期，controller 会把它持有的 shard 重新分配给其他 alive node。

## 目录结构

```text
cmd/
  controller/       controller CLI，主要负责参数解析
  node/             node CLI，启动心跳、runner 和 metrics
  producer/         producer CLI，向 Kafka 写入订单事件
internal/
  applier/          event -> engine/order state/checkpoint 的应用逻辑
  checkpoint/       shard checkpoint 存储
  cluster/          election、membership、ownership、failover 等集群协调逻辑
  controllerapp/    controller 运行时组装
  engine/           分片订单匹配引擎
  eventlog/         Kafka event log 与事件 codec
  matcher/          骑手匹配与空间索引
  metrics/          Prometheus 指标
  node/             runner 与 shard 消费逻辑
  nodeapp/          node 运行时组装
  orderstate/       order state 存储
  producerapp/      producer 运行时组装
  shard/            shard layout
  tools/            测试与工具函数
observability/      Prometheus、Grafana、告警规则
scripts/            本地 e2e 与 Kafka 辅助脚本
```

## 环境要求

- Go 1.25+
- Docker Desktop
- PowerShell

本地分布式链路默认依赖 Docker Compose 启动 Kafka、etcd、Prometheus 和 Grafana。

## 快速运行

启动中间件和观测组件：

```powershell
docker compose up -d
```

创建 Kafka topic：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\kafka_create_topic.ps1 -Topic order-events -Partitions 64
```

启动一个 node：

```powershell
go run ./cmd/node -node-id 1 -metrics-addr :9101
```

启动 controller：

```powershell
go run ./cmd/controller -metrics-addr :9102
```

写入一批订单事件：

```powershell
go run ./cmd/producer -orders 100 -metrics-addr :9103
```

默认地址：

```text
Kafka:      127.0.0.1:9092
etcd:       127.0.0.1:2379
etcd prefix /testp
topic:      order-events
```

## 观测

Prometheus：

```text
http://localhost:9090
```

Grafana：

```text
http://localhost:3000
```

Grafana dashboard 会通过 provisioning 自动加载。Prometheus 默认抓取：

```text
node:       host.docker.internal:9101
controller: host.docker.internal:9102
producer:   host.docker.internal:9103
```

需要注意：producer 当前是短生命周期进程，如果运行太快结束，Prometheus 可能来不及抓到它的指标。

## 故障转移演示

先启动两个 node，注意第二个 node 使用不同的 metrics 端口：

```powershell
go run ./cmd/node -node-id 1 -metrics-addr :9101
go run ./cmd/node -node-id 2 -metrics-addr :9104
```

再启动 controller：

```powershell
go run ./cmd/controller -metrics-addr :9102
```

停止 node 1 后，等待 membership TTL 和 controller sweep 生效。controller 会识别 node 1 已失效，并把它持有的 shard 分配给仍然 alive 的 node。

如果想用脚本跑一轮本地验证，可以参考：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\e2e_failover.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\e2e_kafka_eventlog.ps1
```

## 常用参数

`cmd/node`：

```text
-node-id              node id，默认 1
-data-dir             本地数据目录，默认 ./data
-riders               初始骑手数量，默认 100
-workers              engine worker 数量，默认 2
-seed                 随机种子，默认 1
-heartbeat-interval   心跳间隔，默认 1s
-membership-ttl       membership TTL，默认 5s
-metrics-interval     控制台指标打印间隔，默认 5s
-metrics-addr         Prometheus 地址，默认 :9101
-etcd-endpoints       etcd 地址，默认 127.0.0.1:2379
-etcd-prefix          etcd key 前缀，默认 /testp
-kafka-brokers        Kafka broker，默认 127.0.0.1:9092
-kafka-topic          Kafka topic，默认 order-events
```

`cmd/controller`：

```text
-controller-id        controller election id，默认 hostname-pid
-etcd-endpoints       etcd 地址，默认 127.0.0.1:2379
-etcd-prefix          etcd key 前缀，默认 /testp
-election-ttl         controller election TTL，默认 5s
-membership-ttl       membership TTL，默认 5s
-sweep-interval       dead node 扫描间隔，默认 1s
-shards               shard 数量，默认 64
-metrics-addr         Prometheus 地址，默认 :9102
```

`cmd/producer`：

```text
-orders               写入订单数，默认 100
-start-id             起始订单 ID，默认 1
-seed                 随机种子，默认 1
-metrics-addr         Prometheus 地址，默认 :9103
-kafka-brokers        Kafka broker，默认 127.0.0.1:9092
-kafka-topic          Kafka topic，默认 order-events
```

## 测试

运行全部测试：

```powershell
go test ./...
```

校验 Prometheus 配置和告警规则：

```powershell
docker run --rm --entrypoint promtool -v "${PWD}\observability:/etc/prometheus:ro" prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml
docker run --rm --entrypoint promtool -v "${PWD}\observability:/etc/prometheus:ro" prom/prometheus:v2.55.1 check rules /etc/prometheus/alerts.yml
```
