# Program Design Test: Order Matching Distribution 

这是一个用 Go 编写的订单与骑手匹配模拟项目,用于验证在不同骑手规模、订单规模和运行时长约束下，系统能否持续生成订单并完成分配。

项目从先前的单机订单匹配 benchmark出发，在分布式方向上进行了拓展事件日志、分片拥有权、持久化、节点故障切换、epoch屏障和可观测指标等等的功能。

其中核心逻辑先由我个人手写实现，AI参与了后续优化工作，主要内容如下

- 设计事件字段Event、事件日志EventLog
- 拥有权ownership、持久化checkpoint、存活检测membership：
  - ownership是一个三元组(shard, node, epoch)，表示某个shard由哪个node在当前epoch拥有
  - checkpoint记录了某一时刻下日志的Offset，epoch以及shard-node对，表示某个shard已经处理到哪个事件位置
  - membership是一个接口，定义了检验节点是否存活的动作
- 划分node，controller职责：
  - controller负责维护shard ownership，节点心跳失效后重新分配shard
  - node负责消费自己持有的shard并应用事件
- 节点保活：

  - 节点故障后让shard进行迁移，但是对于已经执行了操作后在持久化保存时宕机的情况，不加入DB事务锁可能会在其他node接手后进行重复操作，后面设计了orderState给正在进行持久化动作时标记node状态为pending，但是意识到就算如此也要考虑标记过程是否也会宕机，可能这个问题不使用事务很难去进行一个完全的处理
- 事件应用前后使用fencing校验，防止脏写入：具体而言就是维护一个版本号epoch，更新shard归属时就会自增，类似CAS的思路
- Controller的一部分高可用能力

  - 通过etcd的租约以及事务锁，保证唯一leader的存在，在接入etcd前手动写了一个raft的简单实现，第二天起来给节点接入etcd时忘记这回事了，重新写的时候又觉得我直接用etcd不就好了...
- 节点负载均衡
  - 使用一致性哈希分配shard，默认一个node在哈希环上有32份，不过因为shard太少(64个)可能会导致分布效果有点平庸。
  - 通过controller更改shard的所属者，当node中的refreshOnce检测到shard归属变化时将其从自己的map中移除并执行context.Cancel，而node的执行层中设置了两层fence隔离，这里会有三种情况，1是在第一层fence前检测到归属变化，安全退出，2是在通过第一层后执行操作时变动，已经发生了操作，但是不会推进到持久化保存，3是正在发生操作，会因为收到Ctx.Done而取消操作
  

主要写了事件从生成到分配到对应节点处理的整个流程并从内存态实现转移到文件态实现，最后调用中间件完成最终实现。（由于先抽出接口写了内存实现和文件实现来验证能不能跑起来，所以项目中存在不少懒加载默认是加载内存或者文件实现）

其中AI给出了一部分修改意见并且对项目进行了重构与测试，全程接管了测试、指标收集和可观测性的构造

后面逐步替换为中间件实现，比如使用Kafka作为事件日志，etcd作为分布式协调和持久化存储，顺便用来做控制器的选举制度，目前还处于一个接口多种实现的中间态，使用Prometheus/Grafana作为可观测指标和告警（这部分是AI构造的）。

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
  node/             node CLI，启动心跳、runner和metrics
  producer/         producer CLI，向Kafka写入订单事件
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

Prometheus:

```text
http://localhost:9090
```

Grafana:

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

如果本机并发或 vet 环境不稳定，可以用更保守的方式：

```powershell
$env:GOFLAGS='-p=1'
go test -vet=off ./...
```

校验 Prometheus 配置和告警规则：

```powershell
docker run --rm --entrypoint promtool -v "${PWD}\observability:/etc/prometheus:ro" prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml
docker run --rm --entrypoint promtool -v "${PWD}\observability:/etc/prometheus:ro" prom/prometheus:v2.55.1 check rules /etc/prometheus/alerts.yml
```

## 设计边界

- Kafka/etcd 都是本地单实例配置，不具备生产级高可用。
- producer 是批量写入，不是长期运行的服务。
- order state 仍是简化模型。
- benchmark 暂时后置，后续再补吞吐、延迟、failover 恢复时间等指标。
