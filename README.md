# Program Design Test: Order Matching Distribution

这是一个用 Go 编写的订单与骑手匹配模拟项目，用于验证在不同骑手规模、订单规模和运行时长约束下，系统能否持续生成订单并完成分配。

项目从先前的单机订单匹配 benchmark 出发，在分布式方向上进行了拓展：事件日志、分片拥有权、持久化、节点故障切换和可观测指标等功能，初步确保了一致性与幂等，但是性能上还有优化的空间。

核心匹配逻辑、核心对象以及具体架构（虽然后面被推翻重建了）最初由手写实现，后续借助 AI 完成了部分重构、测试、指标收集、可观测性配置、Docker Compose 和相关 YAML，并对部分设计思路进行了修正。

## 核心流程

```mermaid
flowchart LR
    Producer["Producer"] -->|"order_created"| OrderTopic[("Kafka: order-events")]
    RetryProducer["Retry Producer<br/>测试工具"] -->|"order_retry_request"| OrderTopic

    subgraph ControlPlane["控制面"]
        Controller["Controller<br/>选主与分片调度"]
        Etcd[("etcd<br/>membership / ownership / epoch")]
        Controller <-->|"选主、检查存活、分配 shard"| Etcd
    end

    subgraph OrderWorkerPlane["订单消费侧"]
        OrderWorker["Order Worker<br/>cmd/node"]
        OrderTx["PG 事务<br/>Inbox + Orders + MatchRequest Outbox + Checkpoint"]
    end

    subgraph PublishPlane["Outbox 发布侧"]
        OutboxPublisher["Outbox Publisher<br/>cmd/outbox-publisher"]
        MatchTopic[("Kafka: match-requests")]
    end

    subgraph MatchPlane["匹配侧"]
        MatcherWorker["Matcher Worker<br/>cmd/matcher-worker"]
        MatchIndex["内存候选索引<br/>TopK riders"]
        MatchTx["PG 事务<br/>Inbox + Reserve Rider + Orders + Result Outbox + Checkpoint"]
    end

    Postgres[("PostgreSQL")]

    OrderTopic --> OrderWorker
    OrderWorker --> OrderTx
    OrderTx --> Postgres
    Postgres --> OutboxPublisher
    OutboxPublisher -->|"match_requested"| MatchTopic
    MatchTopic --> MatcherWorker
    MatcherWorker --> MatchIndex
    MatcherWorker --> MatchTx
    MatchTx --> Postgres
    OutboxPublisher -->|"order_matched / order_missed"| OrderTopic

    OrderWorker -->|"心跳、读取 ownership"| Etcd
    MatcherWorker -->|"心跳、读取 ownership"| Etcd
```

1. 首先`cmd/producer` 生成 `order_created` 事件并批量写入 Kafka；测试时也可以由 `cmd/retry-producer` 为 `missed` 订单写入 `order_retry_request`。
2. 控制器`cmd/controller` 竞选leader，读取etcd中用来观测节点存活状态的membership，并维护shard到node的所属权ownership。
3. `cmd/node` 和 `cmd/matcher-worker` 使用同一个 logical `node-id` 定期写入心跳，读取自己持有的shard ownership。
4. Order Worker在PostgreSQL事务中保存订单、Inbox、`match_requested` Outbox 和 checkpoint。
5. Outbox Publisher 将请求发布到 `match-requests`实现消息队列到数据库的稳定链接，Matcher Worker在内存索引筛选 TopK，基本保留原先单机模型算法，但是目前还在设计怎么高效访问其他节点骑手的算法，此功能在开发中实现过但是对性能影响过大被砍掉了。
6. Matcher Worker在事务中预约骑手、更新订单并写入 `matched/missed` Outbox。
7. 结果经 `order-events` 回到 Order Worker；ownership epoch 变化时旧worker被fencing拒绝。

## 当前能力

- 使用Kafka作为订单事件日志。
- 使用etcd保存节点存活状态membership、分片所属权shard ownership和控制器的leader选举controller election。
- 使用PostgreSQL保存订单、骑手、分消费者 Inbox/Checkpoint 和定向 Outbox。
- `cmd/node`和`cmd/matcher-worker`按当前ownership动态消费自己负责的 shard，目前还在优化批处理流程，基本能跑但是性能有待提升。
- controller通过leader election保证同一时间只有一个active controller向etcd发起ownership维护。
- logical node心跳失效后，controller会把dead node的shard迁移给存活节点。
- ownership带epoch，Order Worker和Matcher Worker都会做fencing校验，避免旧owner在恢复后继续写入过期状态。
- checkpoint在事件成功apply后推进，可能会有重放动作不过能保持最终一致性，因为：
  - PostgreSQL模式下，inbox、order state、outbox 和 checkpoint 在同一个本地事务中提交；重复的 event ID 不会再次修改订单状态。
  - `outbox` 匹配模式通过 `match-requests` topic 和独立 Matcher Worker 完成匹配，最终骑手占用由PostgreSQL条件更新裁决。
- Producer支持批量写Kafka，Outbox Publisher支持批量领取、按 topic 批量发布和批量标记完成，初步完成一部分批处理，还有订单的批处理还在优化中。
- `retry-producer` 可以按订单范围和 attempt 为当前 `missed` 订单批量发送重试事件。
- 暴露 Prometheus 指标，并提供 Grafana dashboard。

## 目录结构

```text
cmd/
  controller/       controller CLI，主要负责参数解析
  matcher-worker/   消费匹配请求并事务性预约骑手
  node/             node CLI，启动心跳、runner 和 metrics
  outbox-publisher/ 将 PostgreSQL Outbox 事件发布到 Kafka
  producer/         producer CLI，向 Kafka 写入订单事件
  retry-producer/   测试用 retry CLI，为 missed 订单写入重试事件
internal/
  applier/          event -> order state / outbox / checkpoint 的应用逻辑
  checkpoint/       shard checkpoint 存储
  cluster/          election、membership、ownership、failover 等集群协调逻辑
  controllerapp/    controller 运行时组装
  eventlog/         Kafka event log 与事件 codec
  matcher/          骑手匹配与空间索引
  matching/         不带队列和持久化副作用的候选索引
  matchworker/      Matcher Worker 的事务处理逻辑
  metrics/          Prometheus 指标
  node/             runner 与 shard 消费逻辑
  nodeapp/          node 运行时组装
  orderstate/       order state 存储
  producerapp/      producer 运行时组装
  shard/            shard layout
  tools/            测试与工具函数
docs/               设计与说明文档
observability/      Prometheus、Grafana、告警规则
scripts/            本地 e2e、容量压测与 Kafka 辅助脚本
```

## 环境要求

- Go 1.25+
- Docker Desktop
- PowerShell

本地分布式链路默认依赖 Docker Compose 启动 Kafka、etcd、PostgreSQL、Prometheus 和 Grafana。

## 快速运行

启动中间件和观测组件：

```powershell
docker compose up -d
```

创建 Kafka topic：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\kafka_create_topics.ps1 -Partitions 64
```

启动一个 node：

```powershell
go run ./cmd/node -node-id 1 -metrics-addr :9101
```

启动 controller：

```powershell
go run ./cmd/controller -metrics-addr :9102
```

启动 Matcher Worker 和 Outbox Publisher：

```powershell
go run ./cmd/matcher-worker -node-id 1
go run ./cmd/outbox-publisher -worker-id publisher-1
```

写入一批订单事件：

```powershell
go run ./cmd/producer -orders 100 -batch-size 100 -metrics-addr :9103
```

如果需要验证多轮重试，可以在订单进入 `missed` 后执行：

```powershell
go run ./cmd/retry-producer -start-id 1 -end-id 100 -attempt 1 -batch-size 100
```

`retry-producer` 只会选择范围内当前为 `missed` 且 attempt 小于目标轮次的订单。事件 ID 使用 `order-{id}-retry-{attempt}`，因此重复执行相同轮次时可以由 Inbox 去重。

默认地址：

```text
Kafka:      127.0.0.1:9092
etcd:       127.0.0.1:2379
PostgreSQL: postgres://testp:testp@127.0.0.1:5432/testp?sslmode=disable
etcd prefix /testp
topics:     order-events, rider-events, match-requests
```

数据库表会在 PostgreSQL 数据卷首次创建时，由 `database/migrations` 下的 SQL 自动初始化。修改 schema 或 query 后要重新生成访问代码：

```powershell
go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate
```

当前主链路固定为 `order-events -> PostgreSQL outbox -> match-requests -> Matcher Worker`。`cmd/node` 不再支持 direct Engine 路径，旧 Engine 代码、测试和相关 benchmark 已移除。

## 设计文档

- [系统设计总览](docs/design.md)
- [设计目标](docs/design.md#1-设计目标)
- [核心对象与不变量](docs/design.md#2-核心对象与不变量)
- [实现思路](docs/design.md#3-实现思路)

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

先启动两个 Order Worker，注意第二个 worker 使用不同的 metrics 端口：

```powershell
go run ./cmd/node -node-id 1 -metrics-addr :9101
go run ./cmd/node -node-id 2 -metrics-addr :9104
```

再启动 controller：

```powershell
go run ./cmd/controller -metrics-addr :9102
```

如果只演示 Order Worker 的 failover，停止 `node-id=1` 的 `cmd/node` 后，等待 membership TTL 和 controller sweep 生效。controller 会识别 logical node 1 已失效，并把它持有的 shard 分配给仍然 alive 的 node。

如果同时启动了 `cmd/matcher-worker -node-id 1`，它也会为同一个 logical node 续租 membership；这时要演示完整 logical node 故障，需要同时停止 `node-id=1` 下的 Order Worker 和 Matcher Worker。

如果想用脚本跑一轮本地验证，可以参考：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\e2e_outbox_matching.ps1
```

## 常用参数

`cmd/node`：

```text
-node-id              node id，默认 1
-data-dir             本地数据目录，默认 ./data
-workers              worker 数量，默认 2
-heartbeat-interval   心跳间隔，默认 1s
-membership-ttl       membership TTL，默认 5s
-metrics-interval     控制台指标打印间隔，默认 5s
-metrics-addr         Prometheus 地址，默认 :9101
-etcd-endpoints       etcd 地址，默认 127.0.0.1:2379
-etcd-prefix          etcd key 前缀，默认 /testp
-kafka-brokers        Kafka broker，默认 127.0.0.1:9092
-kafka-topic          Kafka topic，默认 order-events
-postgres-url         PostgreSQL 连接地址，默认使用本地 testp 数据库
```

`cmd/matcher-worker` 消费 `match-requests`，主要参数为 `-node-id`、`-riders`、`-refresh-interval`、`-match-topic`、`-postgres-url`、`-candidate-limit` 和 `-max-rider-orders`。`cmd/outbox-publisher` 通过 `-worker-id`、`-batch-size`、`-poll-interval`、`-order-topic` 和 `-match-topic` 控制 Outbox 发布。

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
-batch-size           每次批量写入 Kafka 的事件数，默认 100
-start-id             起始订单 ID，默认 1
-seed                 随机种子，默认 1
-metrics-addr         Prometheus 地址，默认 :9103
-kafka-brokers        Kafka broker，默认 127.0.0.1:9092
-kafka-topic          Kafka topic，默认 order-events
```

`cmd/retry-producer`：

```text
-start-id             起始订单 ID
-end-id               结束订单 ID
-attempt              目标重试轮次，从 1 开始
-reason               重试原因，默认 benchmark_retry
-batch-size           每次批量写入 Kafka 的事件数，默认 100
-postgres-url         PostgreSQL 连接地址
-kafka-brokers        Kafka broker，默认 127.0.0.1:9092
-kafka-topic          Kafka topic，默认 order-events
```

## 压测

运行单组端到端压测：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bench_outbox_matching.ps1 `
  -Riders 100 `
  -Orders 10000 `
  -ProducerBatchSize 1000 `
  -PublisherBatchSize 1000 `
  -PublisherCount 2 `
  -ResultCsv .\benchmark-results\capacity-100-10000.csv
```

运行预设的三档容量测试：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bench_capacity_scenarios.ps1
```

脚本预设场景为 100 骑手/1 万订单、1000 骑手/10 万订单和 1 万骑手/100 万订单。结果会写入 `benchmark-results/capacity-scenarios.csv`，每档的轮询采样保存在脚本输出的临时目录中。

当前已完成的本地结果：

| 骑手 | 订单 | Matched | Missed | 耗时 | 吞吐 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 10,000 | 300 | 9,700 | 22.38s | 446.82 单/s |
| 1,000 | 100,000 | 3,000 | 97,000 | 226.55s | 441.41 单/s |

默认每名骑手最多持有 3 个活跃订单，因此两档 matched 数量分别等于 `100 × 3` 和 `1000 × 3`。这两档场景验证的是容量耗尽后的过载处理，不代表系统目标匹配率只有 3%。压测中的 P50/P95/P99 来自数据库终态轮询，是近似端到端延迟，不是逐订单 tracing 延迟。

## 测试

运行全部测试：

```powershell
go test ./...
```

Docker PostgreSQL 集成测试：

```powershell
$env:TEST_DATABASE_URL='postgres://testp:testp@127.0.0.1:5432/testp?sslmode=disable'
go test ./internal/applier ./internal/matchworker ./internal/outbox -count=1
```

校验 Prometheus 配置和告警规则：

```powershell
docker run --rm --entrypoint promtool -v "${PWD}\observability:/etc/prometheus:ro" prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml
docker run --rm --entrypoint promtool -v "${PWD}\observability:/etc/prometheus:ro" prom/prometheus:v2.55.1 check rules /etc/prometheus/alerts.yml
```
