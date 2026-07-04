# Program Design Test: Order Matching Benchmark

这是一个用 Go 编写的订单与骑手匹配模拟项目，用于验证在不同骑手规模、订单规模和运行时长约束下，系统能否持续生成订单并完成分配。

项目采用批量流式模拟的方式：订单分批生成，生成后立即进入调度和匹配流程，同时支持骑手移动、上线、下线等动态事件，初步地模拟骑手行为。经过了七次优化，在2核4GB的云主机上，对于最慢的场景（一万骑手对应 100 万订单）的时间从大于40秒降低至约1.8秒，其中有0.7秒用于尽快地生成批量订单（大约55w单每秒），实际处理时间约1.1秒。

## 目录结构

```text
.
├── main.go                         # 长期实际运行入口
├── benchmark/main.go               # 多场景 benchmark 入口
├── internal/engine                 # sharded 引擎实现
├── internal/matcher                # 网格索引与骑手匹配逻辑
├── internal/model                  # 订单、骑手、事件模型
└── internal/shard                  # 分片布局
```

## 环境要求

```text
Go 1.25.3+
```

本项目无第三方依赖。

## 运行测试

```bash
go test ./...
```

运行标准 Go benchmark：

```bash
go test ./internal/... -run '^$' -bench . -benchmem
```


## 实际运行

默认运行会持续生成订单和骑手事件，直到手动停止：

```bash
go run .
```

运行 60 秒：

```bash
go run . -workers 2 -run-for 60s
```

常用参数：

```text
-riders     初始骑手数，默认 100
-workers    worker 数，默认 2
-run-for    运行时长，0s 表示一直运行，默认0s
-seed       随机种子，默认 1
```

注意：`main.go` 中的订单生成有随机等待：

```text
每批订单数：1-50
每批等待：10ms-50ms
```

因此默认 60 秒运行更像真实流量模拟，不是机器极限压测。默认配置下理论生成速率约为 850 orders/s。

## Benchmark 场景

运行项目级 benchmark：

```bash
go run ./benchmark -workers 2 -profile default
```

可选 profile：

```text
default    跑 100/1000/10000 骑手规模的默认场景
examples   跑带目标时长的示例场景
full       额外包含 10 万骑手、1000 万订单的大规模场景
```

查看目标场景示例：

```bash
go run ./benchmark -show-examples
```

示例目标场景的语义是多组独立测试：

```text
1 分钟内：1000 个骑手，对应 10 万订单
3 分钟内：10000 个骑手，对应 100 万订单
10 分钟内：100000 个骑手，对应 1000 万订单
```

每个场景内部为流水线式的批量模拟。

带目标时长的场景会按目标吞吐匀速投递订单；没有目标时长的 default/full 场景仍按满压方式尽快提交订单。

## 结果指标

benchmark 输出字段：

```text
scenario           场景名称
mode               fixed 或 dynamic，dynamic 会插入骑手事件
riders             初始骑手数
online_riders      结束时在线骑手数
orders             订单数
matched            成功匹配订单数
missed             未匹配订单数
submit_elapsed     订单生成、分批提交和动态骑手事件注入耗时
drain_elapsed      停止提交后等待 worker 处理完队列的耗时
elapsed            总耗时
throughput         吞吐，orders/s
target_elapsed     目标耗时，仅 examples profile 有值
target_throughput  目标吞吐，仅 examples profile 有值
within_target      是否在目标耗时内完成
```

## 2 核 4G 实测数据

测试环境：

```text
云主机：Azure Linux VM
CPU：2 vCPU，Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
内存：约 3.8 GiB
系统：Ubuntu 22.04 Azure kernel
worker：2
```

### 项目级 Benchmark

命令：

```bash
../bin/testP_benchmark -workers 2 -profile default
```

结果：

```text
workers=2 profile=default batch=5000 events_per_batch=3 engine=sharded
wall_time=0:03.73 max_rss_kb=50760
```

| 场景 | 模式 | 骑手数 | 在线骑手 | 订单数 | 匹配数 | miss | 提交耗时 | 排空耗时 | 总耗时 | 吞吐 orders/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100r_1w_orders | fixed | 100 | 100 | 10000 | 10000 | 0 | 966.211µs | 3.932074ms | 4.898423ms | 2041473.35 |
| 100r_1w_orders | dynamic | 100 | 98 | 10000 | 10000 | 0 | 807.055µs | 3.735201ms | 4.542399ms | 2201479.88 |
| 1000r_10w_orders | fixed | 1000 | 1000 | 100000 | 100000 | 0 | 9.449717ms | 40.961777ms | 50.411715ms | 1983665.90 |
| 1000r_10w_orders | dynamic | 1000 | 995 | 100000 | 100000 | 0 | 8.465909ms | 42.222063ms | 50.688221ms | 1972844.93 |
| 1w_r_100w_orders | fixed | 10000 | 10000 | 1000000 | 1000000 | 0 | 696.956045ms | 1.104413728s | 1.801369981s | 555133.04 |
| 1w_r_100w_orders | dynamic | 10000 | 9998 | 1000000 | 1000000 | 0 | 701.15445ms | 1.111083887s | 1.812238527s | 551803.74 |

### 标准 Go Benchmark

命令：

```bash
../bin/engine.test -test.run='^$' -test.bench=. -test.benchtime=1s -test.cpu=2
../bin/matcher.test -test.run='^$' -test.bench=. -test.benchtime=1s -test.cpu=2
../bin/shard.test -test.run='^$' -test.bench=. -test.benchtime=1s -test.cpu=2
```

结果：

```text
== engine ==
goos: linux
goarch: amd64
pkg: testP/internal/engine
cpu: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
BenchmarkNewShardedEngine-2                         	     375	   3144095 ns/op	 2845648 B/op	    5517 allocs/op
BenchmarkShardedSubmitBatchRouting-2                	   29037	     41195 ns/op	   10760 B/op	      66 allocs/op
BenchmarkShardedFindCandidatesHomeShard-2           	  120988	      9864 ns/op	   16941 B/op	       9 allocs/op
BenchmarkShardedCollectCandidatesNeighborShards-2   	   15620	     76844 ns/op	  125920 B/op	      30 allocs/op
BenchmarkShardedMatchOne-2                          	  623016	      1903 ns/op	       0 B/op	       0 allocs/op
BenchmarkShardedApplyRiderMoveSameShard-2           	18259306	        65.62 ns/op	       0 B/op	       0 allocs/op
BenchmarkShardedApplyRiderMoveCrossShard-2          	 6890396	       173.6 ns/op	       0 B/op	       0 allocs/op
PASS
== matcher ==
goos: linux
goarch: amd64
pkg: testP/internal/matcher
cpu: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
BenchmarkGridFindNearbyCandidatesRadius1-2     	   90231	     13317 ns/op	   21746 B/op	       8 allocs/op
BenchmarkGridFindNearbyCandidatesRadius3-2     	   22497	     53796 ns/op	   81001 B/op	      10 allocs/op
BenchmarkGridFindNearbyCandidatesRadius8-2     	    3169	    396131 ns/op	  586359 B/op	      15 allocs/op
BenchmarkGridFindNearbyCandidatesRange0To1-2   	   88975	     13329 ns/op	   21745 B/op	       8 allocs/op
BenchmarkGridFindNearbyCandidatesRange2To3-2   	   25101	     47475 ns/op	   74509 B/op	      10 allocs/op
BenchmarkGridFindNearbyCandidatesRange4To8-2   	    4196	    287475 ns/op	  432840 B/op	      14 allocs/op
BenchmarkGridMoveRider-2                       	11253118	       105.9 ns/op	       0 B/op	       0 allocs/op
PASS
== shard ==
goos: linux
goarch: amd64
pkg: testP/internal/shard
cpu: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
BenchmarkShardLayoutShardID-2            	99360358	        11.80 ns/op	       0 B/op	       0 allocs/op
BenchmarkShardLayoutNeighborShardIDs-2   	25573957	        46.09 ns/op	      80 B/op	       1 allocs/op
PASS
```

### 60 秒实际运行

命令：

```bash
../bin/testP_app -workers 2 -run-for 60s
```

结果：

```text
riders: 100
online_riders: 97
orders: 50392
matched: 50392
missed: 0
workers: 2
shards: 64
shard_layout: 8x8
cell_size: 44721
elapsed: 1m0.000011031s
throughput: 839.87 orders/s
bottom riders:
uid=55 count=316
uid=85 count=410
uid=2 count=450
uid=5 count=450
uid=12 count=450
uid=18 count=450
uid=24 count=450
uid=25 count=450
uid=29 count=450
uid=39 count=450
wall_time=1:00.00 max_rss_kb=8788
```
