# Program Design Test: Order Matching Benchmark

这是一个用 Go 编写的订单与骑手匹配模拟项目，用于验证在不同骑手规模、订单规模和运行时长约束下，系统能否持续生成订单并完成分配。

项目采用批量流式模拟的方式：订单分批生成，生成后立即进入调度和匹配流程，同时支持骑手移动、上线、下线等动态事件，初步地模拟骑手行为。

## 目录结构

```text
.
├── main.go                         # 长期实际运行入口
├── benchmark/main.go               # 多场景 benchmark 入口
├── internal/engine                 # global/sharded 两种引擎实现
├── internal/matcher                # 网格索引与骑手匹配逻辑
├── internal/model                  # 订单、骑手、事件模型
└── internal/scheduler              # 分片调度与 worker pool
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
go run ./benchmark -workers 2 -profile default -engine both
```

可选 profile：

```text
default    跑 100/1000/10000 骑手规模的默认场景
examples   跑带目标时长的示例场景
full       额外包含 10 万骑手、1000 万订单的大规模场景
```

可选 engine：

```text
global     全局 matcher
sharded    分片 matcher
both       同时跑 global 和 sharded
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

当前 `benchmark/main.go` 的实现会按 batch 生成订单并立即提交给引擎处理，但会尽快生成完整个订单量；如果要更严格模拟“在 1/3/10 分钟内按固定速率进入系统”，可以进一步把订单生成改成按目标吞吐匀速投递。

## 结果指标

benchmark 输出字段：

```text
scenario           场景名称
engine             global 或 sharded
mode               fixed 或 dynamic，dynamic 会插入骑手事件
riders             初始骑手数
online_riders      结束时在线骑手数
orders             订单数
matched            成功匹配订单数
missed             未匹配订单数
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
../bin/testP_benchmark -workers 2 -profile default -engine both
```

结果：

```text
workers=2 profile=default batch=5000 events_per_batch=3 engine=both top_k=0
wall_time=0:35.42 max_rss_kb=104492
```

| 场景 | 引擎 | 模式 | 骑手数 | 在线骑手 | 订单数 | 匹配数 | miss | 耗时 | 吞吐 orders/s |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100r_1w_orders | global | fixed | 100 | 100 | 10000 | 10000 | 0 | 60.37032ms | 165644.31 |
| 100r_1w_orders | global | dynamic | 100 | 98 | 10000 | 10000 | 0 | 52.739156ms | 189612.44 |
| 100r_1w_orders | sharded | fixed | 100 | 100 | 10000 | 10000 | 0 | 22.745993ms | 439637.87 |
| 100r_1w_orders | sharded | dynamic | 100 | 98 | 10000 | 10000 | 0 | 17.814431ms | 561342.66 |
| 1000r_10w_orders | global | fixed | 1000 | 1000 | 100000 | 100000 | 0 | 932.581255ms | 107229.26 |
| 1000r_10w_orders | global | dynamic | 1000 | 995 | 100000 | 100000 | 0 | 949.076866ms | 105365.54 |
| 1000r_10w_orders | sharded | fixed | 1000 | 1000 | 100000 | 100000 | 0 | 221.087545ms | 452309.51 |
| 1000r_10w_orders | sharded | dynamic | 1000 | 995 | 100000 | 100000 | 0 | 203.861023ms | 490530.26 |
| 1w_r_100w_orders | global | fixed | 10000 | 10000 | 1000000 | 1000000 | 0 | 9.488863916s | 105386.69 |
| 1w_r_100w_orders | global | dynamic | 10000 | 9998 | 1000000 | 1000000 | 0 | 9.580578343s | 104377.83 |
| 1w_r_100w_orders | sharded | fixed | 10000 | 10000 | 1000000 | 1000000 | 0 | 6.920430492s | 144499.68 |
| 1w_r_100w_orders | sharded | dynamic | 10000 | 9998 | 1000000 | 1000000 | 0 | 6.95612985s | 143758.10 |

### 标准 Go Benchmark

命令：

```bash
../bin/engine.test -test.run='^$' -test.bench=. -test.benchtime=1s -test.cpu=2
../bin/matcher.test -test.run='^$' -test.bench=. -test.benchtime=1s -test.cpu=2
../bin/scheduler.test -test.run='^$' -test.bench=. -test.benchtime=1s -test.cpu=2
```

结果：

```text
== engine ==
goos: linux
goarch: amd64
pkg: testP/internal/engine
cpu: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
BenchmarkNewShardedEngine-2                         	     381	   3189462 ns/op	 2395089 B/op	    5517 allocs/op
BenchmarkShardedSubmitBatchRouting-2                	   18632	     64096 ns/op	   72256 B/op	     346 allocs/op
BenchmarkShardedFindCandidatesHomeShard-2           	  118027	      9957 ns/op	   16941 B/op	       9 allocs/op
BenchmarkShardedCollectCandidatesNeighborShards-2   	   14731	     80840 ns/op	  125920 B/op	      30 allocs/op
BenchmarkShardedMatchOne-2                          	  110334	     10667 ns/op	   16941 B/op	       9 allocs/op
BenchmarkShardedApplyRiderMoveSameShard-2           	18155422	        65.22 ns/op	       0 B/op	       0 allocs/op
BenchmarkShardedApplyRiderMoveCrossShard-2          	 6911408	       174.1 ns/op	       0 B/op	       0 allocs/op
PASS
== matcher ==
goos: linux
goarch: amd64
pkg: testP/internal/matcher
cpu: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
BenchmarkGridFindNearbyCandidatesRadius1-2     	   89456	     13273 ns/op	   21746 B/op	       8 allocs/op
BenchmarkGridFindNearbyCandidatesRadius3-2     	   22431	     53559 ns/op	   80999 B/op	      10 allocs/op
BenchmarkGridFindNearbyCandidatesRadius8-2     	    3109	    389760 ns/op	  586755 B/op	      15 allocs/op
BenchmarkGridFindNearbyCandidatesRange0To1-2   	   90457	     13259 ns/op	   21745 B/op	       8 allocs/op
BenchmarkGridFindNearbyCandidatesRange2To3-2   	   25419	     47376 ns/op	   74521 B/op	      10 allocs/op
BenchmarkGridFindNearbyCandidatesRange4To8-2   	    4206	    286986 ns/op	  433532 B/op	      14 allocs/op
BenchmarkGridMoveRider-2                       	11287210	       105.5 ns/op	       0 B/op	       0 allocs/op
PASS
== scheduler ==
goos: linux
goarch: amd64
pkg: testP/internal/scheduler
cpu: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
BenchmarkShardLayoutShardID-2            	98660389	        11.74 ns/op	       0 B/op	       0 allocs/op
BenchmarkShardLayoutNeighborShardIDs-2   	26423282	        46.03 ns/op	      80 B/op	       1 allocs/op
BenchmarkSchedulerDispatchBatch-2        	   20059	     61512 ns/op	   77632 B/op	     352 allocs/op
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
elapsed: 1m0.00002362s
throughput: 839.87 orders/s
bottom riders:
uid=55 count=323
uid=85 count=461
uid=21 count=487
uid=1 count=506
uid=2 count=506
uid=7 count=506
uid=8 count=506
uid=10 count=506
uid=12 count=506
uid=15 count=506
wall_time=1:00.00 max_rss_kb=9036
```
