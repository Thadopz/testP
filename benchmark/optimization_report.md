# Benchmark Optimization Report

本文记录本项目在订单匹配与 batch 分发路径上的几轮优化，以及每轮 benchmark 对比结果。

## 测试环境

本地 micro benchmark 环境：

```text
OS: Windows
CPU: 13th Gen Intel(R) Core(TM) i9-13980HX
Go benchmark CPU suffix: -32
```

服务器满压 benchmark 环境：

```text
Azure Linux VM
CPU: 2 vCPU, Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
Memory: 约 3.8 GiB
workers: 2
profile: default
engine: both
```

## 版本说明

| 版本 | 主要变化 | 目标 |
| --- | --- | --- |
| V0 | 原始实现 | 基线 |
| V1 | 匹配时直接扫描 cell 并维护 best rider | 去掉 `[]RiderCandidate` 中间数组 |
| V2 | batch 分组前先统计 shard 数量并精确分配容量 | 减少分片时 `append` 扩容 |
| V3 | shard channel payload 改为原始订单 slice + 订单下标 | 避免把 `model.Order` 复制到每个 shard batch |

## V1: 直接扫描并计算最佳 Rider

原始匹配路径：

```text
扫描 grid cell -> 构造 []RiderCandidate -> 遍历 candidates 选择 best rider
```

优化后路径：

```text
扫描 grid cell -> 边扫描边计算 score -> 直接维护 best rider
```

关键收益是避免每个订单匹配时分配候选数组。

### Micro Benchmark

命令：

```bash
go test ./internal/engine ./internal/matcher -run '^$' -bench 'Benchmark(ShardedMatchOne|ShardedFindCandidatesHomeShard|ShardedCollectCandidatesNeighborShards|GridFindNearbyCandidatesRadius1|GridFindNearbyCandidatesRadius3|GridFindNearbyCandidatesRadius8)$' -benchmem -count=3
```

| 指标 | V0 | V1 | 提升 |
| --- | ---: | ---: | ---: |
| `BenchmarkShardedMatchOne` 平均耗时 | 约 7410 ns/op | 约 1720 ns/op | 约 4.31x |
| `BenchmarkShardedMatchOne` 内存 | 16941 B/op | 0 B/op | 100% 减少 |
| `BenchmarkShardedMatchOne` 分配 | 9 allocs/op | 0 allocs/op | 100% 减少 |

说明：`GridFindNearbyCandidates...` 仍然较重，因为它测试的是保留下来的旧候选数组 API。默认匹配主路径已经不再走这个 API，除非启用 `topK > 0`。

### 服务器满压 Benchmark

命令：

```bash
../bin/testP_benchmark -workers 2 -profile default -engine both
```

| 场景 | 引擎 | 模式 | V0 耗时 | V1 耗时 | V0 吞吐 | V1 吞吐 | 提升 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 100r_1w_orders | global | fixed | 60.37032ms | 15.177659ms | 165644.31 | 658863.14 | 约 3.98x |
| 100r_1w_orders | sharded | fixed | 22.745993ms | 5.198577ms | 439637.87 | 1923603.32 | 约 4.38x |
| 1000r_10w_orders | global | fixed | 932.581255ms | 243.654364ms | 107229.26 | 410417.44 | 约 3.83x |
| 1000r_10w_orders | sharded | fixed | 221.087545ms | 52.235623ms | 452309.51 | 1914402.36 | 约 4.23x |
| 1w_r_100w_orders | global | fixed | 9.488863916s | 2.806201009s | 105386.69 | 356353.66 | 约 3.38x |
| 1w_r_100w_orders | sharded | fixed | 6.920430492s | 1.804420787s | 144499.68 | 554194.46 | 约 3.84x |

整轮结果：

| 版本 | wall time | max RSS |
| --- | ---: | ---: |
| V0 | 35.42s | 104492 KB |
| V1 | 9.92s | 58648 KB |

## V2: Batch 分组精确容量

原始分组方式：

```go
grouped := make([][]model.Order, shardCount)
for _, order := range batch.Orders {
    shardID := layout.ShardID(order.X, order.Y)
    grouped[shardID] = append(grouped[shardID], order)
}
```

优化后先统计每个 shard 的订单数量，再按精确容量创建 slice：

```go
counts := make([]int, shardCount)
for _, order := range batch.Orders {
    shardID := layout.ShardID(order.X, order.Y)
    counts[shardID]++
}

grouped := make([][]model.Order, shardCount)
for shardID, count := range counts {
    if count > 0 {
        grouped[shardID] = make([]model.Order, 0, count)
    }
}
```

### Micro Benchmark

命令：

```bash
go test ./internal/engine ./internal/scheduler -run '^$' -bench 'Benchmark(ShardedSubmitBatchRouting|SchedulerDispatchBatch)$' -benchmem -count=5
```

| Benchmark | V1 | V2 | 变化 |
| --- | ---: | ---: | ---: |
| `ShardedSubmitBatchRouting` 平均耗时 | 约 58.7 us/op | 约 45.6 us/op | 约 22% 更快 |
| `ShardedSubmitBatchRouting` 内存 | 72256 B/op | 27760 B/op | 约 62% 减少 |
| `ShardedSubmitBatchRouting` 分配 | 346 allocs/op | 66 allocs/op | 约 81% 减少 |
| `SchedulerDispatchBatch` 平均耗时 | 约 68.5 us/op | 约 44.9 us/op | 约 34% 更快 |
| `SchedulerDispatchBatch` 内存 | 77632 B/op | 27568 B/op | 约 64% 减少 |
| `SchedulerDispatchBatch` 分配 | 352 allocs/op | 66 allocs/op | 约 81% 减少 |

### 端到端 Benchmark

命令：

```bash
go run ./benchmark -workers 32 -profile default -engine sharded
```

| 场景 | 模式 | V2 耗时 | V2 吞吐 |
| --- | --- | ---: | ---: |
| 100r_1w_orders | fixed | 1.0743ms | 9308386.86 |
| 100r_1w_orders | dynamic | 1.2298ms | 8131403.48 |
| 1000r_10w_orders | fixed | 10.4408ms | 9577810.13 |
| 1000r_10w_orders | dynamic | 12.594ms | 7940289.03 |
| 1w_r_100w_orders | fixed | 249.5401ms | 4007371.96 |
| 1w_r_100w_orders | dynamic | 247.0031ms | 4048532.18 |

## V3: Payload 改为订单下标

V2 仍然会把订单复制到每个 shard 的 `[]model.Order` 中。V3 将 shard channel payload 改成：

```go
type ShardOrderBatch struct {
    Orders  []Order
    Indexes []int
}
```

分片时只保存订单下标：

```go
grouped[shardID] = append(grouped[shardID], orderIndex)
```

worker 处理时通过下标访问原始订单：

```go
for _, orderIndex := range batch.Indexes {
    MatchOne(&batch.Orders[orderIndex])
}
```

### Micro Benchmark

命令：

```bash
go test ./internal/engine ./internal/scheduler -run '^$' -bench 'Benchmark(ShardedSubmitBatchRouting|SchedulerDispatchBatch)$' -benchmem -count=5
```

| Benchmark | V1 原始分组 | V2 精确容量 | V3 index payload |
| --- | ---: | ---: | ---: |
| `ShardedSubmitBatchRouting` 内存 | 72256 B/op | 27760 B/op | 10760 B/op |
| `ShardedSubmitBatchRouting` 分配 | 346 allocs/op | 66 allocs/op | 66 allocs/op |
| `SchedulerDispatchBatch` 内存 | 77632 B/op | 27568 B/op | 10736 B/op |
| `SchedulerDispatchBatch` 分配 | 352 allocs/op | 66 allocs/op | 66 allocs/op |

V3 相比 V2 的主要收益：

| Benchmark | V2 内存 | V3 内存 | 变化 |
| --- | ---: | ---: | ---: |
| `ShardedSubmitBatchRouting` | 27760 B/op | 10760 B/op | 约 61% 减少 |
| `SchedulerDispatchBatch` | 27568 B/op | 10736 B/op | 约 61% 减少 |

分配次数仍为 66 allocs/op，因为每个活跃 shard 仍会分配一个 `[]int` 下标 slice。继续降低分配次数需要复用这些 index slice，或让订单在生成阶段直接按 shard 分组。

### 端到端 Benchmark

命令：

```bash
go run ./benchmark -workers 32 -profile default -engine sharded
```

| 场景 | 模式 | V3 耗时 | V3 吞吐 |
| --- | --- | ---: | ---: |
| 100r_1w_orders | fixed | 1.1881ms | 8416799.93 |
| 100r_1w_orders | dynamic | 1.0908ms | 9167583.43 |
| 1000r_10w_orders | fixed | 9.8653ms | 10136539.18 |
| 1000r_10w_orders | dynamic | 9.9427ms | 10057630.22 |
| 1w_r_100w_orders | fixed | 223.9102ms | 4466076.13 |
| 1w_r_100w_orders | dynamic | 240.0114ms | 4166468.76 |

