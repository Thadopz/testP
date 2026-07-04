# Benchmark Optimization Report

本文记录本项目在订单匹配与 batch 分发路径上的几轮优化，以及每轮 benchmark 对比结果。

## 测试环境

本地 micro benchmark 环境：

```text
OS: Windows
CPU: 13th Gen Intel(R) Core(TM) i9-13980HX
Go benchmark CPU suffix: -2
Go benchmark flag: -cpu=2
```

端到端 benchmark 环境：

```text
OS: Windows
CPU: 13th Gen Intel(R) Core(TM) i9-13980HX
workers: 2
profile: default
engine: sharded
```

## 版本说明

| 版本 | 主要变化 | 目标 |
| --- | --- | --- |
| V0 | 原始实现 | 基线 |
| V1 | 匹配时直接扫描 cell 并维护 best rider | 去掉 `[]RiderCandidate` 中间数组 |
| V2 | batch 分组前先统计 shard 数量并精确分配容量 | 减少分片时 `append` 扩容 |
| V3 | shard channel payload 改为原始订单 slice + 订单下标 | 避免把 `model.Order` 复制到每个 shard batch |

说明：早期版本同时保留过 global 和 sharded 两条路径，表格中的 global 数据仅作为历史对照。当前代码已经收敛为 sharded-only。

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

说明：`GridFindNearbyCandidates...` 仍然较重，因为它测试的是保留下来的旧候选数组 API。默认匹配主路径已经不再走这个 API。

### 服务器满压 Benchmark

命令：

```bash
../bin/testP_benchmark -workers 2 -profile default
```

| 场景 | 模式 | V0 sharded 耗时 | V1 sharded 耗时 | V0 吞吐 | V1 吞吐 | 提升 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 100r_1w_orders | fixed | 22.745993ms | 5.198577ms | 439637.87 | 1923603.32 | 约 4.38x |
| 1000r_10w_orders | fixed | 221.087545ms | 52.235623ms | 452309.51 | 1914402.36 | 约 4.23x |
| 1w_r_100w_orders | fixed | 6.920430492s | 1.804420787s | 144499.68 | 554194.46 | 约 3.84x |

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
go test ./internal/engine -run '^$' -bench 'BenchmarkShardedSubmitBatchRouting$' -benchmem -cpu=2 -count=5
```

| Benchmark | V1 | V2 | 变化 |
| --- | ---: | ---: | ---: |
| `ShardedSubmitBatchRouting` 平均耗时 | 约 53.8 us/op | 约 44.1 us/op | 约 18% 更快 |
| `ShardedSubmitBatchRouting` 内存 | 72256 B/op | 27760 B/op | 约 62% 减少 |
| `ShardedSubmitBatchRouting` 分配 | 346 allocs/op | 66 allocs/op | 约 81% 减少 |

### 端到端 Benchmark

命令：

```bash
go run ./benchmark -workers 2 -profile default
```

| 场景 | 模式 | V1 耗时 | V2 耗时 | V1 吞吐 | V2 吞吐 |
| --- | --- | ---: | ---: | ---: | ---: |
| 100r_1w_orders | fixed | 1.5725ms | 1.7466ms | 6359300.48 | 5725409.37 |
| 100r_1w_orders | dynamic | 1.5561ms | 2.2282ms | 6426322.22 | 4487927.48 |
| 1000r_10w_orders | fixed | 24.7496ms | 21.7632ms | 4040469.34 | 4594912.51 |
| 1000r_10w_orders | dynamic | 25.2269ms | 18.8755ms | 3964022.53 | 5297872.90 |
| 1w_r_100w_orders | fixed | 704.4116ms | 666.2066ms | 1419624.55 | 1501035.86 |
| 1w_r_100w_orders | dynamic | 730.4268ms | 693.5949ms | 1369062.58 | 1441763.77 |

V2 对中、大场景更稳定，小场景耗时受调度噪声影响较大。

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
go test ./internal/engine -run '^$' -bench 'BenchmarkShardedSubmitBatchRouting$' -benchmem -cpu=2 -count=5
```

| Benchmark | V1 原始分组 | V2 精确容量 | V3 index payload |
| --- | ---: | ---: | ---: |
| `ShardedSubmitBatchRouting` 平均耗时 | 约 53.8 us/op | 约 44.1 us/op | 约 37.5 us/op |
| `ShardedSubmitBatchRouting` 内存 | 72256 B/op | 27760 B/op | 10760 B/op |
| `ShardedSubmitBatchRouting` 分配 | 346 allocs/op | 66 allocs/op | 66 allocs/op |

V3 相比 V2 的主要收益：

| Benchmark | V2 内存 | V3 内存 | 变化 |
| --- | ---: | ---: | ---: |
| `ShardedSubmitBatchRouting` | 27760 B/op | 10760 B/op | 约 61% 减少 |

| Benchmark | V2 平均耗时 | V3 平均耗时 | 变化 |
| --- | ---: | ---: | ---: |
| `ShardedSubmitBatchRouting` | 约 44.1 us/op | 约 37.5 us/op | 约 15% 更快 |

分配次数仍为 66 allocs/op，因为每个活跃 shard 仍会分配一个 `[]int` 下标 slice。继续降低分配次数需要复用这些 index slice，或让订单在生成阶段直接按 shard 分组。

### 端到端 Benchmark

命令：

```bash
go run ./benchmark -workers 2 -profile default
```

| 场景 | 模式 | V1 耗时 | V2 耗时 | V3 耗时 | V1 吞吐 | V2 吞吐 | V3 吞吐 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100r_1w_orders | fixed | 1.5725ms | 1.7466ms | 2.8171ms | 6359300.48 | 5725409.37 | 3549749.74 |
| 100r_1w_orders | dynamic | 1.5561ms | 2.2282ms | 2.1204ms | 6426322.22 | 4487927.48 | 4716091.30 |
| 1000r_10w_orders | fixed | 24.7496ms | 21.7632ms | 26.5885ms | 4040469.34 | 4594912.51 | 3761024.50 |
| 1000r_10w_orders | dynamic | 25.2269ms | 18.8755ms | 24.4432ms | 3964022.53 | 5297872.90 | 4091117.37 |
| 1w_r_100w_orders | fixed | 704.4116ms | 666.2066ms | 700.4558ms | 1419624.55 | 1501035.86 | 1427641.83 |
| 1w_r_100w_orders | dynamic | 730.4268ms | 693.5949ms | 675.8357ms | 1369062.58 | 1441763.77 | 1479649.57 |

V3 在 batch routing 的 micro benchmark 中继续降低内存和耗时，但端到端收益并不稳定。原因是 V3 用 index 访问原始订单，减少了订单复制，但也引入了一层间接访问，并拉长了原始 batch 的生命周期。在 `workers=2` 下，V2 对 fixed 场景更稳，V3 对 dynamic 大场景略好。
