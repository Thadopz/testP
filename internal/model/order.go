package model

type Order struct {
	ID int64
	X  int
	Y  int
}

type OrderBatch struct {
	Orders []Order
}

// Orders 是原始批次
// Indexes 是当前 shard 要处理的订单下标
type ShardOrderBatch struct {
	Orders  []Order
	Indexes []int
}
