package model

type Order struct {
	ID int64
	X  int
	Y  int
}

type OrderBatch struct {
	Orders []Order
}
