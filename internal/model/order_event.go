package model

type OrderCreated struct {
	OrderID int64
	X       int
	Y       int
}

type OrderCancelled struct {
	OrderID int64
	Reason  string
}

type OrderMatched struct {
	OrderID int64
	RiderID int64
	Score   int
}

type OrderRetryRequest struct {
	OrderID int64
	Attempt int
	Reason  string
}
