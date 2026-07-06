package model

type OrderCreated struct {
	OrderID int
	X       int
	Y       int
}

type OrderCancelled struct {
	OrderID int
	Reason  string
}

type OrderMatched struct {
	OrderID int
	RiderID int
	Score   int
}

type OrderRetryRequest struct {
	OrderID int
	Attempt int
	Reason  string
}
