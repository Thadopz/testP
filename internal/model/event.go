package model

type EventType string

const (
	EventOrderCreated      EventType = "order_created"
	EventOrderCancelled    EventType = "order_cancelled"
	EventOrderRetryRequest EventType = "order_retry_request"
	EventOrderMatched      EventType = "order_matched"
	EventOrderMissed       EventType = "order_missed"

	EventRiderOnline  EventType = "rider_online"
	EventRiderOffline EventType = "rider_offline"
	EventRiderMoved   EventType = "rider_moved"
)

type Event struct {
	ID            string
	Type          EventType
	AggregateType string
	AggregateID   string
	ShardID       int
	OccurredAt    int64 //Unix时间
	Payload       []byte
}
