package model

type Rider struct {
	UID    int64
	X      int
	Y      int
	OnLine bool
	CellID int
	Count  int64
}

type RiderEventType int

const (
	RiderOnline RiderEventType = iota + 1
	RiderOffline
	RiderMove
)

type RiderEvent struct {
	Type RiderEventType
	UID  int64
	X    int
	Y    int
}
