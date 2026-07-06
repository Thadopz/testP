package model

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
