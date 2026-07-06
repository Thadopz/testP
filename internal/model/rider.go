package model

type Rider struct {
	UID    int64
	X      int
	Y      int
	OnLine bool
	CellID int64
	Count  int64
}
