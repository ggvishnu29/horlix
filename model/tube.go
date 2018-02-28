package model

const (
	REPLACE_DATA = iota
	APPEND_DATA
)

type Tube struct {
	ID                  string
	Lock                *Lock
	MsgMap              *MsgMap
	ReadyQueue          *ReadyQueue
	DelayedQueue        *DelayedQueue
	ReservedQueue       *ReservedQueue
	ReserveTimeoutInSec int64
	FuseSetting         *FuseSetting
	IsDeleted           bool
}

func NewTube(ID string, reserveTimeoutInSec int64, fuseSetting *FuseSetting) *Tube {
	return &Tube{
		ID:                  ID,
		Lock:                &Lock{},
		MsgMap:              NewMsgMap(),
		ReadyQueue:          &ReadyQueue{},
		DelayedQueue:        &DelayedQueue{},
		ReservedQueue:       &ReservedQueue{},
		ReserveTimeoutInSec: reserveTimeoutInSec,
		FuseSetting:         fuseSetting,
	}
}

type FuseSetting struct {
	Data int
}

func NewFuseSetting(data int) *FuseSetting {
	return &FuseSetting{
		Data: data,
	}
}
