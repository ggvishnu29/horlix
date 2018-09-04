package model

/*
  QMsg struct captures specific info about Msg struct.
  QMsg struct is used by DelayedQueue, ReadyQueue, ReservedQueue
  struct to maintain list of msgs that are in respective states
*/

type QMsg struct {
	MsgID   string
	Version int64
}

func NewQMsg(msg *Msg) *QMsg {
	return &QMsg{
		MsgID:   msg.ID,
		Version: msg.Data.Version,
	}
}

func (q *QMsg) Clone() *QMsg {
	return &QMsg{
		MsgID:   q.MsgID,
		Version: q.Version,
	}
}
