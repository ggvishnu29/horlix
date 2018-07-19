package model

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
