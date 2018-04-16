package model

type QMsg struct {
	TubeID  string
	MsgID   string
	Version int64
}

func NewQMsg(msg *Msg) *QMsg {
	return &QMsg{
		TubeID:  msg.TubeName,
		MsgID:   msg.ID,
		Version: msg.Data.Version,
	}
}
