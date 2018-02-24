package model

type QMsg struct {
	Msg     *Msg
	Version int64
}

func NewQMsg(msg *Msg) *QMsg {
	return &QMsg{
		Msg:     msg,
		Version: msg.Data.Version,
	}
}
