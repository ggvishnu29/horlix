package model

type MsgMap struct {
	Msgs map[string]*Msg
}

func NewMsgMap() *MsgMap {
	msgs := make(map[string]*Msg)
	return &MsgMap{Msgs: msgs}
}

func (m *MsgMap) AddOrUpdate(msg *Msg) {
	m.Msgs[msg.ID] = msg
}

func (m *MsgMap) Delete(msg *Msg) {
	m.Msgs[msg.ID] = nil
}

func (m *MsgMap) Get(msgID string) *Msg {
	return m.Msgs[msgID]
}
