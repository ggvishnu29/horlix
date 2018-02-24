package model

type MsgMap struct {
	msgs map[string]*Msg
}

func NewMsgMap() *MsgMap {
	msgs := make(map[string]*Msg)
	return &MsgMap{msgs: msgs}
}

func (m *MsgMap) AddOrUpdate(msg *Msg) {
	m.msgs[msg.ID] = msg
}

func (m *MsgMap) Delete(msg *Msg) {
	m.msgs[msg.ID] = nil
}

func (m *MsgMap) Get(msgID string) *Msg {
	return m.msgs[msgID]
}
