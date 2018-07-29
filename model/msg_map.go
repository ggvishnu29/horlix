package model

import (
	"github.com/ggvishnu29/horlix/serde"
)

// MsgMap struct maintains the msg map specific to a tube
type MsgMap struct {
	Msgs   map[string]*Msg // maps msgID to Msg
	TubeID string
}

func NewMsgMap(tubeID string) *MsgMap {
	msgs := make(map[string]*Msg)
	return &MsgMap{
		Msgs:   msgs,
		TubeID: tubeID,
	}
}

func (m *MsgMap) AddOrUpdate(msg *Msg) {
	m.Msgs[msg.ID] = msg
	opr := serde.NewOperation(MSG_MAP, ADD_OR_UPDATE_OPR, &m.TubeID, msg)
	LogOpr(opr)
}

func (m *MsgMap) Delete(msgID string) {
	m.Msgs[msgID] = nil
	opr := serde.NewOperation(MSG_MAP, DELETE_OPR, &m.TubeID, msgID)
	LogOpr(opr)
}

func (m *MsgMap) Get(msgID string) *Msg {
	return m.Msgs[msgID]
}
