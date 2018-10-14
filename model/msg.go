package model

import (
	"time"

	"github.com/ggvishnu29/horlix/serde"
)

type MsgState int

const (
	READY_MSG_STATE MsgState = iota
	RESERVED_MSG_STATE
	DELAYED_MSG_STATE
)

const defaultPriority int = 5
const defaultReserveTimeoutInSec = 10

type Data struct {
	DelayInSec int64
	Priority   int
	Version    int64
	DataSlice  [][]byte
}

func NewData(dataSlice [][]byte, priority int, delayInSec int64) *Data {
	return &Data{
		DelayInSec: delayInSec,
		Priority:   priority,
		Version:    time.Now().Unix(),
		DataSlice:  dataSlice,
	}
}

func (d *Data) Clone() *Data {
	if d == nil {
		return nil
	}
	return &Data{
		DelayInSec: d.DelayInSec,
		Priority:   d.Priority,
		Version:    d.Version,
		DataSlice:  CloneDataSlice(d.DataSlice),
	}
}

type MsgMetaData struct {
	State                  MsgState
	ReservedTimestamp      *time.Time
	DelayedTimestamp       *time.Time
	FirstEnqueuedTimestamp *time.Time
}

func (m *MsgMetaData) Clone() *MsgMetaData {
	return &MsgMetaData{
		State:                  m.State,
		ReservedTimestamp:      m.ReservedTimestamp,
		DelayedTimestamp:       m.DelayedTimestamp,
		FirstEnqueuedTimestamp: m.FirstEnqueuedTimestamp,
	}
}

// Msg struct defines the structure of the msg that is enqueued/dequeued
type Msg struct {
	ID          string
	TubeName    string
	Data        *Data
	Metadata    *MsgMetaData
	WaitingData *Data
	ReceiptID   *string
	IsDeleted   bool
}

func NewMsg(id string, dataSlice [][]byte, delayInSec int64, priority int, tube *Tube) *Msg {
	var msgMetaData *MsgMetaData
	now := time.Now()
	if delayInSec > 0 {
		delayedTimestamp := time.Now().Add(time.Duration(delayInSec) * time.Second)
		msgMetaData = &MsgMetaData{
			State:                  DELAYED_MSG_STATE,
			DelayedTimestamp:       &delayedTimestamp,
			FirstEnqueuedTimestamp: &now,
		}
	} else {
		msgMetaData = &MsgMetaData{
			State: READY_MSG_STATE,
			FirstEnqueuedTimestamp: &now,
		}
	}
	data := &Data{
		DelayInSec: delayInSec,
		Priority:   priority,
		Version:    time.Now().Unix(),
		DataSlice:  dataSlice,
	}
	msg := &Msg{
		ID:       id,
		Metadata: msgMetaData,
		TubeName: tube.ID,
		Data:     data,
	}
	return msg
}

func (m *Msg) Clone() *Msg {
	var data *Data
	var waitingData *Data
	var metadata *MsgMetaData
	if m.Data != nil {
		data = m.Data.Clone()
	}
	if m.WaitingData != nil {
		waitingData = m.WaitingData.Clone()
	}
	if m.Metadata != nil {
		metadata = m.Metadata.Clone()
	}
	return &Msg{
		ID:          m.ID,
		TubeName:    m.TubeName,
		Data:        data,
		Metadata:    metadata,
		WaitingData: waitingData,
		ReceiptID:   m.ReceiptID,
		IsDeleted:   m.IsDeleted,
	}
}

func (m *Msg) SetMsgState(msgState MsgState, shouldTransLog bool) {
	m.Metadata.State = msgState
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_MSG_STATE_OPR, &m.ID, msgState, m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) SetReservedTimestamp(time *time.Time, shouldTransLog bool) {
	m.Metadata.ReservedTimestamp = time
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_RESERVED_TIMESTAMP_OPR, &m.ID, time, m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) SetDelayedTimestamp(time *time.Time, shouldTransLog bool) {
	m.Metadata.DelayedTimestamp = time
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_DELAYED_TIMESTAMP_OPR, &m.ID, time, m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) SetFirstEnqueuedTimestamp(time *time.Time, shouldTransLog bool) {
	m.Metadata.FirstEnqueuedTimestamp = time
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_FIRST_ENQUEUED_TIMESTAMP_OPR, &m.ID, time, m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) SetReceiptID(receiptID *string, shouldTransLog bool) {
	m.ReceiptID = receiptID
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_RECEIPT_ID_OPR, &m.ID, receiptID, m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) SetData(data *Data, shouldTransLog bool) {
	m.Data = data
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_DATA_OPR, &m.ID, data.Clone(), m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) SetDataSlice(dataSlice [][]byte, shouldTransLog bool) {
	m.Data.DataSlice = dataSlice
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_DATA_SLICE_OPR, &m.ID, CloneDataSlice(dataSlice), m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) SetWaitingDataSlice(dataSlice [][]byte, shouldTransLog bool) {
	m.WaitingData.DataSlice = dataSlice
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_WAITING_DATA_SLICE_OPR, &m.ID, CloneDataSlice(dataSlice), m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) AppendWaitingDataToDataSlice(shouldTransLog bool) {
	m.Data.DataSlice = append(m.Data.DataSlice, m.WaitingData.DataSlice...)
	if shouldTransLog {
		opr := serde.NewOperation(MSG, APPEND_WAITING_DATA_TO_DATA_SLICE_OPR, &m.ID, m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) ReplaceDataWithWaitingDataSlice(shouldTransLog bool) {
	m.Data.DataSlice = m.WaitingData.DataSlice
	if shouldTransLog {
		opr := serde.NewOperation(MSG, REPLACE_DATA_WITH_WAITING_DATA_SLICE_OPR, &m.ID, m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) AppendDataSlice(dataSlice [][]byte, shouldTransLog bool) {
	m.Data.DataSlice = append(m.Data.DataSlice, dataSlice...)
	if shouldTransLog {
		opr := serde.NewOperation(MSG, APPEND_DATA_SLICE_OPR, &m.ID, CloneDataSlice(dataSlice), m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) AppendWaitingDataSlice(dataSlice [][]byte, shouldTransLog bool) {
	m.WaitingData.DataSlice = append(m.WaitingData.DataSlice, dataSlice...)
	if shouldTransLog {
		opr := serde.NewOperation(MSG, APPEND_WAITING_DATA_SLICE_OPR, &m.ID, CloneDataSlice(dataSlice), m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) MoveWaitingDataToData(shouldTransLog bool) {
	m.Data = m.WaitingData
	if shouldTransLog {
		opr := serde.NewOperation(MSG, MOVE_WAITING_DATA_TO_DATA, &m.ID, m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) SetDeleted(isDeleted bool, shouldTransLog bool) {
	m.IsDeleted = isDeleted
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_MSG_DELETED_OPR, &m.ID, isDeleted, m.TubeName)
		LogOpr(opr)
	}
}

func (m *Msg) SetWaitingData(data *Data, shouldTransLog bool) {
	m.WaitingData = data
	if shouldTransLog {
		opr := serde.NewOperation(MSG, SET_WAITING_DATA_OPR, &m.ID, data.Clone(), m.TubeName)
		LogOpr(opr)
	}
}
