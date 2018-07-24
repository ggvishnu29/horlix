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

type Data struct {
	DelayInSec int64
	Priority   int
	Version    int64
	DataSlice  [][]byte
}

type MsgMetaData struct {
	State                  MsgState
	ReservedTimestamp      *time.Time
	DelayedTimestamp       *time.Time
	FirstEnqueuedTimestamp *time.Time
}

func NewData(dataSlice [][]byte, priority int, delayInSec int64) *Data {
	return &Data{
		DelayInSec: delayInSec,
		Priority:   priority,
		Version:    time.Now().Unix(),
		DataSlice:  dataSlice,
	}
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

func (m *Msg) SetMsgState(msgState MsgState) {
	m.Metadata.State = msgState
	opr := serde.NewOperation(MSG, SET_MSG_STATE_OPR, &m.ID, msgState)
	LogOpr(opr)
}

func (m *Msg) SetReservedTimestamp(time *time.Time) {
	m.Metadata.ReservedTimestamp = time
	opr := serde.NewOperation(MSG, SET_RESERVED_TIMESTAMP_OPR, &m.ID, time)
	LogOpr(opr)
}

func (m *Msg) SetDelayedTimestamp(time *time.Time) {
	m.Metadata.DelayedTimestamp = time
	opr := serde.NewOperation(MSG, SET_DELAYED_TIMESTAMP_OPR, &m.ID, time)
	LogOpr(opr)
}

func (m *Msg) SetFirstEnqueuedTimestamp(time *time.Time) {
	m.Metadata.FirstEnqueuedTimestamp = time
	opr := serde.NewOperation(MSG, SET_FIRST_ENQUEUED_TIMESTAMP_OPR, &m.ID, time)
	LogOpr(opr)
}

func (m *Msg) SetReceiptID(receiptID *string) {
	m.ReceiptID = receiptID
	opr := serde.NewOperation(MSG, SET_RECEIPT_ID_OPR, &m.ID, receiptID)
	LogOpr(opr)
}

func (m *Msg) SetData(data *Data) {
	m.Data = data
	opr := serde.NewOperation(MSG, SET_DATA_OPR, &m.ID, data)
	LogOpr(opr)
}

func (m *Msg) SetDataSlice(dataSlice [][]byte) {
	m.Data.DataSlice = dataSlice
	opr := serde.NewOperation(MSG, SET_DATA_SLICE_OPR, &m.ID, dataSlice)
	LogOpr(opr)
}

func (m *Msg) SetWaitingDataSlice(dataSlice [][]byte) {
	m.WaitingData.DataSlice = dataSlice
	opr := serde.NewOperation(MSG, SET_WAITING_DATA_SLICE_OPR, &m.ID, dataSlice)
	LogOpr(opr)
}

func (m *Msg) AppendWaitingDataToDataSlice() {
	m.Data.DataSlice = append(m.Data.DataSlice, m.WaitingData.DataSlice...)
	opr := serde.NewOperation(MSG, APPEND_WAITING_DATA_TO_DATA_SLICE_OPR, &m.ID)
	LogOpr(opr)
}

func (m *Msg) ReplaceDataWithWaitingDataSlice() {
	m.Data.DataSlice = m.WaitingData.DataSlice
	opr := serde.NewOperation(MSG, REPLACE_DATA_WITH_WAITING_DATA_SLICE_OPR, &m.ID)
	LogOpr(opr)
}

func (m *Msg) AppendDataSlice(dataSlice [][]byte) {
	m.Data.DataSlice = append(m.Data.DataSlice, dataSlice...)
	opr := serde.NewOperation(MSG, APPEND_DATA_SLICE_OPR, &m.ID, dataSlice)
	LogOpr(opr)
}

func (m *Msg) AppendWaitingDataSlice(dataSlice [][]byte) {
	m.WaitingData.DataSlice = append(m.WaitingData.DataSlice, dataSlice...)
	opr := serde.NewOperation(MSG, APPEND_WAITING_DATA_SLICE_OPR, &m.ID, dataSlice)
	LogOpr(opr)
}

func (m *Msg) MoveWaitingDataToData() {
	m.Data = m.WaitingData
	opr := serde.NewOperation(MSG, MOVE_WAITING_DATA_TO_DATA, &m.ID)
	LogOpr(opr)
}

func (m *Msg) SetDeleted(isDeleted bool) {
	m.IsDeleted = isDeleted
	opr := serde.NewOperation(MSG, SET_MSG_DELETED_OPR, &m.ID, isDeleted)
	LogOpr(opr)
}

func (m *Msg) SetWaitingData(data *Data) {
	m.Data = data
	opr := serde.NewOperation(MSG, SET_WAITING_DATA_OPR, &m.ID, data)
	LogOpr(opr)
}
