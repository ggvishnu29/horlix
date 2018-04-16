package model

import (
	"sort"
)

var delayedQEnqueueCount = 0

type DelayedQueue struct {
	QMsgs []*QMsg
}

func (d *DelayedQueue) Enqueue(qMsg *QMsg) {
	d.QMsgs = append(d.QMsgs, qMsg)
	//todo: get tubeMap lock & get tube lock
	tube := GetTubeMap().GetTube(qMsg.TubeID)
	msgMap := tube.MsgMap
	sort.Slice(d.QMsgs, func(i, j int) bool {
		msg1 := msgMap.Get(d.QMsgs[i].MsgID)
		msg2 := msgMap.Get(d.QMsgs[j].MsgID)
		return msg1.Metadata.DelayedTimestamp.Before(*msg2.Metadata.DelayedTimestamp)
	})
}

func (d *DelayedQueue) Dequeue() *QMsg {
	if len(d.QMsgs) == 0 {
		return nil
	}
	qMsg := d.QMsgs[0]
	d.QMsgs[0] = nil
	if len(d.QMsgs) == 1 {
		d.QMsgs = make([]*QMsg, 0)
	} else {
		d.QMsgs = d.QMsgs[1:]
	}
	return qMsg
}

func (d *DelayedQueue) Peek() *QMsg {
	if len(d.QMsgs) == 0 {
		return nil
	}
	qMsg := d.QMsgs[0]
	return qMsg
}

func (d *DelayedQueue) Size() int64 {
	return int64(len(d.QMsgs))
}

func (d *DelayedQueue) Capacity() int64 {
	return int64(cap(d.QMsgs))
}

// func (d *DelayedQueue) Print() {
// 	for _, qMsg := range d.QMsgs {
// 		logger.LogInfof("%v %v\n", qMsg.Msg.ID, qMsg.Msg.Metadata.DelayedTimestamp)
// 	}
// }
