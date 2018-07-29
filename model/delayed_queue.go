package model

import (
	"sort"

	"github.com/ggvishnu29/horlix/serde"
)

var delayedQEnqueueCount = 0

// DelayedQueue struct maintains the list of msgs that are in delayed
// state for a specific tube
type DelayedQueue struct {
	QMsgs  []*QMsg
	TubeID string
	msgMap *MsgMap
}

func NewDelayedQueue(tubeID string) *DelayedQueue {
	d := &DelayedQueue{
		TubeID: tubeID,
	}
	return d
}

// Init method initializes the msgMap. We do not export the msgMap
// when taking snapshot. so, this method will be called to initialize
// the msgMap when restoring data from the snapshot.
func (d *DelayedQueue) Init(tube *Tube) {
	d.msgMap = tube.MsgMap
}

func (d *DelayedQueue) Enqueue(qMsg *QMsg) {
	d.QMsgs = append(d.QMsgs, qMsg)
	sort.Slice(d.QMsgs, func(i, j int) bool {
		msg1 := d.msgMap.Get(d.QMsgs[i].MsgID)
		msg2 := d.msgMap.Get(d.QMsgs[j].MsgID)
		return msg1.Metadata.DelayedTimestamp.Before(*msg2.Metadata.DelayedTimestamp)
	})
	opr := serde.NewOperation(DELAYED_QUEUE, ENQUEUE_OPR, &d.TubeID, qMsg)
	LogOpr(opr)
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
	opr := serde.NewOperation(DELAYED_QUEUE, DEQUEUE_OPR, &d.TubeID)
	LogOpr(opr)
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
