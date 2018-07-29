package model

import (
	"github.com/ggvishnu29/horlix/serde"
)

var reservedQEnqueueCount = 0

// ReservedQueue struct maintains the list of msgs that are in reserved
// state for a specific tube
type ReservedQueue struct {
	QMsgs  []*QMsg
	TubeID string
}

func NewReservedQueue(tubeID string) *ReservedQueue {
	return &ReservedQueue{
		TubeID: tubeID,
	}
}

func (r *ReservedQueue) Enqueue(qMsg *QMsg) {
	r.QMsgs = append(r.QMsgs, qMsg)
	opr := serde.NewOperation(RESERVED_QUEUE, ENQUEUE_OPR, &r.TubeID, qMsg)
	LogOpr(opr)
}

func (r *ReservedQueue) Dequeue() *QMsg {
	if len(r.QMsgs) == 0 {
		return nil
	}
	qMsg := r.QMsgs[0]
	r.QMsgs[0] = nil
	if len(r.QMsgs) == 1 {
		r.QMsgs = make([]*QMsg, 0)
	} else {
		r.QMsgs = r.QMsgs[1:]
	}
	opr := serde.NewOperation(RESERVED_QUEUE, DEQUEUE_OPR, &r.TubeID)
	LogOpr(opr)
	return qMsg
}

func (r *ReservedQueue) Peek() *QMsg {
	if len(r.QMsgs) == 0 {
		return nil
	}
	qMsg := r.QMsgs[0]
	return qMsg
}

func (r *ReservedQueue) Size() int64 {
	return int64(len(r.QMsgs))
}

func (r *ReservedQueue) Capacity() int64 {
	return int64(cap(r.QMsgs))
}
