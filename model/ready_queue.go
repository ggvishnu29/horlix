package model

import (
	"github.com/ggvishnu29/horlix/serde"
)

// ReadyQueue struct maintains the list of msgs that are in ready
// state for a specific tube
type ReadyQueue struct {
	QMsgs  []*QMsg
	TubeID string
}

func NewReadyQueue(tubeID string) *ReadyQueue {
	return &ReadyQueue{
		TubeID: tubeID,
	}
}

func (r *ReadyQueue) Enqueue(qMsg *QMsg) {
	//todo: sort queue based on priority
	r.QMsgs = append(r.QMsgs, qMsg)
	opr := serde.NewOperation(READY_QUEUE, ENQUEUE_OPR, &r.TubeID, qMsg)
	LogOpr(opr)
}

func (r *ReadyQueue) Dequeue() *QMsg {
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
	opr := serde.NewOperation(READY_QUEUE, DEQUEUE_OPR, &r.TubeID)
	LogOpr(opr)
	return qMsg
}

func (r *ReadyQueue) Size() int64 {
	return int64(len(r.QMsgs))
}

func (r *ReadyQueue) Capacity() int64 {
	return int64(cap(r.QMsgs))
}
