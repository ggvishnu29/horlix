package model

import (
	//"github.com/ggvishnu29/horlix/logger"
	"sort"
)

type ReadyQueue struct {
	QMsgs []*QMsg
}

var readyQEnqueueCount = 0

func (r *ReadyQueue) Enqueue(qMsg *QMsg) {
	r.QMsgs = append(r.QMsgs, qMsg)
	// readyQEnqueueCount++
	// if readyQEnqueueCount < 4 {
	// 	return
	// }
	// //logger.LogInfof("ready queue length: %v\n", len(r.qMsgs))
	// tempQ := make([]*QMsg, len(r.qMsgs))
	// copy(tempQ, r.qMsgs)
	// r.qMsgs = tempQ
	// readyQEnqueueCount = 0
	// runtime.GC()
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
	return qMsg
}

func (r *ReadyQueue) Reprioritize() {
	sort.Slice(r.QMsgs, func(i, j int) bool { return r.QMsgs[i].Msg.Data.Priority > r.QMsgs[j].Msg.Data.Priority })
}

func (r *ReadyQueue) Size() int64 {
	return int64(len(r.QMsgs))
}

func (r *ReadyQueue) Capacity() int64 {
	return int64(cap(r.QMsgs))
}
