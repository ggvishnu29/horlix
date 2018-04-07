package model

import (
	//"github.com/ggvishnu29/horlix/logger"
	"sort"
)

type ReadyQueue struct {
	qMsgs []*QMsg
}

var readyQEnqueueCount = 0

func (r *ReadyQueue) Enqueue(qMsg *QMsg) {
	r.qMsgs = append(r.qMsgs, qMsg)
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
	if len(r.qMsgs) == 0 {
		return nil
	}
	qMsg := r.qMsgs[0]
	r.qMsgs[0] = nil
	if len(r.qMsgs) == 1 {
		r.qMsgs = make([]*QMsg, 0)
	} else {
		r.qMsgs = r.qMsgs[1:]
	}
	return qMsg
}

func (r *ReadyQueue) Reprioritize() {
	sort.Slice(r.qMsgs, func(i, j int) bool { return r.qMsgs[i].Msg.Data.Priority > r.qMsgs[j].Msg.Data.Priority })
}

func (r *ReadyQueue) Size() int64 {
	return int64(len(r.qMsgs))
}

func (r *ReadyQueue) Capacity() int64 {
	return int64(cap(r.qMsgs))
}
