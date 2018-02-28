package model

import (
	"sort"
)

type ReadyQueue struct {
	qMsgs []*QMsg
}

func (r *ReadyQueue) Enqueue(qMsg *QMsg) {
	r.qMsgs = append(r.qMsgs, qMsg)
	sort.Slice(r.qMsgs, func(i, j int) bool {
		return (r.qMsgs[i].Msg.Data.Priority > r.qMsgs[j].Msg.Data.Priority || (r.qMsgs[i].Msg.Data.Priority == r.qMsgs[j].Msg.Data.Priority && r.qMsgs[i].Msg.Metadata.FirstEnqueuedTimestamp.After(*r.qMsgs[j].Msg.Metadata.FirstEnqueuedTimestamp)))
	})
}

func (r *ReadyQueue) Dequeue() *QMsg {
	if len(r.qMsgs) == 0 {
		return nil
	}
	qMsg := r.qMsgs[len(r.qMsgs)-1]
	r.qMsgs = r.qMsgs[0 : len(r.qMsgs)-1]
	return qMsg
}

func (r *ReadyQueue) Reprioritize() {
	sort.Slice(r.qMsgs, func(i, j int) bool { return r.qMsgs[i].Msg.Data.Priority > r.qMsgs[j].Msg.Data.Priority })
}
