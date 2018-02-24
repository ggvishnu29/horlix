package model

import (
	"sort"
)

type ReservedQueue struct {
	qMsgs []*QMsg
}

func (r *ReservedQueue) Enqueue(qMsg *QMsg) {
	r.qMsgs = append(r.qMsgs, qMsg)
	sort.Slice(r.qMsgs, func(i, j int) bool {
		return r.qMsgs[i].Msg.Metadata.ReservedTimestamp.After(*r.qMsgs[j].Msg.Metadata.ReservedTimestamp)
	})
}

func (r *ReservedQueue) Dequeue() *QMsg {
	if len(r.qMsgs) == 0 {
		return nil
	}
	qMsg := r.qMsgs[len(r.qMsgs)-1]
	r.qMsgs = r.qMsgs[0 : len(r.qMsgs)-1]
	return qMsg
}

func (r *ReservedQueue) Reprioritize() {
	sort.Slice(r.qMsgs, func(i, j int) bool {
		return r.qMsgs[i].Msg.Metadata.ReservedTimestamp.After(*r.qMsgs[j].Msg.Metadata.ReservedTimestamp)
	})
}
