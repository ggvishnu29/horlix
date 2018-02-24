package model

import (
	"sort"
)

type DelayedQueue struct {
	qMsgs []*QMsg
}

func (d *DelayedQueue) Enqueue(qMsg *QMsg) {
	d.qMsgs = append(d.qMsgs, qMsg)
	sort.Slice(d.qMsgs, func(i, j int) bool {
		return d.qMsgs[i].Msg.Metadata.DelayedTimestamp.After(*d.qMsgs[j].Msg.Metadata.DelayedTimestamp)
	})
}

func (d *DelayedQueue) Dequeue() *QMsg {
	if len(d.qMsgs) == 0 {
		return nil
	}
	qMsg := d.qMsgs[len(d.qMsgs)-1]
	d.qMsgs = d.qMsgs[0 : len(d.qMsgs)-1]
	return qMsg
}

func (d *DelayedQueue) Reprioritize() {
	sort.Slice(d.qMsgs, func(i, j int) bool {
		return d.qMsgs[i].Msg.Metadata.DelayedTimestamp.After(*d.qMsgs[j].Msg.Metadata.DelayedTimestamp)
	})
}
