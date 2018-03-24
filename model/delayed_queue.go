package model

import (
	"sort"
)

var delayedQEnqueueCount = 0

type DelayedQueue struct {
	qMsgs []*QMsg
}

func (d *DelayedQueue) Enqueue(qMsg *QMsg) {
	d.qMsgs = append(d.qMsgs, qMsg)
	// delayedQEnqueueCount++
	// if delayedQEnqueueCount < 100000 {
	// 	return
	// }
	// tempQ := make([]*QMsg, len(d.qMsgs))
	// copy(tempQ, d.qMsgs)
	// d.qMsgs = tempQ
	// delayedQEnqueueCount = 0
	sort.Slice(d.qMsgs, func(i, j int) bool {
		return d.qMsgs[i].Msg.Metadata.DelayedTimestamp.After(*d.qMsgs[j].Msg.Metadata.DelayedTimestamp)
	})
	//runtime.GC()
}

func (d *DelayedQueue) Dequeue() *QMsg {
	if len(d.qMsgs) == 0 {
		return nil
	}
	qMsg := d.qMsgs[0]
	d.qMsgs[0] = nil
	if len(d.qMsgs) == 1 {
		d.qMsgs = make([]*QMsg, 0)
	} else {
		d.qMsgs = d.qMsgs[1 : len(d.qMsgs)-1]
	}
	return qMsg
}

func (d *DelayedQueue) Peek() *QMsg {
	if len(d.qMsgs) == 0 {
		return nil
	}
	qMsg := d.qMsgs[0]
	return qMsg
}

func (d *DelayedQueue) Reprioritize() {
	sort.Slice(d.qMsgs, func(i, j int) bool {
		return d.qMsgs[i].Msg.Metadata.DelayedTimestamp.After(*d.qMsgs[j].Msg.Metadata.DelayedTimestamp)
	})
}

func (d *DelayedQueue) Size() int64 { 
	return int64(len(d.qMsgs))
}

func (d *DelayedQueue) Capacity() int64 {
	return int64(cap(d.qMsgs))
}
