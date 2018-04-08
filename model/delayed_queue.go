package model

import (
	"sort"

	"github.com/ggvishnu29/horlix/logger"
)

var delayedQEnqueueCount = 0

type DelayedQueue struct {
	QMsgs []*QMsg
}

func (d *DelayedQueue) Enqueue(qMsg *QMsg) {
	d.QMsgs = append(d.QMsgs, qMsg)
	// delayedQEnqueueCount++
	// if delayedQEnqueueCount < 100000 {
	// 	return
	// }
	// tempQ := make([]*QMsg, len(d.qMsgs))
	// copy(tempQ, d.qMsgs)
	// d.qMsgs = tempQ
	// delayedQEnqueueCount = 0
	sort.Slice(d.QMsgs, func(i, j int) bool {
		return d.QMsgs[i].Msg.Metadata.DelayedTimestamp.Before(*d.QMsgs[j].Msg.Metadata.DelayedTimestamp)
	})
	//logger.LogInfo("after enqueueing")
	//d.Print()
	//runtime.GC()
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

func (d *DelayedQueue) Reprioritize() {
	sort.Slice(d.QMsgs, func(i, j int) bool {
		return d.QMsgs[i].Msg.Metadata.DelayedTimestamp.Before(*d.QMsgs[j].Msg.Metadata.DelayedTimestamp)
	})
}

func (d *DelayedQueue) Size() int64 {
	return int64(len(d.QMsgs))
}

func (d *DelayedQueue) Capacity() int64 {
	return int64(cap(d.QMsgs))
}

func (d *DelayedQueue) Print() {
	for _, qMsg := range d.QMsgs {
		logger.LogInfof("%v %v\n", qMsg.Msg.ID, qMsg.Msg.Metadata.DelayedTimestamp)
	}
}
