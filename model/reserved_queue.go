package model

var reservedQEnqueueCount = 0

type ReservedQueue struct {
	qMsgs []*QMsg
}

func (r *ReservedQueue) Enqueue(qMsg *QMsg) {
	r.qMsgs = append(r.qMsgs, qMsg)
	// reservedQEnqueueCount++
	// if reservedQEnqueueCount < 100000 {
	// 	return
	// }
	// tempQ := make([]*QMsg, len(r.qMsgs))
	// copy(tempQ, r.qMsgs)
	// r.qMsgs = tempQ
	// reservedQEnqueueCount = 0
	// runtime.GC()
}

func (r *ReservedQueue) Dequeue() *QMsg {
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

func (r *ReservedQueue) Peek() *QMsg {
	if len(r.qMsgs) == 0 {
		return nil
	}
	qMsg := r.qMsgs[0]
	return qMsg
}

func (r *ReservedQueue) Size() int64 {
	return int64(len(r.qMsgs))
}

func (r *ReservedQueue) Capacity() int64 {
	return int64(cap(r.qMsgs))
}
