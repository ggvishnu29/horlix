package model

var reservedQEnqueueCount = 0

type ReservedQueue struct {
	QMsgs []*QMsg
}

func (r *ReservedQueue) Enqueue(qMsg *QMsg) {
	r.QMsgs = append(r.QMsgs, qMsg)
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
