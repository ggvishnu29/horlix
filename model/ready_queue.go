package model

type ReadyQueue struct {
	QMsgs  []*QMsg
	TubeID string
}

func (r *ReadyQueue) Enqueue(qMsg *QMsg) {
	//todo: sort queue based on priority
	r.QMsgs = append(r.QMsgs, qMsg)
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

func (r *ReadyQueue) Size() int64 {
	return int64(len(r.QMsgs))
}

func (r *ReadyQueue) Capacity() int64 {
	return int64(cap(r.QMsgs))
}
