package worker

import (
	"time"

	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"
)

func StartDelayedQueueWorker(tube *model.Tube) error {
	for !tube.IsDeleted {
		qMsg := tube.DelayedQueue.Dequeue()
		if qMsg == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		msg := qMsg.Msg
		if msg.Data.Version != qMsg.Version {
			continue
		}
		if time.Now().Sub(*msg.Metadata.DelayedTimestamp) >= 0 {
			processDelayedQMsg(tube, msg)
		} else {
			time.Sleep(1 * time.Second)
			continue
		}
	}
	return nil
}

func processDelayedQMsg(tube *model.Tube, msg *model.Msg) {
	operation.BumpUpVersion(msg)
	msg.Metadata.State = model.READY_MSG_STATE
	msg.Metadata.DelayedTimestamp = nil
	qMsg := model.NewQMsg(msg)
	tube.ReadyQueue.Enqueue(qMsg)
}
