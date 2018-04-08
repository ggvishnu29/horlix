package worker

import (
	"time"

	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"
)

func StartDelayedQueueWorker(tube *model.Tube) error {
	for !tube.IsDeleted {
		if err := processDelayedQMsg(tube); err != nil {
			return err
		}
	}
	return nil
}

func processDelayedQMsg(tube *model.Tube) error {
	tube.Lock.Lock()
	qMsg := tube.DelayedQueue.Peek()
	if qMsg == nil {
		tube.Lock.UnLock()
		time.Sleep(1 * time.Second)
		return nil
	}
	msg := qMsg.Msg
	if msg.Data == nil || msg.Data.Version != qMsg.Version || msg.IsDeleted || msg.Metadata.State != model.DELAYED_MSG_STATE {
		tube.DelayedQueue.Dequeue()
		tube.Lock.UnLock()
		return nil
	}
	if time.Now().Sub(*msg.Metadata.DelayedTimestamp) >= 0 {
		qMsg = tube.DelayedQueue.Dequeue()
		moveQMsgToReadyQ(tube, msg)
		tube.Lock.UnLock()
	} else {
		tube.Lock.UnLock()
		time.Sleep(1 * time.Second)
	}
	return nil
}

func moveQMsgToReadyQ(tube *model.Tube, msg *model.Msg) {
	operation.BumpUpVersion(msg)
	msg.Metadata.State = model.READY_MSG_STATE
	msg.Metadata.DelayedTimestamp = nil
	qMsg := model.NewQMsg(msg)
	tube.ReadyQueue.Enqueue(qMsg)
}
