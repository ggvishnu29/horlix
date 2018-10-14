package worker

import (
	"time"

	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"
)

/*
  DelayedQueueWorker is responsible for dequeueing the msgs (of a specific tube)
  that are in DelayedQueue struct and processing them at regular interval
*/
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
	msgMap := tube.MsgMap
	msg := msgMap.Get(qMsg.MsgID)
	if msg == nil || msg.Data == nil || msg.Data.Version != qMsg.Version || msg.IsDeleted || msg.Metadata.State != model.DELAYED_MSG_STATE {
		tube.DelayedQueue.Dequeue(true)
		tube.Lock.UnLock()
		return nil
	}
	if time.Now().Sub(*msg.Metadata.DelayedTimestamp) >= 0 {
		qMsg = tube.DelayedQueue.Dequeue(true)
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
	msg.SetMsgState(model.READY_MSG_STATE, true)
	msg.SetDelayedTimestamp(nil, true)
	qMsg := model.NewQMsg(msg)
	tube.ReadyQueue.Enqueue(qMsg, true)
}
