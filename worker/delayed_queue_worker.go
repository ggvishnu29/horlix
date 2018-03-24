package worker

import (
	"github.com/ggvishnu29/horlix/logger"
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
	logger.LogInfo("processing delayed msg")
	qMsg := tube.DelayedQueue.Peek()
	if qMsg == nil {
		logger.LogInfo("queue msg nil")
		tube.Lock.UnLock()
		time.Sleep(1 * time.Second)
		return nil
	}
	msg := qMsg.Msg
	if msg.Data.Version != qMsg.Version || msg.IsDeleted {
		logger.LogInfo("queue msg version not matching")
		tube.DelayedQueue.Dequeue()
		tube.Lock.UnLock()
		return nil
	}
	//logger.LogInfo("test0")
	//logger.LogInfof("processing delayed msg: %v\n", *msg.Metadata.DelayedTimestamp)
	if time.Now().Sub(*msg.Metadata.DelayedTimestamp) >= 0 {
		logger.LogInfo("making delayed msg ready")
		//logger.LogInfo("inside if")
		qMsg = tube.DelayedQueue.Dequeue()
		//logger.LogInfo("test1")
		moveQMsgToReadyQ(tube, msg)
		//logger.LogInfo("test2")
		tube.Lock.UnLock()
		//logger.LogInfo("test3")
	} else {
		tube.Lock.UnLock()
		time.Sleep(1 * time.Second)
	}
	return nil
}

func moveQMsgToReadyQ(tube *model.Tube, msg *model.Msg) {
	logger.LogInfof("version before: %v\n", msg.Data.Version)
	operation.BumpUpVersion(msg)
	logger.LogInfof("version after: %v\n", msg.Data.Version)
	msg.Metadata.State = model.READY_MSG_STATE
	msg.Metadata.DelayedTimestamp = nil
	qMsg := model.NewQMsg(msg)
	tube.ReadyQueue.Enqueue(qMsg)
}
