package worker

import (
	"time"

	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"
)

func StartReservedQueueWorker(tube *model.Tube) error {
	for !tube.IsDeleted {
		if err := processReservedQMsg(tube); err != nil {
			return err
		}
	}
	return nil
}

func processReservedQMsg(tube *model.Tube) error {
	tube.Lock.Lock()
	qMsg := tube.ReservedQueue.Peek()
	if qMsg == nil {
		tube.Lock.UnLock()
		time.Sleep(1 * time.Second)
		return nil
	}
	msgMap := tube.MsgMap
	msg := msgMap.Get(qMsg.MsgID)
	if msg == nil || msg.Data == nil || msg.Data.Version != qMsg.Version || msg.IsDeleted || msg.Metadata.State != model.RESERVED_MSG_STATE {
		tube.ReservedQueue.Dequeue()
		tube.Lock.UnLock()
		return nil
	}
	if time.Now().Sub(*msg.Metadata.ReservedTimestamp) >= 0 {
		qMsg = tube.ReservedQueue.Dequeue()
		fuseReservedQMsg(tube, msg)
		tube.Lock.UnLock()
	} else {
		tube.Lock.UnLock()
		time.Sleep(1 * time.Second)
	}
	return nil
}

func fuseReservedQMsg(tube *model.Tube, msg *model.Msg) {
	// putting back with delay as zero so that the msg becomes visible immediately
	operation.FuseWaitingDataWithData(msg, 0, tube)
}
