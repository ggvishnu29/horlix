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
	msg := qMsg.Msg
	if msg.Data.Version != qMsg.Version || msg.IsDeleted {
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
	operation.FuseWaitingDataWithData(msg)
}
