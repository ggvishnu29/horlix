package worker

import (
	"time"

	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"
)

func StartReservedQueueWorker(tube *model.Tube) error {
	for !tube.IsDeleted {
		qMsg := tube.ReservedQueue.Dequeue()
		if qMsg == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		msg := qMsg.Msg
		if msg.Data.Version != qMsg.Version {
			continue
		}
		if time.Now().Sub(*msg.Metadata.ReservedTimestamp) >= 0 {
			processReservedQMsg(tube, msg)
		} else {
			time.Sleep(1 * time.Second)
			continue
		}
	}
	return nil
}

func processReservedQMsg(tube *model.Tube, msg *model.Msg) {
	operation.FuseWaitingDataWithData(msg)
}
