package operation

import (
	"time"

	"github.com/ggvishnu29/horlix/model"
)

func FuseReadyData(data *model.Data, msg *model.Msg, tube *model.Tube) {
	if msg.Data == nil {
		msg.SetData(data)
		return
	}
	if tube.FuseSetting.Data == model.REPLACE_DATA {
		msg.SetDataSlice(data.DataSlice)
	} else {
		msg.AppendDataSlice(data.DataSlice)
	}
}

func FuseDelayedData(data *model.Data, msg *model.Msg, tube *model.Tube) {
	if msg.Data == nil {
		msg.SetData(data)
		return
	}
	if tube.FuseSetting.Data == model.REPLACE_DATA {
		msg.SetDataSlice(data.DataSlice)
	} else {
		msg.AppendDataSlice(data.DataSlice)
	}
}

func FuseWaitingData(data *model.Data, msg *model.Msg, tube *model.Tube) {
	if msg.WaitingData == nil {
		msg.SetWaitingData(data)
		if data.DelayInSec > 0 {
			delayedTimestamp := time.Now().Add(time.Duration(data.DelayInSec) * time.Second)
			msg.SetDelayedTimestamp(&delayedTimestamp)
		}
	} else if tube.FuseSetting.Data == model.REPLACE_DATA {
		msg.SetWaitingDataSlice(data.DataSlice)
	} else {
		msg.AppendWaitingDataSlice(data.DataSlice)
	}
}

func FuseWaitingDataWithData(msg *model.Msg, delayInSec int64, tube *model.Tube) {
	msg.SetReservedTimestamp(nil)
	if msg.Data == nil {
		msg.MoveWaitingDataToData()
	} else if msg.WaitingData != nil {
		if tube.FuseSetting.Data == model.REPLACE_DATA {
			msg.ReplaceDataWithWaitingDataSlice()
		} else {
			msg.AppendWaitingDataToDataSlice()
		}
	}
	msg.SetWaitingData(nil)
	BumpUpVersion(msg)
	if delayInSec > 0 {
		msg.SetMsgState(model.DELAYED_MSG_STATE)
		delayedTimestamp := time.Now().Add(time.Duration(delayInSec) * time.Second)
		msg.SetDelayedTimestamp(&delayedTimestamp)
		qMsg := model.NewQMsg(msg)
		tube.DelayedQueue.Enqueue(qMsg)
	} else {
		msg.SetMsgState(model.READY_MSG_STATE)
		msg.SetDelayedTimestamp(nil)
		qMsg := model.NewQMsg(msg)
		tube.ReadyQueue.Enqueue(qMsg)
	}
}
