package operation

import (
	"time"

	"github.com/ggvishnu29/horlix/model"
)

func FuseReadyData(data *model.Data, msg *model.Msg, tube *model.Tube) {
	if msg.Data == nil {
		msg.SetData(data, true)
		return
	}
	if tube.FuseSetting.Data == model.REPLACE_DATA {
		msg.SetDataSlice(data.DataSlice, true)
	} else {
		msg.AppendDataSlice(data.DataSlice, true)
	}
}

func FuseDelayedData(data *model.Data, msg *model.Msg, tube *model.Tube) {
	if msg.Data == nil {
		msg.SetData(data, true)
		return
	}
	if tube.FuseSetting.Data == model.REPLACE_DATA {
		msg.SetDataSlice(data.DataSlice, true)
	} else {
		msg.AppendDataSlice(data.DataSlice, true)
	}
}

func FuseWaitingData(data *model.Data, msg *model.Msg, tube *model.Tube) {
	if msg.WaitingData == nil {
		msg.SetWaitingData(data, true)
		if data.DelayInSec > 0 {
			delayedTimestamp := time.Now().Add(time.Duration(data.DelayInSec) * time.Second)
			msg.SetDelayedTimestamp(&delayedTimestamp, true)
		}
	} else if tube.FuseSetting.Data == model.REPLACE_DATA {
		msg.SetWaitingDataSlice(data.DataSlice, true)
	} else {
		msg.AppendWaitingDataSlice(data.DataSlice, true)
	}
}

func FuseWaitingDataWithData(msg *model.Msg, delayInSec int64, tube *model.Tube) {
	msg.SetReservedTimestamp(nil, true)
	if msg.Data == nil {
		msg.MoveWaitingDataToData(true)
	} else if msg.WaitingData != nil {
		if tube.FuseSetting.Data == model.REPLACE_DATA {
			msg.ReplaceDataWithWaitingDataSlice(true)
		} else {
			msg.AppendWaitingDataToDataSlice(true)
		}
	}
	msg.SetWaitingData(nil, true)
	BumpUpVersion(msg)
	if delayInSec > 0 {
		msg.SetMsgState(model.DELAYED_MSG_STATE, true)
		delayedTimestamp := time.Now().Add(time.Duration(delayInSec) * time.Second)
		msg.SetDelayedTimestamp(&delayedTimestamp, true)
		qMsg := model.NewQMsg(msg)
		tube.DelayedQueue.Enqueue(qMsg, true)
	} else {
		msg.SetMsgState(model.READY_MSG_STATE, true)
		msg.SetDelayedTimestamp(nil, true)
		qMsg := model.NewQMsg(msg)
		tube.ReadyQueue.Enqueue(qMsg, true)
	}
}
