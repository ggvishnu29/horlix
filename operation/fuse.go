package operation

import (
	"time"

	"github.com/ggvishnu29/horlix/model"
)

func FuseReadyData(data *model.Data, msg *model.Msg) {
	if model.TMap.Tubes[msg.TubeName].FuseSetting.Data == model.REPLACE_DATA || msg.Data == nil {
		msg.Data.DataSlice = data.DataSlice
	} else {
		previousDataSlice := msg.Data.DataSlice
		msg.Data.DataSlice = append(previousDataSlice, data.DataSlice...)
	}
}

func FuseDelayedData(data *model.Data, msg *model.Msg) {
	if model.TMap.Tubes[msg.TubeName].FuseSetting.Data == model.REPLACE_DATA || msg.Data == nil {
		msg.Data.DataSlice = data.DataSlice
	} else {
		previousDataSlice := msg.Data.DataSlice
		msg.Data.DataSlice = append(previousDataSlice, data.DataSlice...)
	}
}

func FuseWaitingData(data *model.Data, msg *model.Msg) {
	if msg.WaitingData == nil {
		msg.WaitingData = data
		if data.DelayInSec > 0 {
			delayedTimestamp := time.Now().Add(time.Duration(data.DelayInSec) * time.Second)
			msg.Metadata.DelayedTimestamp = &delayedTimestamp
		}
	} else if model.TMap.Tubes[msg.TubeName].FuseSetting.Data == model.REPLACE_DATA {
		msg.WaitingData.DataSlice = data.DataSlice
	} else {
		previousDataSlice := msg.WaitingData.DataSlice
		msg.WaitingData.DataSlice = append(previousDataSlice, data.DataSlice...)
	}
}

func FuseWaitingDataWithData(msg *model.Msg, delayInSec int64) {
	msg.Metadata.ReservedTimestamp = nil
	if msg.WaitingData != nil {
		if model.TMap.Tubes[msg.TubeName].FuseSetting.Data == model.REPLACE_DATA {
			msg.Data.DataSlice = msg.WaitingData.DataSlice
		} else {
			if msg.Data == nil {
				msg.Data = msg.WaitingData
			} else {
				previousDataSlice := msg.Data.DataSlice
				msg.Data.DataSlice = append(previousDataSlice, msg.WaitingData.DataSlice...)
			}
		}
	}
	msg.WaitingData = nil
	BumpUpVersion(msg)
	if delayInSec > 0 {
		msg.Metadata.State = model.DELAYED_MSG_STATE
		delayedTimestamp := time.Now().Add(time.Duration(delayInSec) * time.Second)
		msg.Metadata.DelayedTimestamp = &delayedTimestamp
		qMsg := model.NewQMsg(msg)
		model.TMap.Tubes[msg.TubeName].DelayedQueue.Enqueue(qMsg)
	} else {
		msg.Metadata.State = model.READY_MSG_STATE
		msg.Metadata.DelayedTimestamp = nil
		qMsg := model.NewQMsg(msg)
		model.TMap.Tubes[msg.TubeName].ReadyQueue.Enqueue(qMsg)
	}
}
