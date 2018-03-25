package operation

import (

	//"github.com/ggvishnu29/horlix/logger"
	"fmt"
	"time"

	"github.com/ggvishnu29/horlix/model"
)

func PutMsg(tubeName string, msgID string, dataBytes []byte, priority int, delayInSec int64) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(tubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.Lock.Lock()
	defer tube.Lock.UnLock()
	msg := tube.MsgMap.Get(msgID)
	if msg == nil {
		msg = model.NewMsg(msgID, dataBytes, delayInSec, priority, tube)
		tube.MsgMap.AddOrUpdate(msg)
		if delayInSec <= 0 {
			msg.Metadata.State = model.READY_MSG_STATE
			qMsg := model.NewQMsg(msg)
			tube.ReadyQueue.Enqueue(qMsg)
		} else {
			msg.Metadata.State = model.DELAYED_MSG_STATE
			delayedTimestamp := time.Now().Add(time.Duration(delayInSec) * time.Second)
			msg.Metadata.DelayedTimestamp = &delayedTimestamp
			qMsg := model.NewQMsg(msg)
			tube.DelayedQueue.Enqueue(qMsg)
		}
		return nil
	}
	data := model.NewData(dataBytes, priority, delayInSec)
	state := msg.Metadata.State
	switch state {
	case model.READY_MSG_STATE:
		FuseReadyData(data, msg)
	case model.RESERVED_MSG_STATE:
		FuseWaitingData(data, msg)
	case model.DELAYED_MSG_STATE:
		//logger.LogInfo("fusing delayed data")
		FuseDelayedData(data, msg)
	default:
		return fmt.Errorf("unknown state for msg: %v", state)
	}
	return nil
}

func GetMsg(tubeName string) (*model.Msg, error) {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(tubeName)
	if tube == nil {
		return nil, fmt.Errorf("tube not found")
	}
	tube.Lock.Lock()
	defer tube.Lock.UnLock()
	for true {
		qMsg := tube.ReadyQueue.Dequeue()
		if qMsg == nil {
			return nil, nil
		}
		msg := qMsg.Msg
		if msg.Data.Version != qMsg.Version || msg.Metadata.State != model.READY_MSG_STATE || msg.IsDeleted {
			continue
		}
		msg.Metadata.State = model.RESERVED_MSG_STATE
		BumpUpVersion(msg)
		reserveTimeoutTimestamp := time.Now().Add(time.Duration(msg.Tube.ReserveTimeoutInSec) * time.Second)
		msg.Metadata.ReservedTimestamp = &reserveTimeoutTimestamp
		receiptID, err := GenerateReceiptID()
		if err != nil {
			// todo: log the actual error
			return nil, fmt.Errorf("error generating unique receipt ID")
		}
		msg.ReceiptID = receiptID
		qMsg = model.NewQMsg(msg)
		msg.Tube.ReservedQueue.Enqueue(qMsg)
		return qMsg.Msg, nil
	}
	return nil, nil
}

func ReleaseMsg(tubeName string, msgID string, receiptID string, delayInSec int64) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(tubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.Lock.Lock()
	defer tube.Lock.UnLock()
	msg := tube.MsgMap.Get(msgID)
	if msg == nil {
		return fmt.Errorf("no msg in the tube with the id")
	}
	if msg.Metadata.State != model.RESERVED_MSG_STATE {
		return fmt.Errorf("msg not in reserved state")
	}
	if msg.ReceiptID != receiptID {
		return fmt.Errorf("receipt ID is not matching")
	}
	FuseWaitingDataWithData(msg, delayInSec)
	return nil
}

func AckMsg(tubeName string, msgID string, receiptID string) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(tubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.Lock.Lock()
	defer tube.Lock.UnLock()
	msg := tube.MsgMap.Get(msgID)
	if msg == nil {
		return fmt.Errorf("no msg in the tube with the id")
	}
	BumpUpVersion(msg)
	if msg.WaitingData == nil {
		//logger.LogInfo("msg waiting data is nil")
		msg.IsDeleted = true
		tube.MsgMap.Delete(msg)
		return nil
	}
	msg.Data.DataSlice = msg.WaitingData.DataSlice
	msg.Data.Priority = msg.WaitingData.Priority
	msg.Data.DelayInSec = msg.WaitingData.DelayInSec
	msg.WaitingData = nil
	//logger.LogInfof("msg delayed timestamp: %v \n", msg.Metadata.DelayedTimestamp)
	if msg.Metadata.DelayedTimestamp != nil && msg.Metadata.DelayedTimestamp.Sub(time.Now()) > 0 {
		msg.Metadata.State = model.DELAYED_MSG_STATE
		qMsg := model.NewQMsg(msg)
		msg.Tube.DelayedQueue.Enqueue(qMsg)
		//logger.LogInfo("enqueued to delayed queue")
	} else {
		msg.Metadata.State = model.READY_MSG_STATE
		msg.Metadata.DelayedTimestamp = nil
		qMsg := model.NewQMsg(msg)
		msg.Tube.ReadyQueue.Enqueue(qMsg)
		//logger.LogInfo("enqueued to ready queue")
	}
	return nil
}

func DeleteMsg(tubeName string, msgID string) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(tubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.Lock.Lock()
	defer tube.Lock.UnLock()
	msg := tube.MsgMap.Get(msgID)
	if msg == nil {
		return fmt.Errorf("no msg in the tube with the id")
	}
	msg.IsDeleted = true
	tube.MsgMap.Delete(msg)
	return nil
}
