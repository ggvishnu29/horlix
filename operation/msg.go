package operation

import (
	"github.com/ggvishnu29/horlix/logger"

	"fmt"
	"time"

	"github.com/ggvishnu29/horlix/contract"
	"github.com/ggvishnu29/horlix/model"
)

const PutMsgOpr = "putmsg"
const GetMsgOpr = "getmsg"
const ReleaseMsgOpr = "releasemsg"
const AckMsgOpr = "ackmsg"
const DeleteMsgOpr = "deletemsg"

func PutMsg(req *contract.PutMsgRequest) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(req.TubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.Lock.Lock()
	defer tube.Lock.UnLock()
	msg := tube.MsgMap.Get(req.MsgID)
	if msg == nil {
		msg = model.NewMsg(req.MsgID, req.DataBytes, req.DelayInSec, req.Priority, tube)
		tube.MsgMap.AddOrUpdate(msg)
		if req.DelayInSec <= 0 {
			msg.Metadata.State = model.READY_MSG_STATE
			qMsg := model.NewQMsg(msg)
			tube.ReadyQueue.Enqueue(qMsg)
		} else {
			msg.Metadata.State = model.DELAYED_MSG_STATE
			delayedTimestamp := time.Now().Add(time.Duration(req.DelayInSec) * time.Second)
			msg.Metadata.DelayedTimestamp = &delayedTimestamp
			qMsg := model.NewQMsg(msg)
			tube.DelayedQueue.Enqueue(qMsg)
		}
		return nil
	}
	data := model.NewData(req.DataBytes, req.Priority, req.DelayInSec)
	state := msg.Metadata.State
	switch state {
	case model.READY_MSG_STATE:
		FuseReadyData(data, msg, tube)
	case model.RESERVED_MSG_STATE:
		FuseWaitingData(data, msg, tube)
	case model.DELAYED_MSG_STATE:
		FuseDelayedData(data, msg, tube)
	default:
		return fmt.Errorf("unknown state for msg: %v", state)
	}
	logger.LogTransaction(PutMsgOpr, req)
	return nil
}

func GetMsg(req *contract.GetMsgRequest) (*model.Msg, error) {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(req.TubeName)
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
		msgMap := tube.MsgMap
		msg := msgMap.Get(qMsg.MsgID)
		if msg == nil || msg.Data.Version != qMsg.Version || msg.Metadata.State != model.READY_MSG_STATE || msg.IsDeleted {
			continue
		}
		msg.Metadata.State = model.RESERVED_MSG_STATE
		BumpUpVersion(msg)
		reserveTimeoutTimestamp := time.Now().Add(time.Duration(model.TMap.Tubes[msg.TubeName].ReserveTimeoutInSec) * time.Second)
		msg.Metadata.ReservedTimestamp = &reserveTimeoutTimestamp
		receiptID, err := GenerateReceiptID()
		if err != nil {
			return nil, fmt.Errorf("error generating unique receipt ID: %v", err)
		}
		msg.ReceiptID = receiptID
		qMsg = model.NewQMsg(msg)
		tube.ReservedQueue.Enqueue(qMsg)
		return msg, nil
	}
	logger.LogTransaction(GetMsgOpr, req)
	return nil, nil
}

func ReleaseMsg(req *contract.ReleaseMsgRequest) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(req.TubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.Lock.Lock()
	defer tube.Lock.UnLock()
	msg := tube.MsgMap.Get(req.MsgID)
	if msg == nil {
		return fmt.Errorf("no msg in the tube with the id")
	}
	if msg.Metadata.State != model.RESERVED_MSG_STATE {
		return fmt.Errorf("msg not in reserved state")
	}
	if msg.ReceiptID != req.ReceiptID {
		return fmt.Errorf("receipt ID is not matching")
	}
	FuseWaitingDataWithData(msg, req.DelayInSec, tube)
	logger.LogTransaction(ReleaseMsgOpr, req)
	return nil
}

func AckMsg(req *contract.AckMsgRequest) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(req.TubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.Lock.Lock()
	defer tube.Lock.UnLock()
	msg := tube.MsgMap.Get(req.MsgID)
	if msg == nil {
		return fmt.Errorf("no msg in the tube with the id")
	}
	BumpUpVersion(msg)
	if msg.WaitingData == nil {
		msg.IsDeleted = true
		tube.MsgMap.Delete(msg)
		return nil
	}
	msg.Data.DataSlice = msg.WaitingData.DataSlice
	msg.Data.Priority = msg.WaitingData.Priority
	msg.Data.DelayInSec = msg.WaitingData.DelayInSec
	msg.WaitingData = nil
	if msg.Metadata.DelayedTimestamp != nil && msg.Metadata.DelayedTimestamp.Sub(time.Now()) > 0 {
		msg.Metadata.State = model.DELAYED_MSG_STATE
		qMsg := model.NewQMsg(msg)
		tube.DelayedQueue.Enqueue(qMsg)
	} else {
		msg.Metadata.State = model.READY_MSG_STATE
		msg.Metadata.DelayedTimestamp = nil
		qMsg := model.NewQMsg(msg)
		tube.ReadyQueue.Enqueue(qMsg)
	}
	logger.LogTransaction(AckMsgOpr, req)
	return nil
}

func DeleteMsg(req *contract.DeleteMsgRequest) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(req.TubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.Lock.Lock()
	defer tube.Lock.UnLock()
	msg := tube.MsgMap.Get(req.MsgID)
	if msg == nil {
		return fmt.Errorf("no msg in the tube with the id")
	}
	msg.IsDeleted = true
	tube.MsgMap.Delete(msg)
	logger.LogTransaction(DeleteMsgOpr, req)
	return nil
}
