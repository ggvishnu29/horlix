package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ggvishnu29/horlix/serde"

	"github.com/satori/go.uuid"

	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/model"
)

var snapshotFilePath string
var transLogPath string
var tempTransLogPath string
var SnapshotLock model.Lock

func InitSnapshotRecovery(workingDir string) {
	snapshotFilePath = workingDir + "/snapshot"
	transLogPath = workingDir + "/transaction.log"
	tempTransLogPath = workingDir + "/to-be-snapshotted.log"
}

func StartSnapshotter() {
	for true {
		time.Sleep(2 * time.Second)
		SnapshotLock.Lock()
		err := TakeSnapshot()
		SnapshotLock.UnLock()
		if err != nil {
			panic(err)
		}
	}
}

func TakeSnapshot() error {
	tmap, err := createTubeMapSnapshot()
	if err != nil {
		return err
	}
	rand, err := uuid.NewV4()
	if err != nil {
		return err
	}
	tempSnapshotFilePath := snapshotFilePath + "-" + rand.String()
	tempSnapshotFile, err := os.Create(tempSnapshotFilePath)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(tempSnapshotFile)
	bytes, err := json.Marshal(tmap)
	if err != nil {
		return err
	}
	byteString := string(bytes[:])
	if _, err := writer.WriteString(byteString + "\n"); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := tempSnapshotFile.Close(); err != nil {
		return err
	}
	os.Rename(tempSnapshotFilePath, snapshotFilePath)
	logger.LogInfo("taken snapshot")
	return nil
}

func createTubeMapSnapshot() (*model.TubeMap, error) {
	logger.LogInfo("taking snapshot")
	tmap, err := loadDataFromSnapshot()
	logger.LogInfo("loaded data from snapshot")
	if err != nil {
		return nil, err
	}
	if tmap == nil {
		tubes := make(map[string]*model.Tube)
		tmap = &model.TubeMap{
			Tubes: tubes,
			Lock:  &model.Lock{},
		}
	}
	err = logger.CopyTruncateTransLogToFile(tempTransLogPath)
	if err != nil {
		return nil, err
	}
	logger.LogInfo("copy truncate trans log file")
	tFile, err := os.Open(tempTransLogPath)
	if err != nil {
		return nil, err
	}
	defer tFile.Close()
	scanner := bufio.NewScanner(tFile)
	i := 0
	for scanner.Scan() {
		i++
		bytes := scanner.Bytes()
		opr := serde.Operation{}
		if err := json.Unmarshal(bytes, &opr); err != nil {
			return nil, err
		}
		switch opr.DataType {
		case model.TUBE:
			switch opr.Opr {
			case model.SET_TUBE_DELETED_OPR:
				tubeID := opr.Params[0].(string)
				isDeleted := opr.Params[1].(bool)
				tube := tmap.GetTube(tubeID)
				tube.SetDeleted(isDeleted, false)
			default:
				return nil, fmt.Errorf("unknown tube operation: %v\n", opr.Opr)
			}
		case model.TUBE_MAP:
			switch opr.Opr {
			case model.PUT_OPR:
				tubeID := opr.Params[0].(string)
				bytes, err := json.Marshal(opr.Params[1])
				if err != nil {
					return nil, err
				}
				var fuseSetting model.FuseSetting
				if err := json.Unmarshal(bytes, &fuseSetting); err != nil {
					return nil, err
				}
				reserveTimeoutInSec := int64(opr.Params[2].(float64))
				tube := model.NewTube(tubeID, reserveTimeoutInSec, &fuseSetting)
				tmap.PutTube(tube, false)
			case model.DELETE_OPR:
				tubeID := *opr.ResourceID
				tmap.DeleteTube(tubeID, false)
			default:
				return nil, fmt.Errorf("unknown tube map operation: %v\n", opr.Opr)
			}
		case model.RESERVED_QUEUE:
			switch opr.Opr {
			case model.ENQUEUE_OPR:
				tubeID := *opr.ResourceID
				bytes, err := json.Marshal(opr.Params[0])
				if err != nil {
					return nil, err
				}
				var qMsg model.QMsg
				if err := json.Unmarshal(bytes, &qMsg); err != nil {
					return nil, err
				}
				reservedQ := tmap.Tubes[tubeID].ReservedQueue
				reservedQ.Enqueue(&qMsg, false)
			case model.DEQUEUE_OPR:
				tubeID := *opr.ResourceID
				tube := tmap.Tubes[tubeID]
				reservedQ := tube.ReservedQueue
				reservedQ.Dequeue(false)
			default:
				return nil, fmt.Errorf("unknown reserved queue operation: %v\n", opr.Opr)
			}
		case model.READY_QUEUE:
			switch opr.Opr {
			case model.ENQUEUE_OPR:
				tubeID := *opr.ResourceID
				bytes, err := json.Marshal(opr.Params[0])
				if err != nil {
					return nil, err
				}
				var qMsg model.QMsg
				if err := json.Unmarshal(bytes, &qMsg); err != nil {
					return nil, err
				}
				readyQ := tmap.Tubes[tubeID].ReadyQueue
				readyQ.Enqueue(&qMsg, false)
			case model.DEQUEUE_OPR:
				tubeID := *opr.ResourceID
				readyQ := tmap.Tubes[tubeID].ReadyQueue
				readyQ.Dequeue(false)
			default:
				return nil, fmt.Errorf("unknown ready queue operation: %v\n", opr.Opr)
			}
		case model.DELAYED_QUEUE:
			switch opr.Opr {
			case model.ENQUEUE_OPR:
				tubeID := *opr.ResourceID
				bytes, err := json.Marshal(opr.Params[0])
				if err != nil {
					return nil, err
				}
				var qMsg model.QMsg
				if err := json.Unmarshal(bytes, &qMsg); err != nil {
					return nil, err
				}
				delayedQ := tmap.Tubes[tubeID].DelayedQueue
				delayedQ.Enqueue(&qMsg, false)
			case model.DEQUEUE_OPR:
				tubeID := *opr.ResourceID
				delayedQ := tmap.Tubes[tubeID].DelayedQueue
				delayedQ.Dequeue(false)
			default:
				return nil, fmt.Errorf("unknown delayed queue operation: %v\n", opr.Opr)
			}
		case model.MSG_MAP:
			switch opr.Opr {
			case model.ADD_OR_UPDATE_OPR:
				tubeID := opr.Params[0].(string)
				tube := tmap.GetTube(tubeID)
				bytes, err := json.Marshal(opr.Params[1])
				if err != nil {
					return nil, err
				}
				var msg model.Msg
				if err := json.Unmarshal(bytes, &msg); err != nil {
					return nil, err
				}
				tube.MsgMap.AddOrUpdate(&msg, false)
			case model.DELETE_OPR:
				tubeID := opr.Params[0].(string)
				msgID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				tube.MsgMap.Delete(msgID, false)
			default:
				return nil, fmt.Errorf("unknown msg map operation: %v\n", opr.Opr)
			}
		case model.MSG:
			switch opr.Opr {
			case model.SET_MSG_STATE_OPR:
				msgID := *opr.ResourceID
				msgState := opr.Params[0].(float64)
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetMsgState(model.MsgState(msgState), false)
			case model.SET_RESERVED_TIMESTAMP_OPR:
				msgID := *opr.ResourceID
				var reservedTimestamp *time.Time
				if opr.Params[0] != nil {
					reservedTimestamp, err = parseTime(opr.Params[0].(string))
					if err != nil {
						return nil, fmt.Errorf("error while parsing reserved timestamp. " + err.Error())
					}
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetReservedTimestamp(reservedTimestamp, false)
			case model.SET_DELAYED_TIMESTAMP_OPR:
				msgID := *opr.ResourceID
				var delayedTimestamp *time.Time
				if opr.Params[0] != nil {
					delayedTimestamp, err = parseTime(opr.Params[0].(string))
					if err != nil {
						return nil, fmt.Errorf("error while parsing delayed timestamp. " + err.Error())
					}
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetDelayedTimestamp(delayedTimestamp, false)
			case model.SET_FIRST_ENQUEUED_TIMESTAMP_OPR:
				msgID := *opr.ResourceID
				var firstEnqueuedTimestamp *time.Time
				if opr.Params[0] != nil {
					firstEnqueuedTimestamp, err = parseTime(opr.Params[0].(string))
					if err != nil {
						return nil, fmt.Errorf("error while parsing first enqueued timestamp. " + err.Error())
					}
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetFirstEnqueuedTimestamp(firstEnqueuedTimestamp, false)
			case model.SET_RECEIPT_ID_OPR:
				msgID := *opr.ResourceID
				var receiptID *string
				if opr.Params[0] != nil {
					r := opr.Params[0].(string)
					receiptID = &r
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetReceiptID(receiptID, false)
			case model.SET_DATA_OPR:
				msgID := *opr.ResourceID
				bytes, err := json.Marshal(opr.Params[0])
				if err != nil {
					return nil, err
				}
				var data model.Data
				if err := json.Unmarshal(bytes, &data); err != nil {
					return nil, err
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetData(&data, false)
			case model.SET_DATA_SLICE_OPR:
				msgID := *opr.ResourceID
				bytes, err := json.Marshal(opr.Params[0])
				if err != nil {
					return nil, err
				}
				var dataSlice [][]byte
				if err := json.Unmarshal(bytes, dataSlice); err != nil {
					return nil, err
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetDataSlice(dataSlice, false)
			case model.SET_WAITING_DATA_SLICE_OPR:
				msgID := *opr.ResourceID
				bytes, err := json.Marshal(opr.Params[0])
				if err != nil {
					return nil, err
				}
				var dataSlice [][]byte
				if err := json.Unmarshal(bytes, &dataSlice); err != nil {
					return nil, err
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetWaitingDataSlice(dataSlice, false)
			case model.APPEND_WAITING_DATA_TO_DATA_SLICE_OPR:
				msgID := *opr.ResourceID
				tubeID := opr.Params[0].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.AppendWaitingDataToDataSlice(false)
			case model.REPLACE_DATA_WITH_WAITING_DATA_SLICE_OPR:
				msgID := *opr.ResourceID
				tubeID := opr.Params[0].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.ReplaceDataWithWaitingDataSlice(false)
			case model.APPEND_DATA_SLICE_OPR:
				msgID := *opr.ResourceID
				bytes, err := json.Marshal(opr.Params[0])
				if err != nil {
					return nil, err
				}
				var dataSlice [][]byte
				if err := json.Unmarshal(bytes, &dataSlice); err != nil {
					return nil, err
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.AppendDataSlice(dataSlice, false)
			case model.APPEND_WAITING_DATA_SLICE_OPR:
				msgID := *opr.ResourceID
				bytes, err := json.Marshal(opr.Params[0])
				if err != nil {
					return nil, err
				}
				var dataSlice [][]byte
				if err := json.Unmarshal(bytes, &dataSlice); err != nil {
					return nil, err
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.AppendWaitingDataSlice(dataSlice, false)
			case model.MOVE_WAITING_DATA_TO_DATA:
				msgID := *opr.ResourceID
				tubeID := opr.Params[0].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.MoveWaitingDataToData(false)
			case model.SET_MSG_DELETED_OPR:
				msgID := *opr.ResourceID
				isDeleted := opr.Params[0].(bool)
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetDeleted(isDeleted, false)
			case model.SET_WAITING_DATA_OPR:
				msgID := *opr.ResourceID
				bytes, err := json.Marshal(opr.Params[0])
				if err != nil {
					return nil, err
				}
				var data model.Data
				if err := json.Unmarshal(bytes, &data); err != nil {
					return nil, err
				}
				tubeID := opr.Params[1].(string)
				tube := tmap.GetTube(tubeID)
				msg := tube.MsgMap.Get(msgID)
				msg.SetWaitingData(&data, false)
			default:
				return nil, fmt.Errorf("unknown msg operation: %v\n", opr.Opr)
			}
		default:
			return nil, fmt.Errorf("unknown datatype: %v\n", opr.DataType)
		}
	}
	logger.LogInfof("applied transaction logs: %v", i)
	return tmap, nil
}

func parseTime(timeStr string) (*time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, timeStr)
	return &t, err
}

func loadDataFromSnapshot() (*model.TubeMap, error) {
	file, err := os.Open(snapshotFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.LogInfo("no snapshot exists. So, starting fresh..")
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	byteString, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	bytes := []byte(byteString)
	tmap := &model.TubeMap{}
	if err := json.Unmarshal(bytes, tmap); err != nil {
		return nil, err
	}
	return tmap, nil
}
