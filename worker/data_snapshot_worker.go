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
		SnapshotLock.Lock()
		err := TakeSnapshot()
		SnapshotLock.UnLock()
		if err != nil {
			panic(err)
		}
		time.Sleep(2 * time.Second)
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
	tmap, err := loadDataFromSnapshot()
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
	tFile, err := os.Open(tempTransLogPath)
	if err != nil {
		return nil, err
	}
	defer tFile.Close()
	scanner := bufio.NewScanner(tFile)
	for scanner.Scan() {
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
				tmap.GetTube(tubeID).IsDeleted = isDeleted
			default:
				//return fmt.Errorf("unknown tube operation: %v\n", opr.Opr)
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
				fmt.Println(fuseSetting.Data)
				fmt.Println("printing param2")
				fmt.Println(opr.Params[2])
				reserveTimeoutInSec := int64(opr.Params[2].(float64))
				tube := model.NewTube(tubeID, reserveTimeoutInSec, &fuseSetting)
				tmap.Tubes[tubeID] = tube
			default:
				//return fmt.Errorf("unknown tube map operation: %v\n", opr.Opr)
			}
		default:
			//return fmt.Errorf("unknown datatype: %v\n", opr.DataType)
		}
	}
	return tmap, nil
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
