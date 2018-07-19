package worker

import (
	"bufio"
	"encoding/json"
	"os"
	"time"

	"github.com/satori/go.uuid"

	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/model"
)

var snapshotFilePath string
var transLogPath string

func InitSnapshotter(workingDir string) {
	snapshotFilePath = workingDir + "/snapshot"
	transLogPath = workingDir + "/transaction.log"
}

func TakeSnapshot() error {
	rand, err := uuid.NewV4()
	if err != nil {
		return err
	}
	tempFilePath := snapshotFilePath + "-" + rand.String()
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(tempFile)
	//encoder := gob.NewEncoder(tempFile)
	tubeMap := model.GetTubeMap()
	tubeMap.Lock.Lock()
	defer tubeMap.Lock.UnLock()
	bytes, err := json.Marshal(tubeMap)
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
	//encoder.Encode(tubeMap)
	tempFile.Close()
	os.Rename(tempFilePath, snapshotFilePath)
	logger.LogInfo("taken snapshot")
	logger.TruncateTransLog()
	logger.LogInfo("trans log truncated")
	return nil
}

func StartSnapshotter() {
	for true {
		err := TakeSnapshot()
		if err != nil {
			panic(err)
		}
		time.Sleep(10 * time.Second)
	}
}
