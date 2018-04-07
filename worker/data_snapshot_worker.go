package worker

import (
	"encoding/gob"
	"os"
	"time"

	"github.com/satori/go.uuid"

	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/model"
)

var snapshotFilePath string

func InitSnapshotter(workingDir string) {
	snapshotFilePath = workingDir + "/snapshot"
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
	encoder := gob.NewEncoder(tempFile)
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	encoder.Encode(tubeMap)
	tempFile.Close()
	os.Rename(tempFilePath, snapshotFilePath)
	return nil
}

func StartSnapshotter() {
	for true {
		err := TakeSnapshot()
		logger.TruncateTransLog()
		if err != nil {
			panic(err)
		}
		time.Sleep(10 * time.Minute)
	}
}
