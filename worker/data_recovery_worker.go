package worker

import (
	"encoding/json"

	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"
)

func RecoverFromTransLog() error {
	tmap, err := createTubeMapSnapshot()
	if err != nil {
		return err
	}
	model.TMap = tmap
	bytes, _ := json.Marshal(model.TMap)
	logger.LogInfof("restore tube map from snapshot: \n %v \n", string(bytes[:]))
	for _, tube := range model.TMap.Tubes {
		operation.SpawnTubeWorkersChan <- tube
	}
	return nil
}
