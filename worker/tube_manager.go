package worker

import (
	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/operation"
)

func StartTubesManager() {
	for tube := range operation.SpawnTubeWorkersChan {
		logger.LogInfof("spanning tube worker for tube: %v\n", tube.ID)
		go StartDelayedQueueWorker(tube)
		go StartReservedQueueWorker(tube)
	}
}
