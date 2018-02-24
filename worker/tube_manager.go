package worker

import (
	"github.com/ggvishnu29/horlix/operation"
)

func StartTubesManager() {
	for tube := range operation.SpawnTubeWorkersChan {
		go StartDelayedQueueWorker(tube)
		go StartReservedQueueWorker(tube)
	}
}
