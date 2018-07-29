package worker

import (
	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/operation"
)

/*
  TubeManager is responsible for spawning different workers for each tube.
  operation.SpawnTubeWorkersChan channel is listened for any newly created
  tube
*/
func StartTubesManager() {
	for tube := range operation.SpawnTubeWorkersChan {
		logger.LogInfof("spanning tube worker for tube: %v\n", tube.ID)
		tube.DelayedQueue.Init(tube)
		go StartDelayedQueueWorker(tube)
		go StartReservedQueueWorker(tube)
	}
}
