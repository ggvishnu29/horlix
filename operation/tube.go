package operation

import (
	"fmt"

	"github.com/ggvishnu29/horlix/contract"
	"github.com/ggvishnu29/horlix/model"
)

var SpawnTubeWorkersChan chan *model.Tube = make(chan *model.Tube)

const CreateTubeOpr = "createtube"
const DeleteTubeOpr = "deletetube"

func CreateTube(req *contract.CreateTubeRequest) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock.Lock()
	defer tubeMap.Lock.UnLock()
	if tubeMap.GetTube(req.TubeName) != nil {
		return fmt.Errorf("tube already exists")
	}
	fuseSetting := &model.FuseSetting{
		Data: req.DataFuseSetting,
	}
	tube := model.NewTube(req.TubeName, req.ReserveTimeoutInSec, fuseSetting)
	tubeMap.PutTube(tube)
	tube.DelayedQueue.Init()
	SpawnTubeWorkersChan <- tube
	return nil
}

func DeleteTube(req *contract.DeleteTubeRequest) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock.Lock()
	defer tubeMap.Lock.UnLock()
	tube := tubeMap.GetTube(req.TubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.SetDeleted(true)
	tubeMap.DeleteTube(tube.ID)
	return nil
}
