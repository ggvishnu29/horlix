package operation

import (
	"fmt"

	"github.com/ggvishnu29/horlix/logger"

	"github.com/ggvishnu29/horlix/contract"
	"github.com/ggvishnu29/horlix/model"
)

var SpawnTubeWorkersChan chan *model.Tube = make(chan *model.Tube)

const createTubeOpr = "createtube"
const deleteTubeOpr = "deletetube"

func CreateTube(req *contract.CreateTubeRequest) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	if tubeMap.GetTube(req.TubeName) != nil {
		return fmt.Errorf("tube already exists")
	}
	fuseSetting := &model.FuseSetting{
		Data: req.DataFuseSetting,
	}
	tube := model.NewTube(req.TubeName, req.ReserveTimeoutInSec, fuseSetting)
	tubeMap.PutTube(tube)
	SpawnTubeWorkersChan <- tube
	logger.LogTransaction(createTubeOpr, req)
	return nil
}

func DeleteTube(req *contract.DeleteTubeRequest) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(req.TubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.IsDeleted = true
	tubeMap.DeleteTube(tube)
	logger.LogTransaction(deleteTubeOpr, req)
	return nil
}
