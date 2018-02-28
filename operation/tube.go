package operation

import (
	"fmt"

	"github.com/ggvishnu29/horlix/model"
)

var SpawnTubeWorkersChan chan *model.Tube = make(chan *model.Tube)

func CreateTube(tubeName string, reserveTimeoutInSec int64, fuseSetting *model.FuseSetting) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	if tubeMap.GetTube(tubeName) != nil {
		return fmt.Errorf("tube already exists")
	}
	tube := model.NewTube(tubeName, reserveTimeoutInSec, fuseSetting)
	tubeMap.PutTube(tube)
	SpawnTubeWorkersChan <- tube
	return nil
}

func DeleteTube(tubeName string) error {
	tubeMap := model.GetTubeMap()
	tubeMap.Lock()
	defer tubeMap.Unlock()
	tube := tubeMap.GetTube(tubeName)
	if tube == nil {
		return fmt.Errorf("tube not found")
	}
	tube.IsDeleted = true
	tubeMap.DeleteTube(tube)
	return nil
}
