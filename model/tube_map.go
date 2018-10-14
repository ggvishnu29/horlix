package model

import (
	"github.com/ggvishnu29/horlix/serde"
)

var TMap = newTubeMap()

/*
  TubeMap struct maintains all tube information. This datastructure
  captures all information that is ever persisted in memory by horlix.
*/
type TubeMap struct {
	Lock  *Lock
	Tubes map[string]*Tube // maps tubeID to tube
}

func newTubeMap() *TubeMap {
	tubes := make(map[string]*Tube)
	return &TubeMap{
		Tubes: tubes,
		Lock:  &Lock{},
	}
}

func GetTubeMap() *TubeMap {
	return TMap
}

func (t *TubeMap) GetTube(tubeName string) *Tube {
	return t.Tubes[tubeName]
}

func (t *TubeMap) PutTube(tube *Tube, shoouldTransLog bool) {
	t.Tubes[tube.ID] = tube
	if shoouldTransLog {
		opr := serde.NewOperation(TUBE_MAP, PUT_OPR, nil, tube.ID, tube.FuseSetting.Clone(), tube.ReserveTimeoutInSec)
		LogOpr(opr)
	}
}

func (t *TubeMap) DeleteTube(tubeID string, shoouldTransLog bool) {
	t.Tubes[tubeID] = nil
	if shoouldTransLog {
		opr := serde.NewOperation(TUBE_MAP, DELETE_OPR, &tubeID)
		LogOpr(opr)
	}
}
