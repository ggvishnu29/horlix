package model

import (
	"sync"
)

var tubeMap = newTubeMap()

type TubeMap struct {
	sync.Mutex
	tubes map[string]*Tube
}

func newTubeMap() *TubeMap {
	tubes := make(map[string]*Tube)
	return &TubeMap{tubes: tubes}
}

func GetTubeMap() *TubeMap {
	return tubeMap
}

func (t *TubeMap) GetTube(tubeName string) *Tube {
	return t.tubes[tubeName]
}

func (t *TubeMap) PutTube(tube *Tube) {
	t.tubes[tube.ID] = tube
}

func (t *TubeMap) DeleteTube(tube *Tube) {
	t.tubes[tube.ID] = nil
}
