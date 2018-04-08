package model

import (
	"sync"
)

var TMap = newTubeMap()

type TubeMap struct {
	sync.Mutex
	Tubes map[string]*Tube
}

func newTubeMap() *TubeMap {
	tubes := make(map[string]*Tube)
	return &TubeMap{Tubes: tubes}
}

func GetTubeMap() *TubeMap {
	return TMap
}

func (t *TubeMap) GetTube(tubeName string) *Tube {
	return t.Tubes[tubeName]
}

func (t *TubeMap) PutTube(tube *Tube) {
	t.Tubes[tube.ID] = tube
}

func (t *TubeMap) DeleteTube(tube *Tube) {
	t.Tubes[tube.ID] = nil
}
