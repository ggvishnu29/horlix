package model

var TMap = newTubeMap()

type TubeMap struct {
	Lock  *Lock
	Tubes map[string]*Tube
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

func (t *TubeMap) PutTube(tube *Tube) {
	t.Tubes[tube.ID] = tube
}

func (t *TubeMap) DeleteTube(tube *Tube) {
	t.Tubes[tube.ID] = nil
}
