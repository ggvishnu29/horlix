package contract

import "encoding/json"

type GetTubeResponse struct {
	TubeID              string
	ReserveTimeoutInSec int64
	DataFuseSetting     int
}

func (g *GetTubeResponse) Serialize() ([]byte, error) {
	return json.Marshal(g)
}

func (g *GetTubeResponse) Deserialize(data []byte) (interface{}, error) {
	req := new(GetTubeResponse)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}
