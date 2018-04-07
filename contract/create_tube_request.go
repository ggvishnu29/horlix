package contract

import "encoding/json"

type CreateTubeRequest struct {
	TubeName            string
	ReserveTimeoutInSec int64
	DataFuseSetting     int
}

func (c *CreateTubeRequest) Serialize() ([]byte, error) {
	return json.Marshal(c)
}

func (c *CreateTubeRequest) Deserialize(data []byte) (interface{}, error) {
	req := new(CreateTubeRequest)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}
