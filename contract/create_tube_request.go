package contract

import (
	"encoding/json"
	"fmt"
)

type CreateTubeRequest struct {
	TubeName            string
	ReserveTimeoutInSec int64
	DataFuseSetting     int
}

func NewCreateTubeRequest(tubeName string, reserveTimeoutInSec int64, dataFuseSetting int) (*CreateTubeRequest, error) {
	if tubeName == "" {
		return nil, fmt.Errorf("tube_id should not be empty")
	}
	if reserveTimeoutInSec < 0 {
		return nil, fmt.Errorf("reserve_timeout_in_sec should be greater than 0")
	}
	if dataFuseSetting < 0 || dataFuseSetting > 1 {
		return nil, fmt.Errorf("data_fuse_setting should be either 0 or 1")
	}
	return &CreateTubeRequest{
		TubeName:            tubeName,
		ReserveTimeoutInSec: reserveTimeoutInSec,
		DataFuseSetting:     dataFuseSetting,
	}, nil
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
