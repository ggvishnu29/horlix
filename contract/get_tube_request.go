package contract

import (
	"encoding/json"
	"fmt"
)

type GetTubeRequest struct {
	TubeID string
}

func NewGetTubeRequest(tubeID string) (*GetTubeRequest, error) {
	if tubeID == "" {
		return nil, fmt.Errorf("tube_id should not be empty")
	}
	return &GetTubeRequest{
		TubeID: tubeID,
	}, nil
}

func (g *GetTubeRequest) Serialize() ([]byte, error) {
	return json.Marshal(g)
}

func (g *GetTubeRequest) Deserialize(data []byte) (interface{}, error) {
	req := new(GetTubeRequest)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}
