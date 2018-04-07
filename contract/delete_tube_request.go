package contract

import "encoding/json"

type DeleteTubeRequest struct {
	TubeName string
}

func (d *DeleteTubeRequest) Serialize() ([]byte, error) {
	return json.Marshal(d)
}

func (d *DeleteTubeRequest) Deserialize(data []byte) (interface{}, error) {
	req := new(DeleteTubeRequest)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}
