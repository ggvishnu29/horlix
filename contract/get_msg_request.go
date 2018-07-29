package contract

import "encoding/json"

type GetMsgRequest struct {
	TubeID string
}

func (g *GetMsgRequest) Serialize() ([]byte, error) {
	return json.Marshal(g)
}

func (g *GetMsgRequest) Deserialize(data []byte) (interface{}, error) {
	req := new(GetMsgRequest)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}
