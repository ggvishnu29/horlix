package contract

import (
	"encoding/json"
)

type PutMessageRequest struct {
	TubeName string
	MsgID string
	DataBytes []byte
	Priority int
	DelayInSec int64
}

func (p *PutMessageRequest) Serialize() ([]byte, error) {
	return json.Marshal(p)
}

func (p *PutMessageRequest) Deserialize(data []byte) (*PutMessageRequest, error) {
	req := new(PutMessageRequest)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}