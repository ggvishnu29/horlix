package contract

import (
	"encoding/json"
)

type PutMsgRequest struct {
	TubeID     string
	MsgID      string
	DataBytes  []byte
	Priority   int
	DelayInSec int64
}

func (p *PutMsgRequest) Serialize() ([]byte, error) {
	return json.Marshal(p)
}

func (p *PutMsgRequest) Deserialize(data []byte) (interface{}, error) {
	req := new(PutMsgRequest)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}
