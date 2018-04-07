package contract

import "encoding/json"

type ReleaseMsgRequest struct {
	TubeName   string
	MsgID      string
	ReceiptID  string
	DelayInSec int64
}

func (p *ReleaseMsgRequest) Serialize() ([]byte, error) {
	return json.Marshal(p)
}

func (p *ReleaseMsgRequest) Deserialize(data []byte) (interface{}, error) {
	req := new(ReleaseMsgRequest)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}
