package contract

import "encoding/json"

type AckMsgRequest struct {
	TubeName  string
	MsgID     string
	ReceiptID string
}

func (a *AckMsgRequest) Serialize() ([]byte, error) {
	return json.Marshal(a)
}

func (a *AckMsgRequest) Deserialize(data []byte) (interface{}, error) {
	req := new(AckMsgRequest)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}
