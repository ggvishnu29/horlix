package contract

import "encoding/json"

type DeleteMsgRequest struct {
	TubeName string
	MsgID    string
}

func (d *DeleteMsgRequest) Serialize() ([]byte, error) {
	return json.Marshal(d)
}

func (d *DeleteMsgRequest) Deserialize(data []byte) (interface{}, error) {
	req := new(DeleteMsgRequest)
	if err := json.Unmarshal(data, req); err != nil {
		return nil, err
	}
	return req, nil
}
