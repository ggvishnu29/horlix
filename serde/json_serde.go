package serde

import "encoding/json"

type JSONSerde struct {
}

func (j *JSONSerde) Serialize(o *Operation) ([]byte, error) {
	return json.Marshal(o)
}

func (j *JSONSerde) Deserialize(bytes []byte) (*Operation, error) {
	o := &Operation{}
	err := json.Unmarshal(bytes, o)
	return o, err
}
