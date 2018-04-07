package contract

type IRequestContract interface {
	Serialize() ([]byte, error)
	Deserialize([]byte) (interface{}, error)
}
