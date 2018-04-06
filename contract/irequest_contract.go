package contract

type IRequestContract interface {
	Serialize() (interface{}, error)
	DeSerialize(interface{}) (interface{}, error)
}