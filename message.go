package queue

type MessageStatus uint32

const (
	Pending MessageStatus = iota
	Acked
	Rejected
)

type Message interface {
	Name() string
	Unmarshal(interface{}) error
	Body() []byte
	Reject() error
	Ack() error
	Status() MessageStatus
}
