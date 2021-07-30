package queue

type Message interface {
	Unmarshal(interface{}) error
	Body() []byte
	Reject() error
	Accept() error
}
