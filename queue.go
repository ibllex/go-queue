package queue

import (
	"time"
)

type Queue interface {
	Size() (int, error)
	Consumer() *Consumer
	Push(msg Message) error
	PushOn(queue string, msg Message) error
	Later(delay time.Duration, msg Message) error
	LaterOn(queue string, delay time.Duration, msg Message) error
	Pop() Message
	PopOn(queue string) Message
}
