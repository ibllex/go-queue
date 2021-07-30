package memq

import (
	"time"

	"github.com/ibllex/go-queue"
)

type Queue struct {
	buffer chan interface{}
}

func NewQueue(bufferSize int) *Queue {
	q := &Queue{
		buffer: make(chan interface{}, bufferSize),
	}

	return q
}

func (q *Queue) Publish(messages ...interface{}) error {
	for _, msg := range messages {
		q.buffer <- msg
	}
	return nil
}

func (q *Queue) Later(delay time.Duration, messages ...interface{}) error {

	time.AfterFunc(delay, func() {
		q.Publish(messages...)
	})

	return nil
}

func (q *Queue) Fetch(n int) (messages []queue.Message, err error) {

	for i := 0; i <= n; i++ {
		select {
		case data := <-q.buffer:
			messages = append(messages, &Message{data})
		default:
		}
	}

	return
}
