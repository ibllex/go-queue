package memq

import (
	"context"
	"errors"
	"time"

	"github.com/ibllex/go-queue"
)

type QueueOption struct {
	// Maximum number of messages that can be stored in the queue,
	// default is 1000
	BufferSize int
}

type Option func(opt *QueueOption) *QueueOption

func WithBufferSize(bufferSize int) Option {
	return func(opt *QueueOption) *QueueOption {
		opt.BufferSize = bufferSize
		return opt
	}
}

type Queue struct {
	name   string
	buffer chan interface{}
}

func NewQueue(name string, opts ...Option) *Queue {

	opt := &QueueOption{}

	for _, o := range opts {
		o(opt)
	}

	if opt.BufferSize <= 0 {
		opt.BufferSize = 1000
	}

	q := &Queue{
		name:   name,
		buffer: make(chan interface{}, opt.BufferSize),
	}

	return q
}

func (q *Queue) Size() int {
	return len(q.buffer)
}

func (q *Queue) Name() string {
	return q.name
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

func (q *Queue) Fetch(ctx context.Context, n int) (messages []queue.Message, err error) {

	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			for _, msg := range messages {
				msg.Reject()
			}
			return nil, errors.New("fetch time out")
		case data := <-q.buffer:
			messages = append(messages, &Message{
				q: q, data: data,
			})
		default:
		}
	}

	return
}
