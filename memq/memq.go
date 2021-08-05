package memq

import (
	"context"
	"time"

	"github.com/ibllex/go-queue"
)

type QueueOption struct {
	// Maximum number of messages that can be stored in the queue,
	// default is 1000
	BufferSize int
	// Synchronize messages
	Sync bool
	// Synchronize messages handler
	SyncHandler queue.Handler
}

type Option func(opt *QueueOption) *QueueOption

func WithSync(handler queue.Handler) Option {
	return func(opt *QueueOption) *QueueOption {
		opt.Sync = true
		opt.SyncHandler = handler
		return opt
	}
}

func WithBufferSize(bufferSize int) Option {
	return func(opt *QueueOption) *QueueOption {
		opt.BufferSize = bufferSize
		return opt
	}
}

type Queue struct {
	name   string
	buffer chan queue.Message

	syncConsumer *queue.Consumer
}

func NewQueue(name string, opts ...Option) (*Queue, error) {

	var err error
	var syncConsumer *queue.Consumer

	opt := &QueueOption{}

	for _, o := range opts {
		o(opt)
	}

	if opt.BufferSize <= 0 {
		opt.BufferSize = 1000
	}

	q := &Queue{
		name:   name,
		buffer: make(chan queue.Message, opt.BufferSize),
	}

	if opt.Sync {
		syncConsumer, err = queue.NewConsumer(q, &queue.ConsumerOption{
			Handler: opt.SyncHandler,
		})
		if err != nil {
			return nil, err
		}
		q.syncConsumer = syncConsumer
	}

	return q, nil
}

func (q *Queue) Size() int {
	return len(q.buffer)
}

func (q *Queue) Name() string {
	return q.name
}

func (q *Queue) Daemon(ctx context.Context, handler queue.HandlerFunc) error {

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-q.buffer:
			handler(msg)
		}
	}

}

func (q *Queue) Consumer(opt *queue.ConsumerOption) (*queue.Consumer, error) {
	return queue.NewConsumer(q, opt)
}

func (q *Queue) Publish(messages ...interface{}) (err error) {
	if q.syncConsumer != nil {
		for _, msg := range messages {
			err = q.syncConsumer.Process(NewMessage(q, msg))
			if err != nil {
				return err
			}
		}

		return
	}

	for _, msg := range messages {
		q.buffer <- NewMessage(q, msg)
	}

	return
}

func (q *Queue) Later(delay time.Duration, messages ...interface{}) error {

	time.AfterFunc(delay, func() {
		q.Publish(messages...)
	})

	return nil
}
