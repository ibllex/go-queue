package queue

import (
	"runtime"
	"time"
)

type Handler interface {
	Handle(Message)
}

type HandlerFunc func(Message)

func (fn HandlerFunc) Handle(message Message) {
	fn(message)
}

func H(hanlder HandlerFunc) Handler {
	return hanlder
}

type ConsumerOption struct {
	// PollDuration is the duration the queue sleeps before checking for new messages
	// Default is 1 second
	PollDuration time.Duration

	// Maximum number of goroutines processing messages.
	// Default is the number of CPUs.
	MaxNumWorker int32

	// The number of messages prefetched in the queue in a poll.
	// Default is 10.
	PrefetchSize int

	// Message handler
	Handler Handler
}

// Consumer reserves messages from the queue, processes them,
// and then either releases or deletes messages from the queue.
type Consumer struct {
	opt *ConsumerOption
	q   Queue
}

// Start consuming messages in the queue.
func (c *Consumer) Start() error {
	go c.start()
	return nil
}

func (c *Consumer) start() {

	t := time.NewTicker(c.opt.PollDuration)
	defer t.Stop()

	for {
		<-t.C
		c.consume()
	}

}

func (c *Consumer) consume() error {
	messages, err := c.q.Fetch(c.opt.PrefetchSize)
	if err != nil {
		return err
	}

	for _, msg := range messages {
		go c.process(msg)
	}

	return nil
}

// Process message bypassing the internal queue
func (c *Consumer) process(msg Message) error {
	if c.opt.Handler != nil {
		c.opt.Handler.Handle(msg)
	}
	return nil
}

func NewConsumer(conn string, opt *ConsumerOption) (*Consumer, error) {
	q, err := GetConnection(conn)
	if err != nil {
		return nil, err
	}

	if opt == nil {
		opt = &ConsumerOption{}
	}

	if opt.PollDuration <= 0 {
		opt.PollDuration = time.Second
	}
	if opt.PrefetchSize <= 0 {
		opt.PrefetchSize = 10
	}
	if opt.MaxNumWorker == 0 {
		opt.MaxNumWorker = int32(runtime.NumCPU())
	}

	return &Consumer{q: q, opt: opt}, nil
}
