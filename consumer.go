package queue

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ibllex/go-queue/internal"
	"github.com/ibllex/go-queue/internal/logger"
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

//
// Consumer status
//

const (
	stateStoped = iota
	stateStarted
)

type ConsumerOption struct {
	// For logging, if it is empty,
	// a random string will be automatically generated
	ID string

	// PollDuration is the duration the queue sleeps before checking for new messages
	// Default is 1 second
	PollDuration time.Duration

	// Maximum number of goroutines processing messages.
	// Default is the number of CPUs.
	MaxNumWorker int32

	// The number of messages prefetched in the queue in a poll.
	// Default is 10.
	PrefetchSize int
	// Time that a polling receive call waits for messages to become
	// available before returning an empty response.
	// Default is 10 seconds
	FetchTimeout time.Duration

	// Message handler
	Handler Handler
}

// Consumer reserves messages from the queue, processes them,
// and then either releases or deletes messages from the queue.
type Consumer struct {
	opt *ConsumerOption
	q   Queue

	state int32 // atomic

	// Pending message
	pending chan Message
}

// Start consuming messages in the queue.
func (c *Consumer) Start(ctx context.Context) error {

	if atomic.LoadInt32(&c.state) == stateStarted {
		return fmt.Errorf("Consumer[%s-%s] is already started", c.q.Name(), c.opt.ID)
	}

	go c.start(ctx)
	atomic.StoreInt32(&c.state, stateStarted)

	logger.Infof("Consumer[%s-%s] started", c.q.Name(), c.opt.ID)
	return nil
}

func (c *Consumer) start(ctx context.Context) {

	t := time.NewTicker(c.opt.PollDuration)
	defer t.Stop()

	for {
		<-t.C

		select {
		case <-ctx.Done():
			atomic.StoreInt32(&c.state, stateStoped)
			logger.Infof("Consumer[%s-%s] stopped", c.q.Name(), c.opt.ID)
			return
		default:
			c.consume(ctx)
		}
	}

}

func (c *Consumer) consume(ctx context.Context) error {
	timeout, _ := context.WithTimeout(ctx, c.opt.FetchTimeout)
	messages, err := c.q.Fetch(timeout, c.opt.PrefetchSize)
	if err != nil {
		logger.Errorf("Consumer[%s-%s] fetching error %s", c.q.Name(), c.opt.ID, err)
		return err
	}

	for _, msg := range messages {
		c.pending <- msg
		go c.process(msg)
	}

	return nil
}

// Process message bypassing the internal queue
func (c *Consumer) process(msg Message) error {
	logger.Infof("Consumer[%s-%s] Processing %s", c.q.Name(), c.opt.ID, msg.Name())
	if c.opt.Handler != nil {
		c.opt.Handler.Handle(msg)
	}

	switch msg.Status() {
	case Acked:
		logger.Infof("Consumer[%s-%s] Processed %s", c.q.Name(), c.opt.ID, msg.Name())
	case Rejected:
		logger.Error("Consumer[%s-%s] Failed %s", c.q.Name(), c.opt.ID, msg.Name())
	case Pending:
		logger.Error("Consumer[%s-%s] Still Pending %s", c.q.Name(), c.opt.ID, msg.Name())
	}

	<-c.pending
	return nil
}

func NewConsumer(queue string, opt *ConsumerOption) (*Consumer, error) {
	q, err := Get(queue)
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
	if opt.MaxNumWorker <= 0 {
		opt.MaxNumWorker = int32(runtime.NumCPU())
	}
	if opt.FetchTimeout <= 0 {
		opt.FetchTimeout = 10 * time.Second
	}
	if opt.ID == "" {
		opt.ID = internal.RandomString(6)
	}

	c := &Consumer{
		q: q, opt: opt,
		pending: make(chan Message, opt.MaxNumWorker),
	}

	return c, nil
}
