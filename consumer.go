package queue

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ibllex/go-queue/internal"
	"github.com/ibllex/go-queue/internal/logger"
)

//
// Consumer
//

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

// Consumer state
const (
	StateStoped = iota
	StateStarted
)

type ConsumerOption struct {
	// For logging, if it is empty,
	// a random string will be automatically generated
	ID string

	// Maximum number of goroutines processing messages.
	// Default is the number of CPUs.
	MaxNumWorker int32

	// The number of messages prefetched in the queue in a poll.
	// Default is 10.
	PrefetchCount int

	// Message handler
	Handler Handler
}

// consumer reserves messages from the queue, processes them,
// and then either releases or deletes messages from the queue.
type Consumer struct {
	opt *ConsumerOption
	w   Worker

	state int32

	// pending messages
	pending chan struct{}

	workersWG sync.WaitGroup
}

// Start consuming messages in the queue.
func (c *Consumer) Start(ctx context.Context) error {

	if atomic.LoadInt32(&c.state) == StateStarted {
		return fmt.Errorf("consumer[%s:%s] is already started", c.w.Name(), c.opt.ID)
	}

	go c.start(ctx)

	atomic.StoreInt32(&c.state, StateStarted)
	logger.Infof("consumer[%s:%s] started", c.w.Name(), c.opt.ID)
	return nil
}

func (c *Consumer) start(ctx context.Context) {

	err := c.w.Daemon(ctx, func(m Message) {
		c.workersWG.Add(1)
		go func() {
			defer func() {
				c.workersWG.Done()
				<-c.pending
			}()
			c.Process(m)
		}()
		// Must be executed later than the worker,
		// because it will block when all the workers are busy
		c.pending <- struct{}{}
	})

	if err != nil {
		logger.Errorf("consumer[%s:%s] exit with error %s", c.w.Name(), c.opt.ID, err)
		return
	}

	atomic.StoreInt32(&c.state, StateStoped)
	logger.Infof("consumer[%s:%s] waiting for all workers to exit", c.w.Name(), c.opt.ID)
	c.workersWG.Wait()
	logger.Infof("consumer[%s:%s] stopped", c.w.Name(), c.opt.ID)

}

// Process message bypassing the internal queue
func (c *Consumer) Process(msg Message) error {
	logger.Infof("consumer[%s:%s] Processing %s", c.w.Name(), c.opt.ID, msg.Name())
	if c.opt.Handler != nil {
		c.opt.Handler.Handle(msg)
	}

	switch msg.Status() {
	case Acked:
		logger.Infof("consumer[%s:%s] Processed %s", c.w.Name(), c.opt.ID, msg.Name())
	case Rejected:
		logger.Error("consumer[%s:%s] Failed %s", c.w.Name(), c.opt.ID, msg.Name())
	case Pending:
		logger.Error("consumer[%s:%s] Still Pending %s", c.w.Name(), c.opt.ID, msg.Name())
	}

	return nil
}

func DefaultConsumerOption(opt *ConsumerOption) *ConsumerOption {

	if opt == nil {
		opt = &ConsumerOption{}
	}

	if opt.PrefetchCount <= 0 {
		opt.PrefetchCount = 10
	}
	if opt.MaxNumWorker <= 0 {
		opt.MaxNumWorker = int32(runtime.NumCPU())
	}
	if opt.ID == "" {
		opt.ID = internal.RandomString(6)
	}

	return opt
}

func NewConsumer(w Worker, opt *ConsumerOption) (*Consumer, error) {

	opt = DefaultConsumerOption(opt)

	c := &Consumer{
		w: w, opt: opt,
		// The pending list must be one less than the maximum number of workers,
		// otherwise when all workers are busy,
		// there will always be a message that has been taken out and has not been processed
		pending: make(chan struct{}, opt.MaxNumWorker-1),
	}

	return c, nil
}
