package queue

import (
	"errors"
	"fmt"
	"time"
)

var (
	queues       = map[string]Queue{}
	defaultQueue = "memory"
)

// Add add a queue connector
func Add(queue Queue) {
	queues[queue.Name()] = queue
}

// Get returns the queue for given name
func Get(name string) (Queue, error) {
	if q, ok := queues[name]; ok {
		return q, nil
	}

	return nil, fmt.Errorf("queue %s not found", name)
}

// Default returns the queue for default name
func Default() (Queue, error) {
	return Get(defaultQueue)
}

// SetDefault set the name of the default queue
func SetDefault(name string) {
	defaultQueue = name
}

//
// Dispatcher
//

type DispatchOption struct {
	Queue string
	Delay time.Duration
}

// Dispatch an message to queue
func Dispatch(opt *DispatchOption, messages ...interface{}) error {

	if len(messages) == 0 {
		return errors.New("no message to dispatch")
	}

	if opt.Queue == "" {
		opt.Queue = defaultQueue
	}

	q, err := Get(opt.Queue)
	if err != nil {
		return err
	}

	if opt.Delay > 0 {
		return q.Later(opt.Delay, messages...)
	}

	return q.Publish(messages...)
}
