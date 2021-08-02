package queue

import (
	"context"
	"time"

	"github.com/ibllex/go-queue/internal/logger"
)

func SetLogger(l logger.Interface) {
	logger.SetDefault(l)
}

//
// Worker
//

type Worker interface {
	Name() string
	Daemon(ctx context.Context, handler HandlerFunc) error
}

//
// Queue
//

type Queue interface {
	Name() string
	Size() int
	Consumer(opt *ConsumerOption) (*Consumer, error)

	Publish(messages ...interface{}) error
	Later(delay time.Duration, messages ...interface{}) error
	// Fetch(ctx context.Context, prefetchCount int) ([]Message, error)
}
