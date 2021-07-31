package queue

import (
	"context"
	"time"

	"github.com/ibllex/go-queue/internal/logger"
)

func SetLogger(l logger.Interface) {
	logger.SetDefault(l)
}

type Queue interface {
	Name() string

	Publish(messages ...interface{}) error
	Later(delay time.Duration, messages ...interface{}) error
	Fetch(ctx context.Context, prefetchSize int) ([]Message, error)
}
