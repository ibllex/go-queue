package queue

import (
	"context"
	"time"
)

type Queue interface {
	Publish(messages ...interface{}) error
	Later(delay time.Duration, messages ...interface{}) error
	Fetch(ctx context.Context, prefetchSize int) ([]Message, error)
}
