package queue

import (
	"time"
)

type Queue interface {
	Publish(messages ...interface{}) error
	Later(delay time.Duration, messages ...interface{}) error
	Fetch(n int) ([]Message, error)
}
