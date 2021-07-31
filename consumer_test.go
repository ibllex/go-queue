package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/ibllex/go-queue"
	"github.com/stretchr/testify/assert"
)

type MockQueue struct {
	fetchTimes int
}

func (q *MockQueue) FetchTimes() int {
	return q.fetchTimes
}

func (q *MockQueue) Publish(messages ...interface{}) error {
	return nil
}

func (q *MockQueue) Later(delay time.Duration, messages ...interface{}) error {
	return nil
}

func (q *MockQueue) Fetch(n int) ([]queue.Message, error) {
	q.fetchTimes++
	return nil, nil
}

func TestStartConsumer(t *testing.T) {
	conn := "default"
	q := &MockQueue{}

	queue.AddConnection(conn, func() queue.Queue {
		return q
	})

	t.Run("manual stop consumer", func(t *testing.T) {
		ctx, stop := context.WithCancel(context.Background())

		c, err := queue.NewConsumer(conn, &queue.ConsumerOption{
			PollDuration: 100 * time.Millisecond,
		})

		assert.Nil(t, err)
		c.Start(ctx)
		time.Sleep(200 * time.Millisecond)

		stop()

		time.Sleep(300 * time.Millisecond)
		assert.Equal(t, 2, q.FetchTimes())
	})

	t.Run("start multi times", func(t *testing.T) {

		ctx, stop := context.WithCancel(context.Background())
		c, err := queue.NewConsumer(conn, &queue.ConsumerOption{
			PollDuration: 100 * time.Millisecond,
		})
		assert.Nil(t, err)

		assert.Nil(t, c.Start(ctx))
		assert.NotNil(t, c.Start(ctx))

		stop()
		time.Sleep(100 * time.Millisecond)

		assert.Nil(t, c.Start(ctx))
		assert.NotNil(t, c.Start(ctx))
	})
}
