package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/ibllex/go-queue"
	"github.com/ibllex/go-queue/memq"
	"github.com/stretchr/testify/assert"
)

type MockQueue struct {
	fetchTimes int
	*memq.Queue
}

func (q *MockQueue) FetchTimes() int {
	return q.fetchTimes
}

func (q *MockQueue) Fetch(n int) ([]queue.Message, error) {
	q.fetchTimes++
	return q.Queue.Fetch(n)
}

func NewMockQueue(bufferSize int) *MockQueue {
	return &MockQueue{
		Queue: memq.NewQueue(bufferSize),
	}
}

func TestStartConsumer(t *testing.T) {
	conn := "default"
	q := NewMockQueue(10)

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
		time.Sleep(200 * time.Millisecond)

		assert.Nil(t, c.Start(ctx))
		assert.NotNil(t, c.Start(ctx))
	})
}

func TestConsume(t *testing.T) {
	conn := "default"
	q := NewMockQueue(1000)
	q.Publish(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

	queue.AddConnection(conn, func() queue.Queue {
		return q
	})

	t.Run("don't fetch new messages when all workers are busy", func(t *testing.T) {
		c, err := queue.NewConsumer(conn, &queue.ConsumerOption{
			PollDuration: 100 * time.Millisecond,
			MaxNumWorker: 1,
			PrefetchSize: 2,
			Handler: queue.H(func(m queue.Message) {
				time.Sleep(200 * time.Millisecond)
				m.Accept()
			}),
		})
		assert.Nil(t, err)

		c.Start(context.Background())
		time.Sleep(400 * time.Millisecond)

		assert.Equal(t, 1, q.fetchTimes)
	})
}
