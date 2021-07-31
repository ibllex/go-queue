package queue_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibllex/go-queue"
	"github.com/ibllex/go-queue/memq"
	"github.com/stretchr/testify/assert"
)

func init() {
	// disable logger
	queue.SetLogger(nil)
}

type MockQueue struct {
	*memq.Queue

	timeout    time.Duration
	fetchTimes int
}

func (q *MockQueue) fetch(ctx context.Context, n int) ([]queue.Message, error) {
	if q.timeout > 0 {
		time.Sleep(q.timeout)
	}

	return q.Queue.Fetch(ctx, n)
}

func (q *MockQueue) Fetch(ctx context.Context, n int) ([]queue.Message, error) {
	q.fetchTimes++
	var err error

	ch := make(chan []queue.Message)
	go func() {
		var messages []queue.Message
		messages, err = q.fetch(ctx, n)
		ch <- messages
	}()

	select {
	case <-ctx.Done():
		return nil, errors.New("timeout")
	case messages := <-ch:
		return messages, err
	}
}

func NewMockQueue(name string, bufferSize int, timeout time.Duration) *MockQueue {
	return &MockQueue{
		Queue:   memq.NewQueue(name, bufferSize),
		timeout: timeout,
	}
}

func TestStartConsumer(t *testing.T) {
	q := NewMockQueue("default", 10, 0)
	queue.Add(q)

	t.Run("manual stop consumer", func(t *testing.T) {
		ctx, stop := context.WithCancel(context.Background())

		c, err := queue.NewConsumer(q.Name(), &queue.ConsumerOption{
			PollDuration: 100 * time.Millisecond,
		})

		assert.Nil(t, err)
		c.Start(ctx)
		time.Sleep(250 * time.Millisecond)

		stop()
		assert.Equal(t, 2, q.fetchTimes)

		time.Sleep(300 * time.Millisecond)
		assert.Equal(t, 2, q.fetchTimes)
	})

	t.Run("start multi times", func(t *testing.T) {

		ctx, stop := context.WithCancel(context.Background())
		c, err := queue.NewConsumer(q.Name(), &queue.ConsumerOption{
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

	t.Run("don't fetch new messages when all workers are busy", func(t *testing.T) {

		q := NewMockQueue("default", 1000, 0)
		q.Publish(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		queue.Add(q)

		c, err := queue.NewConsumer(q.Name(), &queue.ConsumerOption{
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

	t.Run("timeout while fetching message", func(t *testing.T) {

		q := NewMockQueue("timeout", 1000, time.Second)
		q.Publish(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		queue.Add(q)

		var processed int32 = 0
		c, err := queue.NewConsumer("timeout", &queue.ConsumerOption{
			PollDuration: 50 * time.Millisecond,
			MaxNumWorker: 1,
			FetchTimeout: 100 * time.Millisecond,
			Handler: queue.H(func(m queue.Message) {
				var i int
				m.Unmarshal(&i)
				fmt.Println(i)
				atomic.AddInt32(&processed, 1)
			}),
		})

		assert.Nil(t, err)
		c.Start(context.Background())
		time.Sleep(200 * time.Millisecond)

		// Note that this is not 4 times but 2 times
		// beacause the FetchTimeout is 100 millisecond
		// consumer will not fetch new messages when is blocking
		assert.Equal(t, 2, q.fetchTimes)
		assert.Equal(t, int32(0), processed)
	})
}