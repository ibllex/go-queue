package queue_test

import (
	"context"
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

func TestStartConsumer(t *testing.T) {

	t.Run("manual stop consumer", func(t *testing.T) {
		q, _ := memq.NewQueue("default")
		q.Publish(0, 1, 2, 3, 4)

		ctx, cancel := context.WithCancel(context.Background())

		c, err := q.Consumer(&queue.ConsumerOption{
			MaxNumWorker: 1,
			Handler: queue.H(func(m queue.Message) {
				time.Sleep(100 * time.Millisecond)
				m.Ack()
			}),
		})

		assert.Nil(t, err)
		c.Start(ctx)
		time.Sleep(50 * time.Millisecond)
		cancel()

		time.Sleep(1000 * time.Millisecond)
		assert.NotEqual(t, 0, q.Size())
	})

	t.Run("start multi times", func(t *testing.T) {
		q, _ := memq.NewQueue("default")

		ctx, stop := context.WithCancel(context.Background())
		c, err := q.Consumer(nil)
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
		q, _ := memq.NewQueue("default")
		data := []interface{}{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		q.Publish(data...)

		c, err := q.Consumer(&queue.ConsumerOption{
			MaxNumWorker:  1,
			PrefetchCount: 1,
			Handler: queue.H(func(m queue.Message) {
				time.Sleep(400 * time.Millisecond)
				m.Ack()
			}),
		})
		assert.Nil(t, err)

		c.Start(context.Background())
		time.Sleep(300 * time.Millisecond)

		assert.Equal(t, len(data)-1, q.Size())
	})

}
