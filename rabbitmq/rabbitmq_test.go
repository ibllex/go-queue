package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/ibllex/go-encoding"
	"github.com/ibllex/go-queue"
	"github.com/ibllex/go-queue/rabbitmq"
	"github.com/stretchr/testify/assert"
)

const (
	route = "test"
	url   = "amqp://rabbit:rabbit@localhost:5672"
)

var q *rabbitmq.Queue

func init() {
	var err error
	q, err = rabbitmq.NewQueue(route, &rabbitmq.QueueOption{
		URL:   url,
		Codec: encoding.NewJsonCodec(nil),
	})

	if err != nil {
		panic(err)
	}
}

func wait() {
	time.Sleep(100 * time.Millisecond)
}

func purge() {
	if err := q.Purge(); err != nil {
		panic(err)
	}

	wait()
}

func TestQueue(t *testing.T) {

	t.Run("publish", func(t *testing.T) {
		purge()

		assert.Equal(t, 0, q.Size())
		q.Publish(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		wait()

		assert.Equal(t, 10, q.Size())
	})

	t.Run("later", func(t *testing.T) {
		purge()

		assert.Equal(t, 0, q.Size())

		q.Later(100*time.Millisecond, "hello")
		assert.Equal(t, 0, q.Size())

		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, 1, q.Size())
	})

}

func TestMessage(t *testing.T) {

	t.Run("reject", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan bool)

		purge()
		q.Publish(1)

		assert.Equal(t, 1, q.Size())
		c, err := q.Consumer(&queue.ConsumerOption{
			PrefetchCount: 1,
			Handler: queue.H(func(m queue.Message) {
				cancel()
				m.Reject()
				done <- true
			}),
		})
		assert.Nil(t, err)
		c.Start(ctx)

		<-done
		assert.Equal(t, 1, q.Size())
	})

	t.Run("ack", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan bool)

		purge()
		q.Publish(1)

		assert.Equal(t, 1, q.Size())
		c, err := q.Consumer(&queue.ConsumerOption{
			PrefetchCount: 1,
			Handler: queue.H(func(m queue.Message) {
				cancel()
				m.Ack()
				done <- true
			}),
		})
		assert.Nil(t, err)
		c.Start(ctx)

		<-done
		assert.Equal(t, 0, q.Size())
	})
}
