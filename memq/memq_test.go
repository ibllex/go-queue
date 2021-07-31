package memq_test

import (
	"context"
	"testing"
	"time"

	"github.com/ibllex/go-queue/memq"
	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {

	t.Run("publish", func(t *testing.T) {
		q := memq.NewQueue("default")
		assert.Equal(t, 0, q.Size())

		q.Publish(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		assert.Equal(t, 10, q.Size())
	})

	t.Run("later", func(t *testing.T) {
		q := memq.NewQueue("default")
		assert.Equal(t, 0, q.Size())

		q.Later(100*time.Millisecond, "hello")
		assert.Equal(t, 0, q.Size())

		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, 1, q.Size())
	})

	t.Run("over fecth", func(t *testing.T) {
		q := memq.NewQueue("default")
		messages, err := q.Fetch(context.Background(), 5)
		assert.Equal(t, 0, len(messages))
		assert.Nil(t, err)

		q.Publish(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

		messages, err = q.Fetch(context.Background(), 5)
		assert.Nil(t, err)
		assert.Equal(t, 5, len(messages))

		messages, err = q.Fetch(context.Background(), 10)
		assert.Nil(t, err)
		assert.Equal(t, 5, len(messages))
	})

	t.Run("fetch cancel", func(t *testing.T) {
		q := memq.NewQueue("default")
		q.Publish(0, 1, 2, 3, 4, 5)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		messages, err := q.Fetch(ctx, 5)

		assert.Equal(t, 0, len(messages))
		assert.NotNil(t, err)
	})
}

func TestMessage(t *testing.T) {

	t.Run("reject", func(t *testing.T) {
		q := memq.NewQueue("default")
		q.Publish("hello")
		assert.Equal(t, 1, q.Size())

		messages, _ := q.Fetch(context.Background(), 1)
		assert.Equal(t, 0, q.Size())
		messages[0].Reject()

		assert.Equal(t, 1, q.Size())
	})

	t.Run("reject an acked message", func(t *testing.T) {
		q := memq.NewQueue("default")
		q.Publish("hello")
		messages, _ := q.Fetch(context.Background(), 1)
		assert.Equal(t, 0, q.Size())

		assert.Nil(t, messages[0].Ack())
		assert.NotNil(t, messages[0].Reject())

		assert.Equal(t, 0, q.Size())
	})

	t.Run("ack a rejected message", func(t *testing.T) {
		q := memq.NewQueue("default")
		q.Publish("hello")
		messages, _ := q.Fetch(context.Background(), 1)
		assert.Equal(t, 0, q.Size())

		assert.Nil(t, messages[0].Reject())
		assert.NotNil(t, messages[0].Ack())

		assert.Equal(t, 1, q.Size())
	})
}
