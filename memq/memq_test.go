package memq_test

import (
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
}

func TestMessage(t *testing.T) {

	t.Run("reject", func(t *testing.T) {
		q := memq.NewQueue("default")
		msg := memq.NewMessage(q, 10)
		assert.Equal(t, 0, q.Size())

		msg.Reject()
		assert.Equal(t, 1, q.Size())
	})

	t.Run("ack", func(t *testing.T) {
		q := memq.NewQueue("default")
		msg := memq.NewMessage(q, 10)
		assert.Equal(t, 0, q.Size())

		msg.Ack()
		assert.Equal(t, 0, q.Size())
	})

	t.Run("reject an acked message", func(t *testing.T) {
		q := memq.NewQueue("default")
		msg := memq.NewMessage(q, 10)
		assert.Nil(t, msg.Ack())
		assert.NotNil(t, msg.Reject())
	})

	t.Run("ack a rejected message", func(t *testing.T) {
		q := memq.NewQueue("default")
		msg := memq.NewMessage(q, 10)
		assert.Nil(t, msg.Reject())
		assert.NotNil(t, msg.Ack())
	})
}
