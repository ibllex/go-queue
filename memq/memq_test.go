package memq_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/ibllex/go-queue/memq"
	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {

	t.Run("publish", func(t *testing.T) {
		q, _ := memq.NewQueue("default")
		assert.Equal(t, 0, q.Size())

		q.Publish(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		assert.Equal(t, 10, q.Size())
	})

	t.Run("later", func(t *testing.T) {
		q, _ := memq.NewQueue("default")
		assert.Equal(t, 0, q.Size())

		q.Later(100*time.Millisecond, "hello")
		assert.Equal(t, 0, q.Size())

		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, 1, q.Size())
	})
}

func TestMessage(t *testing.T) {

	t.Run("reject", func(t *testing.T) {
		q, _ := memq.NewQueue("default")
		msg := memq.NewMessage(q, 10)
		assert.Equal(t, 0, q.Size())

		msg.Reject()
		assert.Equal(t, 1, q.Size())
	})

	t.Run("ack", func(t *testing.T) {
		q, _ := memq.NewQueue("default")
		msg := memq.NewMessage(q, 10)
		assert.Equal(t, 0, q.Size())

		msg.Ack()
		assert.Equal(t, 0, q.Size())
	})

	t.Run("reject an acked message", func(t *testing.T) {
		q, _ := memq.NewQueue("default")
		msg := memq.NewMessage(q, 10)
		assert.Nil(t, msg.Ack())
		assert.NotNil(t, msg.Reject())
	})

	t.Run("ack a rejected message", func(t *testing.T) {
		q, _ := memq.NewQueue("default")
		msg := memq.NewMessage(q, 10)
		assert.Nil(t, msg.Reject())
		assert.NotNil(t, msg.Ack())
	})
}

type Message struct {
	Data string
}

func TestMessageUnmarshal(t *testing.T) {
	t.Run("struct", func(t *testing.T) {
		data := &Message{Data: "data"}
		msg := memq.NewMessage(nil, data)

		pTarget := &Message{}
		assert.Nil(t, msg.Unmarshal(&pTarget))
		assert.Equal(t, data.Data, pTarget.Data)

		sTarget := Message{}
		assert.Nil(t, msg.Unmarshal(&sTarget))
		assert.Equal(t, data.Data, pTarget.Data)
	})

	t.Run("primitive", func(t *testing.T) {
		sources := []interface{}{
			1, int8(1), int16(1), int32(1), int64(1),
			float32(1), float64(1),
			"string",
			'c',
			[]string{"item-01", "item02"},
			map[string]int{"0": 0, "1": 1},
		}

		for _, src := range sources {
			msg := memq.NewMessage(nil, src)

			target := reflect.New(reflect.TypeOf(src)).Interface()
			assert.Nil(t, msg.Unmarshal(target))

			actual := reflect.ValueOf(target).Elem().Interface()
			if !reflect.DeepEqual(src, actual) {
				t.Errorf("(expected) %T %v != %T %v (actual)", src, src, actual, actual)
			}
		}
	})
}
