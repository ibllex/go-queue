package queue_test

import (
	"sync/atomic"
	"testing"

	"github.com/ibllex/go-queue"
	"github.com/ibllex/go-queue/memq"
	"github.com/stretchr/testify/assert"
)

const taskRoute = "task"

var counter = int32(0)

type FakeTask struct{}

type MockTask struct {
	Count int32
}

func (t *MockTask) Handle() error {
	atomic.AddInt32(&counter, t.Count)
	return nil
}

func (t *MockTask) OnQueue() string {
	return taskRoute
}

func TestRegisterTask(t *testing.T) {
	assert.Panics(t, func() {
		queue.RegisterTask(&FakeTask{})
	})

	assert.NotPanics(t, func() {
		queue.RegisterTask(&MockTask{})
	})
}

func TestDispatch(t *testing.T) {
	atomic.StoreInt32(&counter, 0)

	q, _ := memq.NewQueue(taskRoute, memq.WithSync(queue.TaskHandler()))
	queue.Add(q)
	queue.RegisterTask(&MockTask{})

	assert.NotNil(t, queue.DispatchTask(&FakeTask{}))

	assert.Nil(t, queue.DispatchTask(&MockTask{Count: 1}))
	assert.Equal(t, int32(1), counter)

	assert.Nil(t, queue.DispatchTask(&MockTask{Count: 3}))
	assert.Equal(t, int32(4), counter)
}
