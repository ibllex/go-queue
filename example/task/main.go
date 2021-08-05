package main

import (
	"fmt"
	"sync/atomic"

	"github.com/ibllex/go-queue"
	"github.com/ibllex/go-queue/memq"
)

const taskRoute = "task"

var sum = int32(0)

type AddUpTask struct {
	Count int32
}

// Every task must have a Handle method and return an error
func (t *AddUpTask) Handle() error {
	atomic.AddInt32(&sum, t.Count)
	return nil
}

// OnQueue is optional, the task will be distributed to the default queue by default,
// if OnQueue is specified, it will be distributed to the specified queue
func (t *AddUpTask) OnQueue() string {
	return taskRoute
}

func main() {
	// You must use queue.TaskHandler() as the consumer's Handler,
	// otherwise the tasks cannot be automatically distributed
	q, err := memq.NewQueue(taskRoute, memq.WithSync(queue.TaskHandler()))
	if err != nil {
		panic(err)
	}
	queue.Add(q)

	// This step is necessary, you must register the task before posting the task message,
	// otherwise the task will not be processed correctly
	queue.RegisterTask(&AddUpTask{})

	// Dispatch the task
	queue.DispatchTask(&AddUpTask{Count: 10})
	// Output: now sum is 10
	fmt.Printf("now sum is %d\n", sum)

	queue.DispatchTask(&AddUpTask{Count: 20})
	// Output: now sum is 30
	fmt.Printf("now sum is %d\n", sum)
}
