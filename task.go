package queue

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ibllex/go-encoding"
	"github.com/ibllex/go-queue/internal"
	"github.com/ibllex/go-queue/internal/logger"
)

var (
	Tasks     TaskMap
	taskCodec = encoding.NewMsgPackCodec(nil)
)

type Task interface {
	Handle() error
}

type innerTask struct {
	Name string
	Data []byte
}

//
// Task registration management
//
type TaskMap struct {
	tasks sync.Map
}

func (t *TaskMap) Register(value interface{}) {
	t.RegisterName(internal.NameOf(value), value)
}

func (t *TaskMap) RegisterName(name string, value interface{}) {
	if name == "" {
		// reserved for nil
		panic("attempt to register empty name")
	}

	if _, ok := value.(Task); !ok {
		panic(fmt.Sprintf("%v is not a valid task type", value))
	}

	t.tasks.LoadOrStore(name, reflect.TypeOf(value))
}

func (t *TaskMap) Get(name string) interface{} {
	if v, ok := t.tasks.Load(name); ok {
		if tt, ok := v.(reflect.Type); ok {
			return reflect.New(tt.Elem()).Interface()
		}
	}

	return nil
}

//
// External Interface
//

func RegisterTask(value interface{}) {
	Tasks.Register(value)
}

func RegisterTaskName(name string, value interface{}) {
	Tasks.RegisterName(name, value)
}

// Distribute all tasks
func TaskHandler() HandlerFunc {
	return func(m Message) {
		defer logger.LogIfError(m.Ack())

		var wrapper innerTask
		if err := m.Unmarshal(&wrapper); err != nil {
			logger.Error(err)
			return
		}

		if task := Tasks.Get(wrapper.Name); task != nil {
			err := taskCodec.Unmarshal(wrapper.Data, task)
			if err != nil {
				logger.Error(err)
				return
			}

			if handler, ok := task.(Task); ok {
				logger.LogIfError(handler.Handle())
			}
		}
	}
}

//
// Dispatcher for task
//

func DispatchTask(task interface{}) error {
	return DispatchTaskName(internal.NameOf(task), task)
}

func DispatchTaskName(name string, task interface{}) error {

	if _, ok := task.(Task); !ok {
		return fmt.Errorf("%v is not a valid task type", task)
	}

	tv := reflect.ValueOf(task)
	opt := &DispatchOption{}

	if m := tv.MethodByName("OnQueue"); m.IsValid() {
		if f, ok := m.Interface().(func() string); ok {
			opt.Queue = f()
		}
	}

	if m := tv.MethodByName("Delay"); m.IsValid() {
		if f, ok := m.Interface().(func() time.Duration); ok {
			opt.Delay = f()
		}
	}

	byts, err := taskCodec.Marshal(task)
	if err != nil {
		return err
	}

	return Dispatch(opt, &innerTask{Name: name, Data: byts})
}
