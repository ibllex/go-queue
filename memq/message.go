package memq

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/ibllex/go-queue"
)

type Message struct {
	q    queue.Queue
	data interface{}

	acked    bool
	rejected bool
}

func (m *Message) Name() string {
	return fmt.Sprintf("%v", m.data)
}

func (m *Message) Unmarshal(value interface{}) error {

	v := reflect.ValueOf(value)
	if v.Type().Kind() != reflect.Ptr || !v.Elem().CanSet() {
		return fmt.Errorf("memq.Message: can not set value %v", v)
	}

	dv := reflect.ValueOf(m.data)
	if dv.Type().Kind() != reflect.Ptr {
		if !dv.CanAddr() {
			return fmt.Errorf("memq.Message: can not get value %v", dv)
		}

		dv = dv.Addr()
	}

	switch v.Elem().Kind() {
	case dv.Kind():
		v.Elem().Set(dv)
	case dv.Elem().Kind():
		v.Elem().Set(dv.Elem())
	default:
		return fmt.Errorf("memq.Message: can not set value %v", v)
	}

	return nil
}

func (m *Message) Body() []byte {
	return nil
}

func (m *Message) Reject() error {
	if m.acked {
		return errors.New("you can not reject an acked message")
	}

	m.rejected = true
	return m.q.Publish(m.data)
}

func (m *Message) Ack() error {
	if m.rejected {
		return errors.New("you can not ack a rejected message")
	}
	m.acked = true
	return nil
}

func (m *Message) Status() queue.MessageStatus {
	if m.acked {
		return queue.Acked
	}

	if m.rejected {
		return queue.Rejected
	}

	return queue.Pending
}

func NewMessage(q queue.Queue, v interface{}) *Message {
	return &Message{
		q:    q,
		data: v,
	}
}
