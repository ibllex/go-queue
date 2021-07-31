package memq

import (
	"errors"
	"fmt"
	"reflect"
)

type Message struct {
	q    *Queue
	data interface{}

	acked    bool
	rejected bool
}

func (m *Message) Unmarshal(value interface{}) error {

	v := reflect.ValueOf(value)
	if v.Type().Kind() == reflect.Ptr && v.Elem().CanSet() {
		v.Elem().Set(reflect.ValueOf(m.data))
		return nil
	}

	return fmt.Errorf("memq.Message: can not set value %v", v)
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
