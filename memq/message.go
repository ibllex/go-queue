package memq

import (
	"fmt"
	"reflect"
)

type Message struct {
	data interface{}
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
	return nil
}

func (m *Message) Accept() error {
	return nil
}
