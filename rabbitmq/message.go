package rabbitmq

import (
	"errors"
	"strconv"

	"github.com/ibllex/go-encoding"
	"github.com/ibllex/go-queue"
	"github.com/streadway/amqp"
)

type Message struct {
	codec    encoding.Codec
	delivery amqp.Delivery

	acked    bool
	rejected bool
}

func (m *Message) Name() string {
	return strconv.FormatUint(m.delivery.DeliveryTag, 10)
}

func (m *Message) Unmarshal(value interface{}) error {
	return m.codec.Unmarshal(m.delivery.Body, value)
}

func (m *Message) Body() []byte {
	return m.delivery.Body
}

func (m *Message) Reject() (err error) {
	if m.acked {
		return errors.New("you can not reject an acked message")
	}

	err = m.delivery.Reject(true)
	if err == nil {
		m.rejected = true
	}

	return
}

func (m *Message) Ack() (err error) {
	if m.rejected {
		return errors.New("you can not ack a rejected message")
	}

	err = m.delivery.Ack(false)
	if err == nil {
		m.acked = true
	}

	return
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

func NewMessage(delivery amqp.Delivery, codec encoding.Codec) *Message {
	return &Message{
		delivery: delivery,
		codec:    codec,
	}
}
