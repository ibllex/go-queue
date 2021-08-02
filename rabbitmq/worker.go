package rabbitmq

import (
	"context"
	"fmt"

	"github.com/ibllex/go-queue"
	"github.com/streadway/amqp"
)

type Worker struct {
	id string
	q  *Queue
	ch *amqp.Channel
}

func (w *Worker) Name() string {
	return w.q.name
}

func (w *Worker) Daemon(ctx context.Context, handler queue.HandlerFunc) error {
	deliveries, err := w.ch.Consume(
		w.q.name, // queue
		w.id,     // consumer
		false,    // auto-ack
		false,    // exclusive
		false,    // noLocal
		false,    // noWait
		nil,      // arguments,
	)

	if err != nil {
		return fmt.Errorf("queue consume error: %s", err)
	}

	for {
		select {
		case <-ctx.Done():
			return w.ch.Close()
		case d := <-deliveries:
			handler(NewMessage(d, w.q.opt.Codec))
		}
	}

}

func NewWorker(id string, q *Queue, ch *amqp.Channel) *Worker {
	return &Worker{id: id, q: q, ch: ch}
}
