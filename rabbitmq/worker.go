package rabbitmq

import (
	"context"
	"fmt"

	"github.com/ibllex/go-queue"
)

type Worker struct {
	id string
	q  *Queue

	opt *queue.ConsumerOption
}

func (w *Worker) Name() string {
	return w.q.name
}

func (w *Worker) Daemon(ctx context.Context, handler queue.HandlerFunc) error {
	ch, err := w.q.conn.Channel()
	if err != nil {
		return fmt.Errorf("create channel error: %s", err)
	}

	// prefetch setting, see (https://www.rabbitmq.com/consumer-prefetch.html) for more detail
	err = ch.Qos(w.opt.PrefetchCount, 0, false)
	if err != nil {
		return fmt.Errorf("channel qos error: %s", err)
	}

	deliveries, err := ch.Consume(
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
			return ch.Close()
		case d := <-deliveries:
			handler(NewMessage(d, w.q.opt.Codec))
		}
	}

}

func NewWorker(id string, q *Queue, opt *queue.ConsumerOption) *Worker {
	return &Worker{id: id, q: q, opt: opt}
}
