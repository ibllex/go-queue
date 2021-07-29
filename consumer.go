package queue

import (
	"context"
)

// Consumer reserves messages from the queue, processes them,
// and then either releases or deletes messages from the queue.
type Consumer struct {
	q Queue
}

// Start consuming messages in the queue.
func (c *Consumer) Start(ctx context.Context) error {
	return nil
}
