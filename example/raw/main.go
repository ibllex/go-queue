package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ibllex/go-queue"
	"github.com/ibllex/go-queue/memq"
)

const route = "example"

type Task struct {
	Name   string
	Params interface{}
}

func handler(name string) func(queue.Message) {
	return func(m queue.Message) {
		// Decode message
		var t Task
		m.Unmarshal(&t)
		fmt.Printf("%s processed: %s => %s\n", name, t.Name, t.Params)

		// You must call Ack or Reject in the handler,
		// otherwise the message will always be in the Pengding state,
		// and the rejected message will be returned to the message queue again
		m.Ack()
		time.Sleep(1 * time.Second)
	}
}

func startConsumer(name string) {

	q, _ := queue.Get(route)
	c, err := q.Consumer(&queue.ConsumerOption{
		ID:           name,
		Handler:      queue.H(handler(name)),
		MaxNumWorker: 2,
	})

	if err != nil {
		log.Fatal(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Stop the consumer after 2 seconds
	time.AfterFunc(2*time.Second, func() {
		cancel()
		log.Printf("%s stopped\n", name)
	})
	// Restart the consumer in 5 seconds
	time.AfterFunc(5*time.Second, func() {
		c.Start(context.Background())
		log.Printf("%s restart\n", name)
	})

	err = c.Start(ctx)
	if err != nil {
		log.Fatalf("error start consumer %s\n", err)
		return
	}
}

func main() {
	// Create an in-memory queue
	q, err := memq.NewQueue(route)
	// You can also switch to other queue backends without changing other codes
	// here is rabbitmq backend example
	// q, err := rabbitmq.NewQueue(route, &rabbitmq.QueueOption{
	// 	URL: "amqp://rabbit:rabbit@127.0.0.1:5672",
	// 	// Codec is using for marshal and unmarshal messages
	// 	// default is gob codec with s2 compression
	// 	// for more details, please see https://github.com/ibllex/go-encoding
	// 	Codec: encoding.NewJsonCodec(nil),
	// })

	if err != nil {
		panic(err)
	}

	// Add the queue to the queue manager
	queue.Add(q)

	// If you don’t want to display the default output of the queue,
	// you can set logger to nil or other custom logger
	queue.SetLogger(nil)

	// Here we start two consumers, mainly for demonstration,
	// in real life you should adjust the value of MaxNumWorker to instead of multiple consumers
	startConsumer("consumer 01")
	startConsumer("consumer 02")

	// Start producer
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()

		for {
			<-t.C
			// Use the Queue parameter to indicate that which queue you want to send messages,
			// you can also use Delay parameter to send delay messages
			opt := &queue.DispatchOption{Queue: route}
			err := queue.Dispatch(opt, &Task{
				Name: "send email",
				Params: map[string]interface{}{
					"subject": "hey jude",
					"from":    "the-beatles@example.com",
				},
			})
			if err != nil {
				log.Println(err)
			}
		}
	}()

	forever := make(chan bool)
	<-forever
}
