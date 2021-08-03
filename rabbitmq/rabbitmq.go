package rabbitmq

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ibllex/go-encoding"
	"github.com/ibllex/go-queue"
	"github.com/ibllex/go-queue/internal"
	"github.com/streadway/amqp"
)

type QueueOption struct {
	// URL is a string in the AMQP URI format
	URL string
	// Codec is using for marshal and unmarshal messages
	// default is gob codec with s2 compression
	Codec encoding.Codec
}

type Queue struct {
	name string
	opt  *QueueOption

	conn *amqp.Connection
	ch   *amqp.Channel
}

func (q *Queue) Name() string {
	return q.name
}

func (q *Queue) Size() int {

	// create a temporary channel, so the main channel will not be closed on exception
	ch, err := q.conn.Channel()
	if err != nil {
		return 0
	}

	data, err := ch.QueueDeclarePassive(
		q.name, //name
		false,  //durable
		true,   //delete when unused
		false,  //exclusive
		false,  //no wait
		nil,    //arguments
	)

	if err != nil {
		return 0
	}

	ch.Close()
	return data.Messages
}

func (q *Queue) Consumer(opt *queue.ConsumerOption) (*queue.Consumer, error) {

	opt = queue.DefaultConsumerOption(opt)
	ch, err := q.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create channel error: %s", err)
	}

	// prefetch setting, see (https://www.rabbitmq.com/consumer-prefetch.html) for more detail
	err = ch.Qos(opt.PrefetchCount, 0, false)
	if err != nil {
		return nil, fmt.Errorf("channel qos error: %s", err)
	}

	return queue.NewConsumer(NewWorker(opt.ID, q, ch), opt)
}

func (q *Queue) Publish(messages ...interface{}) (err error) {

	for _, msg := range messages {
		err = q.publish(q.name, msg)
		if err != nil {
			return err
		}
	}

	return
}

func (q *Queue) Later(delay time.Duration, messages ...interface{}) (err error) {

	destination := q.name + ".delay." + strconv.FormatInt(delay.Microseconds(), 10)
	arguments := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": q.name,
		"x-message-ttl":             delay.Milliseconds(),
		"x-expires":                 delay.Milliseconds() * 2,
	}

	_, err = q.ch.QueueDeclare(
		destination, //name
		true,        //durable
		false,       //delete when unused
		false,       //exclusive
		false,       //no wait
		arguments,   //arguments
	)
	if err != nil {
		return err
	}

	for _, msg := range messages {
		err = q.publish(destination, msg)
		if err != nil {
			return err
		}
	}

	return
}

func (q *Queue) publish(destination string, msg interface{}) error {

	body, err := q.opt.Codec.Marshal(msg)
	if err != nil {
		return err
	}

	return q.ch.Publish(
		"",          // exchange
		destination, // routing key
		true,        // mandatory
		false,       // immediate
		amqp.Publishing{
			CorrelationId: internal.RandomString(32),
			ContentType:   "text/plain",
			Body:          body,
			DeliveryMode:  amqp.Persistent,
		},
	)

}

func (q *Queue) Purge() error {
	_, err := q.ch.QueuePurge(q.name, false)
	return err
}

func NewQueue(name string, opt *QueueOption) (*Queue, error) {

	conn, err := amqp.Dial(opt.URL)
	if err != nil {
		return nil, fmt.Errorf("dial error: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create channel error: %s", err)
	}

	_, err = ch.QueueDeclare(
		name,  //name
		true,  //durable
		false, //delete when unused
		false, //exclusive
		false, //no wait
		nil,   //arguments
	)
	if err != nil {
		return nil, fmt.Errorf("queue declare error: %s", err)
	}

	if opt.Codec == nil {
		opt.Codec = encoding.NewGobCodec(
			encoding.NewS2Compressor(),
		)
	}

	return &Queue{
		name: name,
		opt:  opt,
		conn: conn,
		ch:   ch,
	}, err
}
