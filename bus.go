package queue

import (
	"errors"
	"fmt"
	"time"
)

// Connector is a function to create a queue connection
type Connector func() Queue

var (
	connections     = map[string]Connector{}
	defaultConnName = "sync"
)

// AddConnection add a queue connector
func AddConnection(name string, connector Connector) {
	connections[name] = connector
}

// GetConnection returns the connection for given name
func GetConnection(name string) (Queue, error) {
	if connector, ok := connections[name]; ok {
		return connector(), nil
	}

	return nil, fmt.Errorf("connection %s not found", name)
}

// DefaultConnection returns the connection for default connection name
func DefaultConnection() (Queue, error) {
	return GetConnection(defaultConnName)
}

// SetDefaultConnection set the name of the default queue connection
func SetDefaultConnection(name string) {
	defaultConnName = name
}

//
// Dispatcher
//

type DispatchOption struct {
	Message    Message
	Connection string
	Queue      string
	Delay      time.Duration
}

// Dispatch an message to queue
func Dispatch(opt *DispatchOption) error {

	if opt.Message == nil {
		return errors.New("invalid message")
	}

	if opt.Connection == "" {
		opt.Connection = defaultConnName
	}

	q, err := GetConnection(opt.Connection)
	if err != nil {
		return err
	}

	if opt.Queue != "" && opt.Delay > 0 {
		return q.LaterOn(opt.Queue, opt.Delay, opt.Message)
	}

	if opt.Queue != "" {
		return q.PushOn(opt.Queue, opt.Message)
	}

	if opt.Delay > 0 {
		return q.Later(opt.Delay, opt.Message)
	}

	return q.Push(opt.Message)
}
