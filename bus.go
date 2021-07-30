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
	defaultConnName = "memory"
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
	Connection string
	Delay      time.Duration
}

// Dispatch an message to queue
func Dispatch(opt *DispatchOption, messages ...interface{}) error {

	if len(messages) == 0 {
		return errors.New("no message to dispatch")
	}

	if opt.Connection == "" {
		opt.Connection = defaultConnName
	}

	q, err := GetConnection(opt.Connection)
	if err != nil {
		return err
	}

	if opt.Delay > 0 {
		return q.Later(opt.Delay, messages...)
	}

	return q.Publish(messages...)
}
