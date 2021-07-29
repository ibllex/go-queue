package queue

import "github.com/ibllex/go-encoding"

type Message interface {
	encoding.Codec
}
