package internal_test

import (
	"testing"

	"github.com/ibllex/go-queue/internal"
	"github.com/stretchr/testify/assert"
)

type MockStruct struct{}

func MockFunc(arg string) (ret int) {
	return 0
}

func TestNameOf(t *testing.T) {
	ptr := &MockStruct{}

	items := map[string]interface{}{
		"*internal_test.MockStruct":                           ptr,
		"**internal_test.MockStruct":                          &ptr,
		"github.com/ibllex/go-queue/internal_test.MockStruct": MockStruct{},
		"func(string) int":                                    MockFunc,
		"int":                                                 0,
		"int32":                                               int32(0),
		"float32":                                             float32(0.0),
		"float64":                                             0.0,
	}

	for name, value := range items {
		assert.Equal(t, name, internal.NameOf(value))
	}
}
