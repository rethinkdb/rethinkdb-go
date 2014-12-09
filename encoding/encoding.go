package encoding

import (
	"encoding"
	"reflect"
	"time"

	"github.com/dancannon/gorethink/types"
)

var (
	// type constants
	stringType   = reflect.TypeOf("")
	timeType     = reflect.TypeOf(new(time.Time)).Elem()
	geometryType = reflect.TypeOf(new(types.Geometry)).Elem()

	marshalerType     = reflect.TypeOf(new(Marshaler)).Elem()
	textMarshalerType = reflect.TypeOf(new(encoding.TextMarshaler)).Elem()
)

// Marshaler is the interface implemented by objects that
// can marshal themselves into a valid RQL psuedo-type.
type Marshaler interface {
	MarshalRQL() (interface{}, error)
}

// Unmarshaler is the interface implemented by objects
// that can unmarshal a psuedo-type object of themselves.
type Unmarshaler interface {
	UnmarshalRQL(interface{}) error
}

func init() {
	encoderCache.m = make(map[reflect.Type]encoderFunc)
	decoderCache.m = make(map[decoderCacheKey]decoderFunc)
}
