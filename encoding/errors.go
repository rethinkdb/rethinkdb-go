package encoding

import (
	"fmt"
	"reflect"
	"strings"
)

type MarshalerError struct {
	Type reflect.Type
	Err  error
}

func (e *MarshalerError) Error() string {
	return "rethinkdb: error calling MarshalRQL for type " + e.Type.String() + ": " + e.Err.Error()
}

type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "rethinkdb: UnmarshalRQL(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "rethinkdb: UnmarshalRQL(non-pointer " + e.Type.String() + ")"
	}
	return "rethinkdb: UnmarshalRQL(nil " + e.Type.String() + ")"
}

// An InvalidTypeError describes a value that was
// not appropriate for a value of a specific Go type.
type DecodeTypeError struct {
	DestType, SrcType reflect.Type
	Reason            string
}

func (e *DecodeTypeError) Error() string {
	if e.Reason != "" {
		return "rethinkdb: could not decode type " + e.SrcType.String() + " into Go value of type " + e.DestType.String() + ": " + e.Reason
	} else {
		return "rethinkdb: could not decode type " + e.SrcType.String() + " into Go value of type " + e.DestType.String()
	}
}

// An UnsupportedTypeError is returned by Marshal when attempting
// to encode an unsupported value type.
type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return "rethinkdb: unsupported type: " + e.Type.String()
}

// An UnsupportedTypeError is returned by Marshal when attempting
// to encode an unexpected value type.
type UnexpectedTypeError struct {
	DestType, SrcType reflect.Type
}

func (e *UnexpectedTypeError) Error() string {
	return "rethinkdb: expected type: " + e.DestType.String() + ", got " + e.SrcType.String()
}

type UnsupportedValueError struct {
	Value reflect.Value
	Str   string
}

func (e *UnsupportedValueError) Error() string {
	return "rethinkdb: unsupported value: " + e.Str
}

// Error implements the error interface and can represents multiple
// errors that occur in the course of a single decode.
type Error struct {
	Errors []string
}

func (e *Error) Error() string {
	points := make([]string, len(e.Errors))
	for i, err := range e.Errors {
		points[i] = fmt.Sprintf("* %s", err)
	}

	return fmt.Sprintf(
		"%d error(s) decoding:\n\n%s",
		len(e.Errors), strings.Join(points, "\n"))
}
