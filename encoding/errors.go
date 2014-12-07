package encoding

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type MarshalerError struct {
	Type reflect.Type
	Err  error
}

func (e *MarshalerError) Error() string {
	return "gorethink: error calling MarshalRQL for type " + e.Type.String() + ": " + e.Err.Error()
}

// An UnsupportedTypeError is returned by Marshal when attempting
// to encode an unsupported value type.
type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return "gorethink: unsupported type: " + e.Type.String()
}

// An UnsupportedTypeError is returned by Marshal when attempting
// to encode an unexpected value type.
type UnexpectedTypeError struct {
	ExpectedType, ActualType reflect.Type
}

func (e *UnexpectedTypeError) Error() string {
	return "gorethink: expected type: " + e.ExpectedType.String() + ", got " + e.ActualType.String()
}

// An UnsupportedTypeError is returned by Marshal when attempting
// to encode an unconvertible value type.
type UnconvertibleTypeError struct {
	ExpectedType, ActualType reflect.Type
}

func (e *UnconvertibleTypeError) Error() string {
	return "gorethink: expected type: " + e.ExpectedType.String() + ", got unconvertible" + e.ActualType.String()
}

type UnsupportedValueError struct {
	Value reflect.Value
	Str   string
}

func (e *UnsupportedValueError) Error() string {
	return "gorethink: unsupported value: " + e.Str
}

// An InvalidTypeError describes a value that was
// not appropriate for a value of a specific Go type.
type InvalidTypeError struct {
	ExpectedType, ActualType reflect.Type
	Reason                   error
}

func (e *InvalidTypeError) Error() string {
	return "gorethink: cannot decode " + e.ActualType.String() + " into Go value of type " + e.ExpectedType.String() + ": " + e.Reason.Error()
}

// An DecodeFieldError describes a object key that
// led to an unexported (and therefore unwritable) struct field.
// (No longer used; kept for compatibility.)
type DecodeFieldError struct {
	Key   string
	Type  reflect.Type
	Field reflect.StructField
}

func (e *DecodeFieldError) Error() string {
	return "gorethink: cannot decode object key " + strconv.Quote(e.Key) + " into unexported field " + e.Field.Name + " of type " + e.Type.String()
}

// An InvalidDecodeError describes an invalid argument passed to Decode.
// (The argument to Decode must be a non-nil pointer.)
type InvalidDecodeError struct {
	Value reflect.Value
}

func (e *InvalidDecodeError) Error() string {
	if e.Value.Kind() != reflect.Ptr {
		return "gorethink: Decode error (" + e.Value.Type().String() + " must be a pointer)"
	}
	if !e.Value.CanAddr() {
		return "gorethink: Decode error (" + e.Value.Type().String() + " must be addressable)"
	}
	return "gorethink: Decode error"
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

func appendErrors(errors []string, err error) []string {
	switch e := err.(type) {
	case *Error:
		return append(errors, e.Errors...)
	default:
		return append(errors, e.Error())
	}
}
