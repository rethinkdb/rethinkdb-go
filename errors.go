package gorethink

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	p "github.com/dancannon/gorethink/ql2"
)

var (
	ErrNoHosts              = errors.New("no hosts provided")
	ErrNoConnectionsStarted = errors.New("no connections were made when creating the session")
	ErrHostQueryFailed      = errors.New("unable to populate hosts")
	ErrInvalidNode          = errors.New("invalid node")
	ErrClusterClosed        = errors.New("cluster closed")

	ErrNoConnections    = errors.New("gorethink: no connections were available")
	ErrConnectionClosed = errors.New("gorethink: the connection is closed")

	ErrBusyBuffer = errors.New("Busy buffer")
)

func printCarrots(t Term, frames []*p.Frame) string {
	var frame *p.Frame
	if len(frames) > 1 {
		frame, frames = frames[0], frames[1:]
	} else if len(frames) == 1 {
		frame, frames = frames[0], []*p.Frame{}
	}

	for i, arg := range t.args {
		if frame.GetPos() == int64(i) {
			t.args[i] = Term{
				termType: p.Term_DATUM,
				data:     printCarrots(arg, frames),
			}
		}
	}

	for k, arg := range t.optArgs {
		if frame.GetOpt() == k {
			t.optArgs[k] = Term{
				termType: p.Term_DATUM,
				data:     printCarrots(arg, frames),
			}
		}
	}

	b := &bytes.Buffer{}
	for _, c := range t.String() {
		if c != '^' {
			b.WriteString(" ")
		} else {
			b.WriteString("^")
		}
	}

	return b.String()
}

// Error constants
var ErrEmptyResult = errors.New("The result does not contain any more rows")

// Connection/Response errors

type rqlResponseError struct {
	response *Response
	term     *Term
}

func (e rqlResponseError) Error() string {
	var err = "An error occurred"
	if e.response != nil {
		json.Unmarshal(e.response.Responses[0], &err)
	}

	if e.term == nil {
		return fmt.Sprintf("gorethink: %s", err)
	}

	return fmt.Sprintf("gorethink: %s in: \n%s", err, e.term.String())

}

func (e rqlResponseError) String() string {
	return e.Error()
}

type RQLCompileError struct {
	rqlResponseError
}

type RQLRuntimeError struct {
	rqlResponseError
}

type RQLClientError struct {
	rqlResponseError
}

type RQLDriverError struct {
	message string
}

func (e RQLDriverError) Error() string {
	return fmt.Sprintf("gorethink: %s", e.message)
}

func (e RQLDriverError) String() string {
	return e.Error()
}

type RQLConnectionError struct {
	message string
}

func (e RQLConnectionError) Error() string {
	return fmt.Sprintf("gorethink: %s", e.message)
}

func (e RQLConnectionError) String() string {
	return e.Error()
}
