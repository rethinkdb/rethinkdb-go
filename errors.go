package gorethink

import (
	"bytes"
	"fmt"

	p "github.com/dancannon/gorethink/ql2"
)

func printCarrots(t RqlTerm, frames []*p.Frame) string {
	var frame *p.Frame
	if len(frames) > 1 {
		frame, frames = frames[0], frames[1:]
	} else if len(frames) == 1 {
		frame, frames = frames[0], []*p.Frame{}
	}

	for i, arg := range t.args {
		if frame.GetPos() == int64(i) {
			t.args[i] = RqlTerm{
				termType: p.Term_DATUM,
				data:     printCarrots(arg, frames),
			}
		}
	}

	for k, arg := range t.optArgs {
		if frame.GetOpt() == k {
			t.optArgs[k] = RqlTerm{
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

// Connection/Response errors
// ----------------------------------------------------------------------------
type rqlResponseError struct {
	response *p.Response
	term     RqlTerm
}

func (e rqlResponseError) Error() string {
	message, _ := deconstructDatum(e.response.GetResponse()[0], map[string]interface{}{})

	return fmt.Sprintf("gorethink: %s in: \n%s", message, e.term)
}

func (e rqlResponseError) String() string {
	return e.Error()
}

type RqlCompileError struct {
	rqlResponseError
}

type RqlRuntimeError struct {
	rqlResponseError
}

type RqlClientError struct {
	rqlResponseError
}

type RqlDriverError struct {
	message string
}

func (e RqlDriverError) Error() string {
	return fmt.Sprintf("gorethink: %s", e.message)
}

func (e RqlDriverError) String() string {
	return e.Error()
}

type RqlConnectionError struct {
	message string
}

func (e RqlConnectionError) Error() string {
	return fmt.Sprintf("gorethink: %s", e.message)
}

func (e RqlConnectionError) String() string {
	return e.Error()
}
