package gorethink

import (
	"fmt"
	p "github.com/dancannon/gorethink/ql2"
)

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
