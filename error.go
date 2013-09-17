package rethinkgo

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
