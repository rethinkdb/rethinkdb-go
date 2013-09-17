package rethinkgo

import (
	"fmt"
	p "github.com/dancannon/gorethink/ql2"
)

// Connection/Response errors
// ----------------------------------------------------------------------------

type RqlCompileError struct {
	response *p.Response
}

func (e RqlCompileError) Error() string {
	return "RqlCompileError"
}

type RqlRuntimeError struct {
	response *p.Response
}

func (e RqlRuntimeError) Error() string {
	return "RqlRuntimeError"
}

type RqlClientError struct {
	response *p.Response
}

func (e RqlClientError) Error() string {
	return "RqlClientError"
}

type RqlDriverError struct {
	message string
}

func (e RqlDriverError) Error() string {
	return fmt.Sprintf("gorethink: %s", e.message)
}
