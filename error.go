package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

// ErrBadQuery indicates that the server has told us we have constructed an
// invalid query.
//
// Example usage:
//
//   err := r.Table("heroes").ArrayToStream().ArrayToStream().Run(session).Err()
type ErrBadQuery struct {
	response *p.Response
}

func (e ErrBadQuery) Error() string {
	return "Server could not make sense of our query"
}

// ErrRuntime indicates that the server has encountered an error while
// trying to execute our query.
//
// Example usage:
//
//   err := r.Table("table_that_doesnt_exist").Run(session).Err()
//   err := r.RuntimeError("error time!").Run(session).Err()
type ErrRuntime struct {
	response *p.Response
}

func (e ErrRuntime) Error() string {
	return "Server could not execute our query"
}

// ErrBrokenClient means the server believes there's a bug in the client
// library, for instance a malformed protocol buffer.
type ErrBrokenClient struct {
	response *p.Response
}

func (e ErrBrokenClient) Error() string {
	return "Whoops, looks like there's a bug in this client library, please report it at https://github.com/christopherhesse/rethinkgo/issues/new"
}

// ErrWrongResponseType is returned when .Exec(), .One(). or .All() have
// been used, but the expected response type does not match the type we got
// from the server.
//
// Example usage:
//
//  var row []interface{}
//  err := r.Table("heroes").Get("Archangel", "name").Run(session).All(&row)
type ErrWrongResponseType struct {
	response *p.Response
}

func (e ErrWrongResponseType) Error() string {
	return "rethinkdb: Wrong response type, you may have used the wrong one of: .Exec(), .One(), .All()"
}

// ----------------------------------------------------------------------------
// Result/Rows errors

// ErrNoRow indicates that rows.Scan has been called before rows.Next
type ErrRowsClosed struct {
}

func (e ErrRowsClosed) Error() string {
	return "rethinkdb: Rows closed"
}

// ErrNoRows indicates that the server has not returned any results but row.Scan
// has been called.
type ErrNoRows struct {
}

func (e ErrBadQuery) Error() string {
	return "rethinkdb: no rows in the result set"
}

// ErrNoRow indicates that rows.Scan has been called before rows.Next
type ErrNoCurrRow struct {
}

func (e ErrNoCurrRow) Error() string {
	return "rethinkdb: Scan called without calling Next"
}
