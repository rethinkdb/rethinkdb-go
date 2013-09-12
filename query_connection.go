package rethinkgo

import (
	"time"
)

// Session represents a connection to a server, use it to run queries against a
// database, with either sess.Run(query) or query.Run(session).  Do not share a
// session between goroutines, create a new one for each goroutine.
type Session struct {
	// current query identifier, just needs to be unique for each query, so we
	// can match queries with responses, e.g. 4782371
	token int64
	// address of server, e.g. "localhost:28015"
	address string
	// database to use if no database is specified in query, e.g. "test"
	database string
	// maximum duration of a single query
	timeout time.Duration
	// authorization key for servers configured to check this
	authkey string

	// the format time values should be returned as
	timeFormat string

	debug bool

	// conn   *connection
	closed bool
}

func NewSession(args map[string]interface{}) *Session {
	s := &Session{}

	if token, ok := args["token"]; ok {
		s.token = token.(int64)
	}
	if address, ok := args["address"]; ok {
		s.address = address.(string)
	}
	if database, ok := args["database"]; ok {
		s.database = database.(string)
	}
	if timeout, ok := args["timeout"]; ok {
		s.timeout = timeout.(time.Duration)
	}
	if authkey, ok := args["authkey"]; ok {
		s.authkey = authkey.(string)
	}
	if debug, ok := args["debug"]; ok {
		s.debug = debug.(bool)
	}

	return s
}

// Connect creates a new database session.
//
// NOTE: You probably should not share sessions between goroutines.
//
// Example usage:
//
//  session, err := r.Connect(map[string]interface{} {
//      "address": localhost:28015"
//      "database": "test"
//  })
func Connect(args map[string]interface{}) (*Session, error) {
	var err error

	s := NewSession(args)
	// s.conn, err = serverConnect(s.address, s.authkey)
	return s, err
}

// Reconnect closes and re-opens a session.
//
// Example usage:
//
//  err := session.Reconnect()
func (s *Session) Reconnect() error {
	var err error
	if err = s.Close(); err != nil {
		return err
	}

	s.closed = false
	// s.conn, err = serverConnect(s.address, s.authkey)
	return err
}

// Close closes the session, freeing any associated resources.
//
// Example usage:
//
//  err := session.Close()
func (s *Session) Close() error {
	if s.closed {
		return nil
	}

	// err := s.conn.Close()
	var err error = nil
	s.closed = true

	return err
}

// func (s *Session) Context() context {
// 	return context{databaseName: s.database, atomic: true}
// }

// func (s *Session) Run(t TermBase) *Rows {
// 	queryProto, err := s.Context().buildProtobuf(query)
// 	if err != nil {
// 		return &Rows{lasterr: err}
// 	}

// 	queryProto.Token = proto.Int64(s.getToken())
// 	buffer, responseType, err := s.conn.executeQuery(queryProto, s.timeout)
// 	if err != nil {
// 		return &Rows{lasterr: err}
// 	}

// 	switch responseType {
// 	case p.Response_SUCCESS_ATOM:
// 		// single document (or json) response, return an iterator anyway for
// 		// consistency of types
// 		return &Rows{
// 			buffer:       buffer,
// 			complete:     true,
// 			responseType: responseType,
// 		}
// 	case p.Response_SUCCESS_PARTIAL:
// 		// beginning of stream of rows, there are more results available from the
// 		// server than the ones we just received, so save the session we used in
// 		// case the user wants more
// 		return &Rows{
// 			session:      s,
// 			buffer:       buffer,
// 			token:        queryProto.GetToken(),
// 			responseType: responseType,
// 		}
// 	case p.Response_SUCCESS_SEQUENCE:
// 		// end of a stream of rows, since we got this on the initial query this means
// 		// that we got a stream response, but the number of results was less than the
// 		// number required to break the response into chunks. we can just return all
// 		// the results in one go, as this is the only response
// 		return &Rows{
// 			buffer:       buffer,
// 			complete:     true,
// 			responseType: responseType,
// 		}
// 	}
// 	return &Rows{lasterr: fmt.Errorf("rethinkdb: Unexpected response type from server: %v", responseType)}
// }
