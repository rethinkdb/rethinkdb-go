package gorethink

import (
	"fmt"
	"sync"
	"time"

	p "github.com/dancannon/gorethink/ql2"
)

type Query struct {
	Type       p.Query_QueryType
	Token      int64
	Term       *Term
	GlobalOpts map[string]interface{}
}

func (q *Query) build() []interface{} {
	res := []interface{}{q.Type}
	if q.Term != nil {
		res = append(res, q.Term.build())
	}

	if len(q.GlobalOpts) > 0 {
		res = append(res, q.GlobalOpts)
	}

	return res
}

type Session struct {
	address    string
	database   string
	timeout    time.Duration
	authkey    string
	timeFormat string

	// Pool configuration options
	initialCap  int
	maxCap      int
	idleTimeout time.Duration

	token int64

	// Response cache, used for batched responses
	sync.Mutex
	closed bool

	pool ConnectionPool
}

func newSession(args map[string]interface{}) *Session {
	s := &Session{}

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

	// Pool configuration options
	if initialCap, ok := args["initial_cap"]; ok {
		s.initialCap = int(initialCap.(int64))
	} else {
		s.initialCap = 5
	}
	if maxCap, ok := args["max_cap"]; ok {
		s.maxCap = int(maxCap.(int64))
	} else {
		s.maxCap = 30
	}
	if idleTimeout, ok := args["idle_timeout"]; ok {
		s.idleTimeout = idleTimeout.(time.Duration)
	} else {
		s.idleTimeout = 10 * time.Second
	}

	return s
}

type ConnectOpts struct {
	Address     string        `gorethink:"address,omitempty"`
	Database    string        `gorethink:"database,omitempty"`
	Timeout     time.Duration `gorethink:"timeout,omitempty"`
	AuthKey     string        `gorethink:"authkey,omitempty"`
	InitialCap  int           `gorethink:"initial_cap,omitempty"`
	MaxCap      int           `gorethink:"max_cap,omitempty"`
	IdleTimeout time.Duration `gorethink:"idle_timeout,omitempty"`
}

func (o *ConnectOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Connect creates a new database session.
//
// Supported arguments include address, database, timeout, authkey,
// and timeFormat. Pool options include maxIdle, maxActive and idleTimeout.
//
// By default maxIdle and maxActive are set to 1: passing values greater
// than the default (e.g. maxIdle: "10", maxActive: "20") will provide a
// pool of re-usable connections.
//
// Basic connection example:
//
//	var session *r.Session
// 	session, err := r.Connect(r.ConnectOpts{
// 		Address:  "localhost:28015",
// 		Database: "test",
// 		AuthKey:  "14daak1cad13dj",
// 	})
func Connect(args ConnectOpts) (*Session, error) {
	s := newSession(args.toMap())
	err := s.Reconnect()

	return s, err
}

type CloseOpts struct {
	NoReplyWait bool `gorethink:"noreplyWait,omitempty"`
}

func (o *CloseOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Reconnect closes and re-opens a session.
func (s *Session) Reconnect(optArgs ...CloseOpts) error {
	if err := s.Close(optArgs...); err != nil {
		return err
	}

	s.closed = false
	if s.pool == nil {
		s.pool = NewSimplePool(s)
	}

	// Check the connection
	_, err := s.getConn()

	return err
}

// Close closes the session
func (s *Session) Close(optArgs ...CloseOpts) error {
	if s.closed {
		return nil
	}

	if len(optArgs) >= 1 {
		if optArgs[0].NoReplyWait {
			s.NoReplyWait()
		}
	}

	if s.pool != nil {
		s.pool.Close()
	}
	s.closed = true

	return nil
}

// noreplyWait ensures that previous queries with the noreply flag have been
// processed by the server. Note that this guarantee only applies to queries
// run on the given connection
func (s *Session) NoReplyWait() {
	s.noreplyWaitQuery()
}

// Use changes the default database used
func (s *Session) Use(database string) {
	s.database = database
}

// SetTimeout causes any future queries that are run on this session to timeout
// after the given duration, returning a timeout error.  Set to zero to disable.
func (s *Session) SetTimeout(timeout time.Duration) {
	s.timeout = timeout
}

// startQuery creates a query from the term given and sends it to the server.
// The result from the server is returned as a cursor
func (s *Session) startQuery(t Term, opts map[string]interface{}) (*Cursor, error) {
	conn, err := s.getConn()
	if err != nil {
		return nil, err
	}

	cur, err := conn.StartQuery(t, opts)

	return cur, err
}

// func (s *Session) handleBatchResponse(cursor *Cursor, response *Response) {
// 	cursor.extend(response)

// 	s.Lock()
// cursor.outstandingRequests--

// if response.Type != p.Response_SUCCESS_PARTIAL &&
// 	response.Type != p.Response_SUCCESS_FEED &&
// 	cursor.outstandingRequests == 0 {
// 	delete(s.cache, response.Token)
// }
// 	s.Unlock()
// }

// continueQuery continues a previously run query.
// This is needed if a response is batched.
func (s *Session) continueQuery(cursor *Cursor) error {
	cursor.mu.Lock()
	conn := cursor.conn
	cursor.mu.Unlock()

	return conn.ContinueQuery(cursor.token)
}

// asyncContinueQuery asynchronously continues a previously run query.
// This is needed if a response is batched.
func (s *Session) asyncContinueQuery(cursor *Cursor) error {
	cursor.mu.Lock()
	if cursor.outstandingRequests != 0 {
		cursor.mu.Unlock()
		return nil
	}
	cursor.outstandingRequests = 1
	conn := cursor.conn
	cursor.mu.Unlock()

	return conn.AsyncContinueQuery(cursor.token)
}

// stopQuery sends closes a query by sending Query_STOP to the server.
func (s *Session) stopQuery(cursor *Cursor) error {
	cursor.mu.Lock()
	cursor.outstandingRequests++
	conn := cursor.conn
	cursor.mu.Unlock()

	return conn.StopQuery(cursor.token)
}

// noreplyWaitQuery sends the NOREPLY_WAIT query to the server.
func (s *Session) noreplyWaitQuery() error {
	conn, err := s.getConn()
	if err != nil {
		return err
	}

	return conn.NoReplyWait()
}

var tmpConn *Connection

func (s *Session) getConn() (*Connection, error) {
	return s.pool.Get(), nil
}
