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
	maxCap int

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
	if maxCap, ok := args["max_active"]; ok {
		s.maxCap = int(maxCap.(int64))
	} else {
		s.maxCap = 30
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

	if s.pool != nil {
		s.pool = NewSimplePool(s)
	}
	if s.pool == nil {

		s.closed = false
		s.pool = NewSimplePool(s)

		// See if there are any connections in the pool
		if s.pool.Size() == 0 {
			s.pool.Close()
			return ErrNoConnections
		}
	}

	return nil
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
	s.pool = nil
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

// continueQuery continues a previously run query.
// This is needed if a response is batched.
func (s *Session) continueQuery(cursor *Cursor) error {
	cursor.mu.Lock()
	conn := cursor.conn
	cursor.mu.Unlock()

	return conn.ContinueQuery(cursor.token)
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

func (s *Session) getConn() (*Connection, error) {
	conn := s.pool.Get()
	if conn == nil {
		return nil, RqlConnectionError{"No connections available"}
	}

	return s.pool.Get(), nil
}
