package gorethink

import (
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
	opts ConnectOpts
	pool *Pool

	// Response cache, used for batched responses
	sync.Mutex
	closed bool
	token  int64
}

type ConnectOpts struct {
	Address  string        `gorethink:"address,omitempty"`
	Database string        `gorethink:"database,omitempty"`
	AuthKey  string        `gorethink:"authkey,omitempty"`
	Timeout  time.Duration `gorethink:"timeout,omitempty"`

	MaxIdle     int           `gorethink:"max_idle,omitempty"`
	MaxOpen     int           `gorethink:"max_open,omitempty"`
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
func Connect(opts ConnectOpts) (*Session, error) {
	// Connect
	s := &Session{
		opts: opts,
	}
	err := s.Reconnect()
	if err != nil {
		return nil, err
	}

	return s, nil
}

type CloseOpts struct {
	NoReplyWait bool `gorethink:"noreplyWait,omitempty"`
}

func (o *CloseOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Reconnect closes and re-opens a session.
func (s *Session) Reconnect(optArgs ...CloseOpts) error {
	var err error

	if err = s.Close(optArgs...); err != nil {
		return err
	}

	setup := s.pool == nil

	s.pool, err = NewPool(&s.opts)
	if err != nil {
		return err
	}

	if setup {
		// Check if we can get a connection
		c, err := s.pool.GetConn()
		if err != nil {
			return err
		}
		s.pool.PutConn(c, nil, false)
	}

	s.closed = false

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

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
func (s *Session) SetMaxIdleConns(n int) {
	s.pool.SetMaxIdleConns(n)
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
func (s *Session) SetMaxOpenConns(n int) {
	s.pool.SetMaxOpenConns(n)
}

// noreplyWait ensures that previous queries with the noreply flag have been
// processed by the server. Note that this guarantee only applies to queries
// run on the given connection
func (s *Session) NoReplyWait() {
	s.noreplyWaitQuery()
}

// Use changes the default database used
func (s *Session) Use(database string) {
	s.opts.Database = database
}

// startQuery creates a query from the term given and sends it to the server.
// The result from the server is returned as a cursor
func (s *Session) startQuery(t Term, opts map[string]interface{}) (*Cursor, error) {
	conn, err := s.GetConn()
	if err != nil {
		return nil, err
	}

	cur, err := conn.StartQuery(t, opts)

	return cur, err
}

// noreplyWaitQuery sends the NOREPLY_WAIT query to the server.
func (s *Session) noreplyWaitQuery() error {
	conn, err := s.GetConn()
	if err != nil {
		return err
	}

	return conn.NoReplyWait()
}

func (s *Session) GetConn() (*Connection, error) {
	return s.pool.GetConn()
}
