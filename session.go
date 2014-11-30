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
	Opts ConnectOpts

	// Response cache, used for batched responses
	sync.Mutex
	closed bool
	token  int64

	pool ConnectionPool
}

type ConnectOpts struct {
	Address  string `gorethink:"address,omitempty"`
	Database string `gorethink:"database,omitempty"`
	AuthKey  string `gorethink:"authkey,omitempty"`

	MinCap       int           `gorethink:"min_cap,omitempty"`
	MaxCap       int           `gorethink:"max_cap,omitempty"`
	Timeout      time.Duration `gorethink:"timeout,omitempty"`
	IdleTimeout  time.Duration `gorethink:"idle_timeout,omitempty"`
	WaitRetry    time.Duration `gorethink:"wait_retry,omitempty"`
	MaxWaitRetry time.Duration `gorethink:"max_wait_retry,omitempty"`
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
	// Set defaults
	if opts.MinCap == 0 {
		opts.MinCap = 1
	}
	if opts.MaxCap == 0 {
		opts.MaxCap = 1
	}
	if opts.Timeout == 0 {
		opts.Timeout = time.Second
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = time.Hour
	}
	if opts.WaitRetry == 0 {
		opts.WaitRetry = 1
	}
	if opts.MaxWaitRetry == 0 {
		opts.MaxWaitRetry = 1
	}

	// Connect
	s := &Session{
		Opts: opts,
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
	s.Opts.Database = database
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
