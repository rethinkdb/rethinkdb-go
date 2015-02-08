package gorethink

import (
	"sync"
	"time"

	p "github.com/dancannon/gorethink/ql2"
)

type executor interface {
	Exec(q Query) error
	Query(q Query) (*Cursor, error)

	newQuery(t Term, opts map[string]interface{}) Query
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

	MaxIdle int `gorethink:"max_idle,omitempty"`
	MaxOpen int `gorethink:"max_open,omitempty"`
}

func (o *ConnectOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Connect creates a new database session.
//
// Supported arguments include Address, Database, Timeout, Authkey. Pool
// options include MaxIdle, MaxOpen.
//
// By default maxIdle and maxOpen are set to 1: passing values greater
// than the default (e.g. MaxIdle: "10", MaxOpen: "20") will provide a
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

	s.pool, err = NewPool(Host{
		Address:  s.opts.Address,
		Database: s.opts.Database,
		AuthKey:  s.opts.AuthKey,
		Timeout:  s.opts.Timeout,
	}, s.opts.MaxIdle, s.opts.MaxOpen)
	if err != nil {
		return err
	}

	// Ping connection to check it is valid
	err = s.pool.Ping()
	if err != nil {
		return err
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

// NoReplyWait ensures that previous queries with the noreply flag have been
// processed by the server. Note that this guarantee only applies to queries
// run on the given connection
func (s *Session) NoReplyWait() error {
	return s.pool.Exec(Query{
		Type: p.Query_NOREPLY_WAIT,
	})
}

// Use changes the default database used
func (s *Session) Use(database string) {
	s.opts.Database = database
}

func (s *Session) Query(q Query) (*Cursor, error) {
	return s.pool.Query(q)
}

func (s *Session) Exec(q Query) error {
	return s.pool.Exec(q)
}

func (s *Session) newQuery(t Term, opts map[string]interface{}) Query {
	queryOpts := map[string]interface{}{}
	for k, v := range opts {
		queryOpts[k] = Expr(v).build()
	}
	if s.opts.Database != "" {
		queryOpts["db"] = Db(s.opts.Database).build()
	}

	// Construct query
	return Query{
		Type: p.Query_START,
		Term: &t,
		Opts: queryOpts,
	}
}
