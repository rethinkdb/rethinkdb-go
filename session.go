package gorethink

import (
	"code.google.com/p/goprotobuf/proto"
	p "github.com/dancannon/gorethink/ql2"
	"sync/atomic"
	"time"
)

type Session struct {
	token      int64
	address    string
	database   string
	timeout    time.Duration
	authkey    string
	timeFormat string

	// Pool configuration options
	maxIdle     int
	maxActive   int
	idleTimeout time.Duration

	closed bool

	pool *Pool
}

func newSession(args map[string]interface{}) *Session {
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

	// Pool configuration options
	if maxIdle, ok := args["maxIdle"]; ok {
		s.maxIdle = maxIdle.(int)
	} else {
		s.maxIdle = 1
	}
	if maxActive, ok := args["maxActive"]; ok {
		s.maxActive = maxActive.(int)
	} else {
		s.maxActive = 1
	}
	if idleTimeout, ok := args["idleTimeout"]; ok {
		s.idleTimeout = idleTimeout.(time.Duration)
	}

	return s
}

// Connect creates a new database session.
func Connect(args map[string]interface{}) (*Session, error) {
	s := newSession(args)
	err := s.Reconnect()

	return s, err
}

// Reconnect closes and re-opens a session.
func (s *Session) Reconnect() error {
	if err := s.Close(); err != nil {
		return err
	}

	s.closed = false
	if s.pool == nil {
		s.pool = &Pool{
			Session:     s,
			MaxIdle:     s.maxIdle,
			MaxActive:   s.maxActive,
			IdleTimeout: s.idleTimeout,
		}
	}
	return nil
}

// Close closes the session
func (s *Session) Close() error {
	if s.closed {
		return nil
	}

	var err error
	if s.pool != nil {
		err = s.pool.Close()
	}
	s.closed = true

	return err
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

// getToken generates the next query token, used to number requests and match
// responses with requests.
func (s *Session) nextToken() int64 {
	return atomic.AddInt64(&s.token, 1)
}

// startQuery creates a query from the term given and sends it to the server.
// The result from the server is returned as ResultRows
func (s *Session) startQuery(t RqlTerm, opts map[string]interface{}) (*ResultRows, error) {
	token := s.nextToken()

	// Build query tree
	pt := t.build()

	// Build global options
	globalOpts := []*p.Query_AssocPair{}
	for k, v := range opts {
		if k == "db" {
			globalOpts = append(globalOpts, &p.Query_AssocPair{
				Key: proto.String("db"),
				Val: Db(v).build(),
			})
		} else if k == "use_outdated" {
			globalOpts = append(globalOpts, &p.Query_AssocPair{
				Key: proto.String("use_outdated"),
				Val: Expr(v).build(),
			})
		} else if k == "noreply" {
			globalOpts = append(globalOpts, &p.Query_AssocPair{
				Key: proto.String("noreply"),
				Val: Expr(v).build(),
			})
		}
	}
	// If no DB option was set default to the value set in the connection
	if _, ok := opts["db"]; !ok {
		globalOpts = append(globalOpts, &p.Query_AssocPair{
			Key: proto.String("db"),
			Val: Db(s.database).build(),
		})
	}

	// Construct query
	query := &p.Query{
		Type:          p.Query_START.Enum(),
		Token:         proto.Int64(token),
		Query:         pt,
		GlobalOptargs: globalOpts,
	}

	conn := s.pool.Get()
	defer conn.Close()

	return conn.SendQuery(s, query, t, opts)
}

// continueQuery continues a previously run query.
func (s *Session) continueQuery(q *p.Query, t RqlTerm, opts map[string]interface{}) (*ResultRows, error) {
	nq := &p.Query{
		Type:  p.Query_CONTINUE.Enum(),
		Token: q.Token,
	}

	conn := s.pool.Get()
	defer conn.Close()

	return conn.SendQuery(s, nq, t, opts)
}

// stopQuery sends closes a query by sending Query_STOP to the server.
func (s *Session) stopQuery(q *p.Query, t RqlTerm, opts map[string]interface{}) (*ResultRows, error) {
	nq := &p.Query{
		Type:  p.Query_STOP.Enum(),
		Token: q.Token,
	}

	conn := s.pool.Get()
	defer conn.Close()

	return conn.SendQuery(s, nq, t, opts)
}
