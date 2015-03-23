package gorethink

import (
	"time"

	p "github.com/dancannon/gorethink/ql2"
)

// A Session represents a connection to a RethinkDB cluster and should be used
// when executing queries.
type Session struct {
	hosts   []Host
	opts    *ConnectOpts
	cluster *Cluster
	closed  bool
}

// ConnectOpts is used to specify optional arguments when connecting to a cluster.
type ConnectOpts struct {
	Database string        `gorethink:"database,omitempty"`
	AuthKey  string        `gorethink:"authkey,omitempty"`
	Timeout  time.Duration `gorethink:"timeout,omitempty"`

	MaxIdle int `gorethink:"max_idle,omitempty"`
	MaxOpen int `gorethink:"max_open,omitempty"`

	// Below options are for cluster discovery, please note there is a high
	// probability of these changing as the API is still being worked on.

	DiscoverHosts       bool          `gorethink:"discover_hosts,omitempty"`
	NodeRefreshInterval time.Duration `gorethink:"node_refresh_interval,omitempty"`
}

func (o *ConnectOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Connect creates a new database session.
//
// 	session, err := r.Connect("localhost:28015")
func Connect(address string) (*Session, error) {
	return ConnectWithOpts(ConnectOpts{}, address)
}

// Connect creates a new database session with the given options.
//
// Supported arguments include Database, Timeout, Authkey. Pool
// options include MaxIdle, MaxOpen.
//
// By default maxIdle and maxOpen are set to 1: passing values greater
// than the default (e.g. MaxIdle: "10", MaxOpen: "20") will provide a
// pool of re-usable connections.
//
// Basic connection example:
//
// 	session, err := r.ConnectWithOpts(r.ConnectOpts{
// 		Database: "test",
// 		AuthKey:  "14daak1cad13dj",
// 	}, "localhost:28015")
func ConnectWithOpts(opts ConnectOpts, address string) (*Session, error) {
	return ConnectClusterWithOpts(opts, address)
}

// ConnectCluster creates a new database session using the given hosts as seeds
// to discover the rest of the cluster.
//
// 	session, err := r.Connect("localhost:28015", "localhost:28016")
func ConnectCluster(addresses ...string) (*Session, error) {
	// We assume that if using this function the user wants host discovery
	opts := ConnectOpts{
		DiscoverHosts: true,
	}

	return ConnectClusterWithOpts(opts, addresses...)
}

// ConnectClusterWithOpts creates a new database session using the given hosts as seeds
// to discover the rest of the cluster.
// Supported arguments include Database, Timeout, Authkey. Pool
// options include MaxIdle, MaxOpen.
//
// By default maxIdle and maxOpen are set to 1: passing values greater
// than the default (e.g. MaxIdle: "10", MaxOpen: "20") will provide a
// pool of re-usable connections.
//
// Basic connection example:
//
// 	session, err := r.ConnectWithOpts(r.ConnectOpts{
// 		Database: "test",
// 		AuthKey:  "14daak1cad13dj",
// 	}, "localhost:28015", "localhost:28016")
func ConnectClusterWithOpts(opts ConnectOpts, addresses ...string) (*Session, error) {
	hosts := make([]Host, len(addresses))
	for i, address := range addresses {
		hostname, port := splitAddress(address)
		hosts[i] = NewHost(hostname, port)
	}
	if len(hosts) <= 0 {
		return nil, ErrNoHosts
	}

	// Connect
	s := &Session{
		hosts: hosts,
		opts:  &opts,
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

	s.cluster, err = NewCluster(s.hosts, s.opts)
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

	if s.cluster != nil {
		s.cluster.Close()
	}
	s.cluster = nil
	s.closed = true

	return nil
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
func (s *Session) SetMaxIdleConns(n int) {
	s.opts.MaxIdle = n
	s.cluster.SetMaxIdleConns(n)
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
func (s *Session) SetMaxOpenConns(n int) {
	s.opts.MaxOpen = n
	s.cluster.SetMaxOpenConns(n)
}

// NoReplyWait ensures that previous queries with the noreply flag have been
// processed by the server. Note that this guarantee only applies to queries
// run on the given connection
func (s *Session) NoReplyWait() error {
	return s.cluster.Exec(Query{
		Type: p.Query_NOREPLY_WAIT,
	})
}

// Use changes the default database used
func (s *Session) Use(database string) {
	s.opts.Database = database
}

// Query executes a ReQL query using the session to connect to the database
func (s *Session) Query(q Query) (*Cursor, error) {
	return s.cluster.Query(q)
}

// Exec executes a ReQL query using the session to connect to the database
func (s *Session) Exec(q Query) error {
	return s.cluster.Exec(q)
}

// SetHosts resets the hosts used when connecting to the RethinkDB cluster
func (s *Session) SetHosts(hosts []Host) {
	s.hosts = hosts
}

func (s *Session) newQuery(t Term, opts map[string]interface{}) Query {
	return newQuery(t, opts, s.opts)
}
