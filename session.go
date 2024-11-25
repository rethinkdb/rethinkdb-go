package rethinkdb

import (
	"crypto/tls"
	"log/slog"
	"sync"
	"time"

	"context"

	p "gopkg.in/rethinkdb/rethinkdb-go.v6/ql2"
)

// A Session represents a connection to a RethinkDB cluster and should be used
// when executing queries.
type Session struct {
	hosts []Host
	opts  *ConnectOpts

	mu      sync.RWMutex
	cluster *Cluster
	closed  bool
}

// ConnectOpts is used to specify optional arguments when connecting to a cluster.
type ConnectOpts struct {
	// Address holds the address of the server initially used when creating the
	// session. Only used if Addresses is empty
	Address string `rethinkdb:"address,omitempty" json:"address,omitempty"`
	// Addresses holds the addresses of the servers initially used when creating
	// the session.
	Addresses []string `rethinkdb:"addresses,omitempty" json:"addresses,omitempty"`
	// Database is the default database name used when executing queries, this
	// value is only used if the query does not contain any DB term
	Database string `rethinkdb:"database,omitempty" json:"database,omitempty"`
	// Username holds the username used for authentication, if blank (and the v1
	// handshake protocol is being used) then the admin user is used
	Username string `rethinkdb:"username,omitempty" json:"username,omitempty"`
	// Password holds the password used for authentication (only used when using
	// the v1 handshake protocol)
	Password string `rethinkdb:"password,omitempty" json:"password,omitempty"`
	// AuthKey is used for authentication when using the v0.4 handshake protocol
	// This field is no deprecated
	AuthKey string `rethinkdb:"authkey,omitempty" json:"authkey,omitempty"`
	// Timeout is the time the driver waits when creating new connections, to
	// configure the timeout used when executing queries use WriteTimeout and
	// ReadTimeout
	Timeout time.Duration `rethinkdb:"timeout,omitempty" json:"timeout,omitempty"`
	// WriteTimeout is the amount of time the driver will wait when sending the
	// query to the server
	// Deprecated: use RunOpts.Context instead
	WriteTimeout time.Duration `rethinkdb:"write_timeout,omitempty" json:"write_timeout,omitempty"`
	// ReadTimeout is the amount of time the driver will wait for a response from
	// the server when executing queries.
	// Deprecated: use RunOpts.Context instead
	ReadTimeout time.Duration `rethinkdb:"read_timeout,omitempty" json:"read_timeout,omitempty"`
	// KeepAlivePeriod is the keep alive period used by the connection, by default
	// this is 30s. It is not possible to disable keep alive messages
	KeepAlivePeriod time.Duration `rethinkdb:"keep_alive_timeout,omitempty" json:"keep_alive_timeout,omitempty"`
	// TLSConfig holds the TLS configuration and can be used when connecting
	// to a RethinkDB server protected by SSL
	TLSConfig *tls.Config `rethinkdb:"tlsconfig,omitempty" json:"tlsconfig,omitempty"`
	// HandshakeVersion is used to specify which handshake version should be
	// used, this currently defaults to v1 which is used by RethinkDB 2.3 and
	// later. If you are using an older version then you can set the handshake
	// version to 0.4
	HandshakeVersion HandshakeVersion `rethinkdb:"handshake_version,omitempty" json:"handshake_version,omitempty"`
	// UseJSONNumber indicates whether the cursors running in this session should
	// use json.Number instead of float64 while unmarshalling documents with
	// interface{}. The default is `false`.
	UseJSONNumber bool `json:"use_json_number,omitempty"`
	// NumRetries is the number of times a query is retried if a connection
	// error is detected, queries are not retried if RethinkDB returns a
	// runtime error.
	// Default is 3.
	NumRetries int `json:"num_retries,omitempty"`

	// InitialCap is used by the internal connection pool and is used to
	// configure how many connections are created for each host when the
	// session is created. If zero then no connections are created until
	// the first query is executed.
	InitialCap int `rethinkdb:"initial_cap,omitempty" json:"initial_cap,omitempty"`
	// MaxOpen is used by the internal connection pool and is used to configure
	// the maximum number of connections held in the pool. By default the
	// maximum number of connections is 1
	MaxOpen int `rethinkdb:"max_open,omitempty" json:"max_open,omitempty"`

	// Below options are for cluster discovery, please note there is a high
	// probability of these changing as the API is still being worked on.

	// DiscoverHosts is used to enable host discovery, when true the driver
	// will attempt to discover any new nodes added to the cluster and then
	// start sending queries to these new nodes.
	DiscoverHosts bool `rethinkdb:"discover_hosts,omitempty" json:"discover_hosts,omitempty"`
	// HostDecayDuration is used by the go-hostpool package to calculate a weighted
	// score when selecting a host. By default a value of 5 minutes is used.
	HostDecayDuration time.Duration `json:"host_decay_duration,omitempty"`

	// UseOpentracing is used to enable creating opentracing-go spans for queries.
	// Each span is created as child of span from the context in `RunOpts`.
	// This span lasts from point the query created to the point when cursor closed.
	UseOpentracing bool `json:"use_opentracing,omitempty"`

	// Deprecated: This function is no longer used due to changes in the
	// way hosts are selected.
	NodeRefreshInterval time.Duration `rethinkdb:"node_refresh_interval,omitempty" json:"node_refresh_interval,omitempty"`
	// Deprecated: Use InitialCap instead
	MaxIdle int `rethinkdb:"max_idle,omitempty" json:"max_idle,omitempty"`

	Log *slog.Logger
}

func (o ConnectOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Connect creates a new database session. To view the available connection
// options see ConnectOpts.
//
// By default maxIdle and maxOpen are set to 1: passing values greater
// than the default (e.g. MaxIdle: "10", MaxOpen: "20") will provide a
// pool of re-usable connections.
//
// Basic connection example:
//
//	session, err := r.Connect(r.ConnectOpts{
//		Address: "localhost:28015",
//		Database: "test",
//		AuthKey:  "14daak1cad13dj",
//	})
//
// Cluster connection example:
//
//	session, err := r.Connect(r.ConnectOpts{
//		Addresses: []string{"localhost:28015", "localhost:28016"},
//		Database: "test",
//		AuthKey:  "14daak1cad13dj",
//	})
func Connect(opts ConnectOpts) (*Session, error) {
	var addresses = opts.Addresses
	if len(addresses) == 0 {
		addresses = []string{opts.Address}
	}

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
		// note: s.Reconnect() will initialize cluster information which
		// will cause the .IsConnected() method to be caught in a loop
		return &Session{
			hosts: hosts,
			opts:  &opts,
		}, err
	}

	return s, nil
}

// CloseOpts allows calls to the Close function to be configured.
type CloseOpts struct {
	NoReplyWait bool `rethinkdb:"noreplyWait,omitempty"`
}

func (o CloseOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// IsConnected returns true if session has a valid connection.
func (s *Session) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.cluster == nil || s.closed {
		return false
	}
	return s.cluster.IsConnected()
}

// Reconnect closes and re-opens a session.
func (s *Session) Reconnect(optArgs ...CloseOpts) error {
	var err error

	if err = s.Close(optArgs...); err != nil {
		return err
	}

	s.mu.Lock()
	s.cluster, err = NewCluster(s.hosts, s.opts)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	s.closed = false
	s.mu.Unlock()

	return nil
}

// Close closes the session
func (s *Session) Close(optArgs ...CloseOpts) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	if len(optArgs) >= 1 {
		if optArgs[0].NoReplyWait {
			s.mu.Unlock()
			s.NoReplyWait()
			s.mu.Lock()
		}
	}

	if s.cluster != nil {
		return s.cluster.Close()
	}
	s.cluster = nil
	s.closed = true

	return nil
}

// SetInitialPoolCap sets the initial capacity of the connection pool.
func (s *Session) SetInitialPoolCap(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.opts.InitialCap = n
	s.cluster.SetInitialPoolCap(n)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
func (s *Session) SetMaxIdleConns(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.opts.MaxIdle = n
	s.cluster.SetMaxIdleConns(n)
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
func (s *Session) SetMaxOpenConns(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.opts.MaxOpen = n
	s.cluster.SetMaxOpenConns(n)
}

// NoReplyWait ensures that previous queries with the noreply flag have been
// processed by the server. Note that this guarantee only applies to queries
// run on the given connection
func (s *Session) NoReplyWait() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return ErrConnectionClosed
	}

	return s.cluster.Exec(context.TODO(), Query{ // nil = connection opts' defaults
		Type: p.Query_NOREPLY_WAIT,
	})
}

// Use changes the default database used
func (s *Session) Use(database string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.opts.Database = database
}

// Database returns the selected database set by Use
func (s *Session) Database() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.opts.Database
}

// Query executes a ReQL query using the session to connect to the database
func (s *Session) Query(ctx context.Context, q Query) (*Cursor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrConnectionClosed
	}

	return s.cluster.Query(ctx, q)
}

// Exec executes a ReQL query using the session to connect to the database
func (s *Session) Exec(ctx context.Context, q Query) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return ErrConnectionClosed
	}

	return s.cluster.Exec(ctx, q)
}

// Server returns the server name and server UUID being used by a connection.
func (s *Session) Server() (ServerResponse, error) {
	return s.cluster.Server()
}

// SetHosts resets the hosts used when connecting to the RethinkDB cluster
func (s *Session) SetHosts(hosts []Host) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.hosts = hosts
}

func (s *Session) newQuery(t Term, opts map[string]interface{}) (Query, error) {
	return newQuery(t, opts, s.opts)
}
