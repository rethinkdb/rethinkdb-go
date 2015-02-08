package gorethink

import (
	"errors"
	"sync"
	"time"

	"github.com/bitly/go-hostpool"
	p "github.com/dancannon/gorethink/ql2"
)

var (
	ErrNoHosts              = errors.New("no hosts provided")
	ErrNoConnectionsStarted = errors.New("no connections were made when creating the session")
	ErrHostQueryFailed      = errors.New("unable to populate hosts")
)

type Cluster struct {
	opts  ClusterOpts
	nodes *Nodes
}

type ClusterOpts struct {
	Hosts             []string      `gorethink:"hosts,omitempty"`
	Database          string        `gorethink:"database,omitempty"`
	AuthKey           string        `gorethink:"authkey,omitempty"`
	Timeout           time.Duration `gorethink:"timeout,omitempty"`
	MaxIdle           int           `gorethink:"max_idle,omitempty"`
	MaxOpen           int           `gorethink:"max_open,omitempty"`
	DiscoverHosts     bool          `gorethink:"discover_hosts,omitempty"`
	DiscoverInterval  time.Duration `gorethink:"discover_interval,omitempty"`
	HostDecayDuration time.Duration `gorethink:"host_decay_duration,omitempty"`
}

func ConnectCluster(opts ClusterOpts) (*Cluster, error) {
	c := &Cluster{
		opts: opts,
		nodes: &Nodes{
			opts: opts,
		},
	}

	//Check that hosts in the ClusterConfig is not empty
	if len(opts.Hosts) <= 0 {
		return nil, ErrNoHosts
	}

	// Seed host pool
	if err := c.nodes.Seed(opts.Hosts); err != nil {
		return nil, err
	}

	if opts.DiscoverHosts {
		go c.nodes.discover()
	}

	return c, nil
}

func (c *Cluster) Query(q Query) (cursor *Cursor, err error) {
	hpr, node := c.nodes.Get()
	defer func() {
		// Only mark RqlConnectionErrors as failures
		if _, ok := err.(RqlConnectionError); ok {
			hpr.Mark(err)
		} else {
			hpr.Mark(nil)
		}
	}()

	if node == nil {
		return nil, ErrNoHosts
	}

	cursor, err = node.pool.Query(q)
	return
}

func (c *Cluster) Exec(q Query) (err error) {
	hpr, node := c.nodes.Get()
	defer func() {
		// Only mark RqlConnectionErrors as failures
		if _, ok := err.(RqlConnectionError); ok {
			hpr.Mark(err)
		} else {
			hpr.Mark(nil)
		}
	}()

	if node == nil {
		return ErrNoHosts
	}

	err = node.pool.Exec(q)
	return
}

func (c *Cluster) newQuery(t Term, opts map[string]interface{}) Query {
	queryOpts := map[string]interface{}{}
	for k, v := range opts {
		queryOpts[k] = Expr(v).build()
	}
	if c.opts.Database != "" {
		queryOpts["db"] = Db(c.opts.Database).build()
	}

	// Construct query
	return Query{
		Type: p.Query_START,
		Term: &t,
		Opts: queryOpts,
	}
}

// Nodes stores a list of all of the discovered hosts. Internally it uses an
// epsilon greedy hostpool from the go-hostpool package to determine which
// node to use.
type Nodes struct {
	opts ClusterOpts

	sync.RWMutex
	hosts []string
	nodes map[string]*Node
	hp    hostpool.HostPool
}

func (n *Nodes) Seed(seeds []string) error {
	n.hosts = []string{}
	n.nodes = make(map[string]*Node)

	for _, addr := range seeds {
		host := Host{
			Address:  addr,
			Database: n.opts.Database,
			AuthKey:  n.opts.AuthKey,
			Timeout:  n.opts.Timeout,
		}
		pool, err := NewPool(host, n.opts.MaxIdle, n.opts.MaxOpen)
		if err != nil {
			continue
		}
		if err := pool.Ping(); err != nil {
			continue
		}

		n.hosts = append(n.hosts, addr)
		n.nodes[addr] = &Node{
			host: host,
			pool: pool,
		}
	}

	if n.Size() <= 0 {
		return ErrNoConnectionsStarted
	}

	n.refreshHostPool()

	return nil
}

func (n *Nodes) Get() (hostpool.HostPoolResponse, *Node) {
	hpr := n.hp.Get()

	node, ok := n.nodes[hpr.Host()]
	if !ok {
		return hpr, nil
	}

	return hpr, node
}

func (n *Nodes) Size() int {
	n.RLock()
	defer n.RUnlock()

	return len(n.nodes)
}

func (n *Nodes) refreshHostPool() {
	n.hp = hostpool.NewEpsilonGreedy(n.hosts, n.opts.HostDecayDuration, &hostpool.LinearEpsilonValueCalculator{})
}

// discover attempts to find new nodes in the cluster using the current nodes
func (n *Nodes) discover() {

}

type Node struct {
	host Host
	pool *Pool
}

// Host contains information about a discovered RethinkDB server.
type Host struct {
	Address  string        `gorethink:"address,omitempty"`
	Database string        `gorethink:"database,omitempty"`
	AuthKey  string        `gorethink:"authkey,omitempty"`
	Timeout  time.Duration `gorethink:"timeout,omitempty"`
}
