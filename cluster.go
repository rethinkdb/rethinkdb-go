package gorethink

import (
	"errors"
	"fmt"
	"log"
	"strings"
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
	Hosts              []string      `gorethink:"hosts,omitempty"`
	Database           string        `gorethink:"database,omitempty"`
	AuthKey            string        `gorethink:"authkey,omitempty"`
	Timeout            time.Duration `gorethink:"timeout,omitempty"`
	MaxIdle            int           `gorethink:"max_idle,omitempty"`
	MaxOpen            int           `gorethink:"max_open,omitempty"`
	DiscoverHosts      bool          `gorethink:"discover_hosts,omitempty"`
	HostDecayDuration  time.Duration `gorethink:"host_decay_duration,omitempty"`
	ErrorSleepDuration time.Duration `gorethink:"error_sleep_duration,omitempty"`
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
		go c.discover()
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

type serverStatusResult struct {
	ID      string `gorethink:"id"`
	Name    string `gorethink:"name"`
	Status  string `gorethink:"status"`
	Network struct {
		Hostname           string `gorethink:"hostname"`
		HTTPAdminPort      int64  `gorethink:"http_admin_port"`
		ClusterPort        int64  `gorethink:"cluster_port"`
		ReqlPort           int64  `gorethink:"reql_port"`
		CanonicalAddresses []struct {
			Host string `gorethink:"host"`
			Port int64  `gorethink:"port"`
		} `gorethink:"canonical_addresses"`
	} `gorethink:"network"`
}

// discover attempts to find new nodes in the cluster using the current nodes
func (c *Cluster) discover() {
	c.discoverInitialHosts()
	c.discoverHostChanges()
}

func (c *Cluster) discoverInitialHosts() {
	query := Db("rethinkdb").Table("server_status")
	cursor, err := c.Query(c.newQuery(query, map[string]interface{}{}))
	if err != nil {
		log.Printf("Error discovering hosts, %s", err)
	}

	var result []serverStatusResult
	err = cursor.All(&result)
	if err != nil {
		log.Printf("Error discovering hosts, %s", err)
	}

	addrs := make([]string, 0, len(result))
	for _, host := range result {
		if host.Status == "connected" {
			addrs = append(addrs, strings.ToLower(fmt.Sprintf("%s:%d", host.Network.Hostname, host.Network.ReqlPort)))
		}
	}
	c.nodes.Seed(addrs)
}

func (c *Cluster) discoverHostChanges() {
	for {
		query := Db("rethinkdb").Table("server_status").Changes()
		cursor, err := c.Query(c.newQuery(query, map[string]interface{}{}))
		if err != nil {
			log.Printf("Error discovering hosts, %s", err)
		}

		var result struct {
			NewVal serverStatusResult `gorethink:"new_val"`
			OldVal serverStatusResult `gorethink:"old_val"`
		}

		for cursor.Next(&result) {
			addr := fmt.Sprintf("%s:%d", result.NewVal.Network.Hostname, result.NewVal.Network.ReqlPort)
			addr = strings.ToLower(addr)

			switch result.NewVal.Status {
			case "connected":
				c.nodes.AddHost(addr)
			case "disconnected":
				c.nodes.RemoveHost(addr)
			default:
				continue
			}

			// If all hosts have been removed then return
			if c.nodes.Size() <= 0 {
				return
			}

			c.nodes.refreshHostPool()
		}
		if cursor.Err() != nil {
			log.Printf("Error discovering hosts %s, retrying in %d", err, c.opts.ErrorSleepDuration)
		}

		// If an error occurs sleep and setup changefeed again
		time.Sleep(c.opts.ErrorSleepDuration)
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

func (n *Nodes) Seed(addrs []string) error {
	n.nodes = make(map[string]*Node)

	for _, addr := range addrs {
		if err := n.AddHost(addr); err != nil {
			log.Printf("Error connecting to host %s in cluster %s", addr, err)
			continue
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

func (n *Nodes) AddHost(addr string) error {
	host := Host{
		Address:  addr,
		Database: n.opts.Database,
		AuthKey:  n.opts.AuthKey,
		Timeout:  n.opts.Timeout,
	}
	pool, err := NewPool(host, n.opts.MaxIdle, n.opts.MaxOpen)
	if err != nil {
		return err
	}
	if err := pool.Ping(); err != nil {
		return err
	}

	n.nodes[addr] = &Node{
		host: host,
		pool: pool,
	}

	return nil
}

func (n *Nodes) RemoveHost(addr string) {
	delete(n.nodes, addr)
}

func (n *Nodes) Hosts() []string {
	n.RLock()
	defer n.RUnlock()

	i := 0
	hosts := make([]string, len(n.nodes))
	for _, h := range n.nodes {
		hosts[i] = h.host.Address
		i++
	}

	return hosts
}

func (n *Nodes) Size() int {
	n.RLock()
	defer n.RUnlock()

	return len(n.nodes)
}

func (n *Nodes) refreshHostPool() {
	n.hp = hostpool.NewEpsilonGreedy(n.Hosts(), n.opts.HostDecayDuration, &hostpool.LinearEpsilonValueCalculator{})
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
