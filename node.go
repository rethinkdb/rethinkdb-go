package gorethink

import (
	"sync"
	"sync/atomic"
	"time"

	p "github.com/dancannon/gorethink/ql2"
)

// Node represents a database server in the cluster
type Node struct {
	ID      string
	Host    Host
	aliases []Host

	cluster       *Cluster
	pool          *Pool
	refreshTicker *time.Ticker

	mu     sync.RWMutex
	closed bool
	health int64
}

// Closed returns true if the node is closed
func (n *Node) Closed() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.closed
}

// Close closes the session
func (n *Node) Close(optArgs ...CloseOpts) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return nil
	}

	if len(optArgs) >= 1 {
		if optArgs[0].NoReplyWait {
			n.NoReplyWait()
		}
	}

	n.refreshTicker.Stop()
	if n.pool != nil {
		n.pool.Close()
	}
	n.pool = nil
	n.closed = true

	return nil
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
func (n *Node) SetMaxIdleConns(idleConns int) {
	n.pool.SetMaxIdleConns(idleConns)
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
func (n *Node) SetMaxOpenConns(openConns int) {
	n.pool.SetMaxOpenConns(openConns)
}

// NoReplyWait ensures that previous queries with the noreply flag have been
// processed by the server. Note that this guarantee only applies to queries
// run on the given connection
func (n *Node) NoReplyWait() error {
	return n.pool.Exec(Query{
		Type: p.Query_NOREPLY_WAIT,
	})
}

func (n *Node) Query(q Query) (cursor *Cursor, err error) {
	if n.Closed() {
		return nil, ErrInvalidNode
	}

	cursor, err = n.pool.Query(q)
	return
}

func (n *Node) Exec(q Query) (err error) {
	if n.Closed() {
		return ErrInvalidNode
	}

	err = n.pool.Exec(q)
	return
}

func (n *Node) Refresh() {
	cursor, err := n.pool.Query(newQuery(
		Db("rethinkdb").Table("server_status").Get(n.ID),
		map[string]interface{}{},
		n.cluster.opts,
	))
	if err != nil {
		n.DecrementHealth()
		return
	}
	defer cursor.Close()

	var status nodeStatus
	err = cursor.One(&status)
	if err != nil {
		return
	}

	if status.Status != "connected" {
		n.DecrementHealth()
		return
	}

	// If status check was successful reset health
	n.ResetHealth()
}

func (n *Node) DecrementHealth() {
	atomic.AddInt64(&n.health, -1)
}

func (n *Node) ResetHealth() {
	atomic.StoreInt64(&n.health, 100)
}

func (n *Node) IsHealthy() bool {
	return n.health > 0
}

type nodeStatus struct {
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
