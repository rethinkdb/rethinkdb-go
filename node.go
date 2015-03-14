package gorethink

import (
	"sync"
)

// Node represents a database server in the cluster
type Node struct {
	ID      string
	Host    Host
	aliases []Host

	cluster *Cluster
	pool    *Pool

	mu     sync.RWMutex
	active bool
}

// IsActive returns true if the node is active
func (n *Node) IsActive() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.active
}

func (n *Node) Query(q Query) (cursor *Cursor, err error) {
	if !n.IsActive() {
		return nil, ErrInvalidNode
	}

	cursor, err = n.pool.Query(q)
	return
}

func (n *Node) Exec(q Query) (err error) {
	if !n.IsActive() {
		return ErrInvalidNode
	}

	err = n.pool.Exec(q)
	return
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

func (s nodeStatus) CreateNode(c *Cluster) *Node {
	aliases := make([]Host, len(s.Network.CanonicalAddresses))
	for i, aliasAddress := range s.Network.CanonicalAddresses {
		aliases[i] = NewHost(aliasAddress.Host, int(aliasAddress.Port))
	}

	return &Node{
		ID:      s.ID,
		Host:    aliases[0],
		aliases: aliases,
		cluster: c,
	}
}
