package gorethink

import (
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNoHosts              = errors.New("no hosts provided")
	ErrNoConnectionsStarted = errors.New("no connections were made when creating the session")
	ErrHostQueryFailed      = errors.New("unable to populate hosts")
	ErrInvalidNode          = errors.New("invalid node")
	ErrClusterClosed        = errors.New("cluster closed")
)

type Cluster struct {
	opts ConnectOpts

	mu sync.RWMutex
	// Initial host nodes specified by user.
	seeds []Host
	// Active nodes in cluster.
	nodes  []*Node
	closed bool

	nodeIndex int64
}

func ConnectCluster(addresses ...string) (*Cluster, error) {
	return ConnectClusterWithOpts(ConnectOpts{}, addresses...)
}

func ConnectClusterWithOpts(opts ConnectOpts, addresses ...string) (*Cluster, error) {
	hosts := make([]Host, len(addresses))
	for i, address := range addresses {
		hostname, port := splitAddress(address)
		hosts[i] = NewHost(hostname, port)
	}
	if len(hosts) <= 0 {
		return nil, ErrNoHosts
	}

	c := &Cluster{
		seeds: hosts,
		opts:  opts,
	}

	//Check that hosts in the ClusterConfig is not empty
	c.seedNodes()
	if !c.IsConnected() {
		return nil, ErrNoHosts
	}

	if opts.DiscoverHosts {
		go c.discover()
	}

	return c, nil
}

func (c *Cluster) Query(q Query) (cursor *Cursor, err error) {
	node, err := c.GetRandomNode()
	if err != nil {
		return nil, err
	}

	return node.Query(q)
}

func (c *Cluster) Exec(q Query) (err error) {
	node, err := c.GetRandomNode()
	if err != nil {
		return err
	}

	return node.Exec(q)
}

func (c *Cluster) newQuery(t Term, opts map[string]interface{}) Query {
	return newQuery(t, opts, c.opts)
}

// Close closes the cluster
func (c *Cluster) Close(optArgs ...CloseOpts) error {
	if c.closed {
		return nil
	}

	for _, node := range c.GetNodes() {
		err := node.Close(optArgs...)
		if err != nil {
			return err
		}
	}

	c.closed = true

	return nil
}

func (c *Cluster) seedNodes() {
	// Add existing nodes to map
	nodesMap := map[string]*Node{}
	for _, node := range c.GetNodes() {
		nodesMap[node.ID] = node
	}

	for _, seedHost := range c.seeds {
		conn, err := NewConnection(seedHost.String(), ConnectOpts{})
		if err != nil {
			continue
		}
		defer conn.Close()

		_, cursor, err := conn.Query(newQuery(
			Db("rethinkdb").Table("server_status"),
			map[string]interface{}{},
			c.opts,
		))
		if err != nil {
			continue
		}

		var results []nodeStatus
		err = cursor.All(&results)
		if err != nil {
			continue
		}

		for _, result := range results {
			node, err := c.connectNode(result)
			if err == nil {
				if _, ok := nodesMap[node.ID]; !ok {
					nodesMap[node.ID] = node
				}
			}
		}
	}

	nodes := []*Node{}
	for _, node := range nodesMap {
		nodes = append(nodes, node)
	}

	c.setNodes(nodes)
}

// discover attempts to find new nodes in the cluster using the current nodes
func (c *Cluster) discover() {
	for {
		node, err := c.GetRandomNode()
		if err != nil {
			time.Sleep(c.opts.ErrorSleepDuration)
			continue
		}

		cursor, err := node.Query(newQuery(
			Db("rethinkdb").Table("server_status").Changes(),
			map[string]interface{}{},
			c.opts,
		))

		if err != nil {
			log.Printf("Error discovering hosts, %s", err)
		}

		var result struct {
			NewVal nodeStatus `gorethink:"new_val"`
			OldVal nodeStatus `gorethink:"old_val"`
		}

		for cursor.Next(&result) {
			addr := fmt.Sprintf("%s:%d", result.NewVal.Network.Hostname, result.NewVal.Network.ReqlPort)
			addr = strings.ToLower(addr)

			switch result.NewVal.Status {
			case "connected":
				node, err := c.connectNode(result.NewVal)
				if !c.nodeExists(node) {
					if err == nil {
						c.addNode(node)
					}
				}
			case "disconnected":
				c.removeNode(result.OldVal.ID)
			default:
				continue
			}

			// If all hosts have been removed then return
			if len(c.GetNodes()) <= 0 {
				return
			}
		}
		if cursor.Err() != nil {
			log.Printf("Error discovering hosts %s, retrying in %d", err, c.opts.ErrorSleepDuration)
		}

		// If an error occurs sleep and setup changefeed again
		time.Sleep(c.opts.ErrorSleepDuration)
	}
}

func (c *Cluster) connectNode(s nodeStatus) (*Node, error) {
	aliases := make([]Host, len(s.Network.CanonicalAddresses))
	for i, aliasAddress := range s.Network.CanonicalAddresses {
		aliases[i] = NewHost(aliasAddress.Host, int(s.Network.ReqlPort))
	}

	// Keep trying to connect to one of the host aliases
	var pool *Pool
	var err error

	for len(aliases) > 0 {
		pool, err = NewPool(aliases[0], c.opts)
		if err != nil {
			aliases = aliases[1:]
			continue
		}

		err = pool.Ping()
		if err != nil {
			aliases = aliases[1:]
			continue
		}

		// Ping successful so break out of loop
		break
	}

	if len(aliases) == 0 {
		return nil, ErrInvalidNode
	}
	if err != nil {
		return nil, err
	}

	return &Node{
		ID:      s.ID,
		Host:    aliases[0],
		aliases: aliases,
		cluster: c,
		pool:    pool,
	}, nil
}

// IsConnected returns true if cluster has nodes and is not already closed.
func (c *Cluster) IsConnected() bool {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()

	return (len(c.GetNodes()) > 0) && !closed
}

// AddSeeds adds new hosts to the cluster.
// They will be added to the cluster on next tend call.
func (c *Cluster) AddSeeds(hosts []Host) {
	c.mu.Lock()
	c.seeds = append(c.seeds, hosts...)
	c.mu.Unlock()
}

func (c *Cluster) getSeeds() []Host {
	c.mu.RLock()
	seeds := c.seeds
	c.mu.RUnlock()

	return seeds
}

// GetRandomNode returns a random node on the cluster
// TODO(dancannon) replace with hostpool
func (c *Cluster) GetRandomNode() (*Node, error) {
	if !c.IsConnected() {
		return nil, ErrClusterClosed
	}
	// Must copy array reference for copy on write semantics to work.
	nodeArray := c.GetNodes()
	length := len(nodeArray)
	for i := 0; i < length; i++ {
		// Must handle concurrency with other non-tending goroutines, so nodeIndex is consistent.
		index := int(math.Abs(float64(c.nextNodeIndex() % int64(length))))
		node := nodeArray[index]

		if !node.Closed() {
			return node, nil
		}
	}
	return nil, ErrNoHosts
}

// GetNodes returns a list of all nodes in the cluster
func (c *Cluster) GetNodes() []*Node {
	c.mu.RLock()
	nodes := c.nodes
	c.mu.RUnlock()

	return nodes
}

func (c *Cluster) nodeExists(search *Node) bool {
	for _, node := range c.GetNodes() {
		if node.ID == search.ID {
			return true
		}
	}
	return false
}

func (c *Cluster) addNode(node *Node) {
	c.mu.Lock()
	c.nodes = append(c.nodes, node)
	c.mu.Unlock()
}

func (c *Cluster) addNodes(nodesToAdd []*Node) {
	c.mu.Lock()
	c.nodes = append(c.nodes, nodesToAdd...)
	c.mu.Unlock()
}

func (c *Cluster) setNodes(nodes []*Node) {
	c.mu.Lock()
	c.nodes = nodes
	c.mu.Unlock()
}

func (c *Cluster) removeNode(nodeID string) {
	nodes := c.GetNodes()
	nodeArray := make([]*Node, len(nodes)-1)
	count := 0

	// Add nodes that are not in remove list.
	for _, n := range nodes {
		if n.ID != nodeID {
			nodeArray[count] = n
			count++
		}
	}

	// Do sanity check to make sure assumptions are correct.
	if count < len(nodeArray) {
		// Resize array.
		nodeArray2 := make([]*Node, count)
		copy(nodeArray2, nodeArray)
		nodeArray = nodeArray2
	}

	c.setNodes(nodeArray)
}

func (c *Cluster) nextNodeIndex() int64 {
	return atomic.AddInt64(&c.nodeIndex, 1)
}
