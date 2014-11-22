package gorethink

import (
	"sync"
	"sync/atomic"
	"time"
)

type ConnectionPool interface {
	Get() *Connection
	Size() int
	HandleError(*Connection, error, bool)
	Close()
}

//NewPoolFunc is the type used by ClusterConfig to create a pool of a specific type.
type NewPoolFunc func(*Session) ConnectionPool

//SimplePool is the current implementation of the connection pool inside gocql. This
//pool is meant to be a simple default used by gocql so users can get up and running
//quickly.
type SimplePool struct {
	s        *Session
	connPool *RoundRobin
	conns    map[*Connection]struct{}

	// protects connPoll, conns, quit
	mu sync.Mutex

	cFillingPool chan int

	quit     bool
	quitWait chan bool
	quitOnce sync.Once
}

//NewSimplePool is the function used by gocql to create the simple connection pool.
//This is the default if no other pool type is specified.
func NewSimplePool(s *Session) ConnectionPool {
	pool := &SimplePool{
		s:            s,
		connPool:     NewRoundRobin(),
		conns:        make(map[*Connection]struct{}),
		quitWait:     make(chan bool),
		cFillingPool: make(chan int, 1),
	}

	if pool.connect() == nil {
		pool.cFillingPool <- 1
		go pool.fillPool()
	}

	return pool
}

func (c *SimplePool) connect() error {
	conn, err := Dial(c.s)
	if err != nil {
		return err
	}

	return c.addConn(newConnection(c.s, conn, c))
}

func (c *SimplePool) addConn(conn *Connection) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.quit {
		conn.Close()
		return nil
	}

	c.connPool.AddNode(conn)
	c.conns[conn] = struct{}{}

	return nil
}

//fillPool manages the pool of connections making sure that each host has the correct
//amount of connections defined. Also the method will test a host with one connection
//instead of flooding the host with number of connections defined in the cluster config
func (c *SimplePool) fillPool() {
	//Debounce large amounts of requests to fill pool
	select {
	case <-time.After(1 * time.Millisecond):
		return
	case <-c.cFillingPool:
		defer func() { c.cFillingPool <- 1 }()
	}

	c.mu.Lock()
	isClosed := c.quit
	c.mu.Unlock()
	//Exit if cluster(session) is closed
	if isClosed {
		return
	}

	numConns := 1
	//See if the host already has connections in the pool
	c.mu.Lock()
	conns := c.connPool
	c.mu.Unlock()

	//if the host has enough connections just exit
	numConns = conns.Size()
	if numConns >= c.s.maxCap {
		return
	}

	//This is reached if the host is responsive and needs more connections
	//Create connections for host synchronously to mitigate flooding the host.
	go func(conns int) {
		for ; conns < c.s.maxCap; conns++ {
			c.connect()
		}
	}(numConns)
}

// Should only be called if c.mu is locked
func (c *SimplePool) removeConnLocked(conn *Connection) {
	conn.Close()
	c.connPool.RemoveNode(conn)
	delete(c.conns, conn)
}

func (c *SimplePool) removeConn(conn *Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeConnLocked(conn)
}

//HandleError is called by a Connection object to report to the pool an error has occured.
//Logic is then executed within the pool to clean up the erroroneous connection and try to
//top off the pool.
func (c *SimplePool) HandleError(conn *Connection, err error, closed bool) {
	if !closed {
		// ignore all non-fatal errors
		return
	}
	c.removeConn(conn)
	if !c.quit {
		go c.fillPool() // top off pool.
	}
}

//Pick selects a connection to be used by the query.
func (c *SimplePool) Get() *Connection {
	//Check if connections are available
	c.mu.Lock()
	conns := len(c.conns)
	c.mu.Unlock()

	if conns == 0 {
		//try to populate the pool before returning.
		c.fillPool()
	}

	return c.connPool.Get()
}

//Size returns the number of connections currently active in the pool
func (p *SimplePool) Size() int {
	p.mu.Lock()
	conns := len(p.conns)
	p.mu.Unlock()
	return conns
}

//Close kills the pool and all associated connections.
func (c *SimplePool) Close() {
	c.quitOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.quit = true
		close(c.quitWait)
		for conn := range c.conns {
			c.removeConnLocked(conn)
		}
	})
}

type RoundRobin struct {
	pool []*Connection
	pos  uint32
	mu   sync.RWMutex
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

func (r *RoundRobin) AddNode(node *Connection) {
	r.mu.Lock()
	r.pool = append(r.pool, node)
	r.mu.Unlock()
}

func (r *RoundRobin) RemoveNode(node *Connection) {
	r.mu.Lock()
	n := len(r.pool)
	for i := 0; i < n; i++ {
		if r.pool[i] == node {
			r.pool[i], r.pool[n-1] = r.pool[n-1], r.pool[i]
			r.pool = r.pool[:n-1]
			break
		}
	}
	r.mu.Unlock()
}

func (r *RoundRobin) Size() int {
	r.mu.RLock()
	n := len(r.pool)
	r.mu.RUnlock()
	return n
}

func (r *RoundRobin) Get() *Connection {
	pos := atomic.AddUint32(&r.pos, 1)
	var conn *Connection
	r.mu.RLock()
	if len(r.pool) > 0 {
		conn = r.pool[pos%uint32(len(r.pool))]
	}
	r.mu.RUnlock()
	if conn == nil {
		return nil
	}
	if conn.closed {
		return nil
	}
	return conn
}

func (r *RoundRobin) Close() {
	r.mu.Lock()
	for i := 0; i < len(r.pool); i++ {
		r.pool[i].Close()
	}
	r.pool = nil
	r.mu.Unlock()
}
