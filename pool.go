package gorethink

import (
	"errors"
	"sync"
)

const defaultMaxIdleConns = 2
const defaultMaxOpenConns = 0

// maxBadConnRetries is the number of maximum retries if the driver returns
// driver.ErrBadConn to signal a broken connection.
const maxBadConnRetries = 10

var (
	connectionRequestQueueSize = 1000000

	errPoolClosed   = errors.New("gorethink: pool is closed")
	errConnClosed   = errors.New("gorethink: conn is closed")
	errConnBusy     = errors.New("gorethink: conn is busy")
	errConnInactive = errors.New("gorethink: conn was never active")
)

type Pool struct {
	opts *ConnectOpts

	mu           sync.Mutex // protects following fields
	err          error      // the last error that occurred
	freeConn     []*Connection
	connRequests []chan connRequest
	numOpen      int
	pendingOpens int
	// Used to signal the need for new connections
	// a goroutine running connectionOpener() reads on this chan and
	// maybeOpenNewConnections sends on the chan (one send per needed connection)
	// It is closed during p.Close(). The close tells the connectionOpener
	// goroutine to exit.
	openerCh chan struct{}
	closed   bool
	lastPut  map[*Connection]string // stacktrace of last conn's put; debug only
	maxIdle  int                    // zero means defaultMaxIdleConns; negative means 0
	maxOpen  int                    // <= 0 means unlimited
}

func NewPool(opts *ConnectOpts) (*Pool, error) {
	p := &Pool{
		opts: opts,

		openerCh: make(chan struct{}, connectionRequestQueueSize),
		lastPut:  make(map[*Connection]string),
		maxIdle:  defaultMaxIdleConns,
		maxOpen:  defaultMaxOpenConns,
	}
	go p.connectionOpener()
	return p, nil
}

func (p *Pool) GetConn() (*Connection, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPoolClosed
	}

	// If p.maxOpen > 0 and the number of open connections is over the limit
	// and there are no free connection, make a request and wait.
	if p.maxOpen > 0 && p.numOpen >= p.maxOpen && len(p.freeConn) == 0 {
		// Make the connRequest channel. It's buffered so that the
		// connectionOpener doesn't block while waiting for the req to be read.
		req := make(chan connRequest, 1)
		p.connRequests = append(p.connRequests, req)
		p.maybeOpenNewConnections()
		p.mu.Unlock()
		ret := <-req
		// Check if pool has been closed
		if ret.conn == nil && p.closed {
			return nil, errPoolClosed
		}
		return ret.conn, ret.err
	}
	if n := len(p.freeConn); n > 0 {
		c := p.freeConn[0]
		copy(p.freeConn, p.freeConn[1:])
		p.freeConn = p.freeConn[:n-1]
		c.active = true
		p.mu.Unlock()
		return c, nil
	}
	p.numOpen++ // optimistically
	p.mu.Unlock()
	c, err := NewConnection(p.opts)
	if err != nil {
		p.mu.Lock()
		p.numOpen-- // correct for earlier optimism
		p.mu.Unlock()
		return nil, err
	}
	p.mu.Lock()
	c.pool = p
	c.active = true
	p.mu.Unlock()
	return c, nil
}

// connIfFree returns (wanted, nil) if wanted is still a valid conn and
// isn't in use.
//
// The error is errConnClosed if the connection if the requested connection
// is invalid because it's been closed.
//
// The error is errConnBusy if the connection is in use.
func (p *Pool) connIfFree(wanted *Connection) (*Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if wanted.closed {
		return nil, errConnClosed
	}
	if wanted.active {
		return nil, errConnBusy
	}
	idx := -1
	for ii, v := range p.freeConn {
		if v == wanted {
			idx = ii
			break
		}
	}
	if idx >= 0 {
		p.freeConn = append(p.freeConn[:idx], p.freeConn[idx+1:]...)
		wanted.active = true
		return wanted, nil
	}

	return nil, errConnBusy
}

func (p *Pool) PutConn(c *Connection, err error, closed bool) {
	p.mu.Lock()
	if !c.active {
		p.mu.Unlock()
		return
	}
	c.active = false
	if closed {
		p.maybeOpenNewConnections()
		p.mu.Unlock()
		c.Close()
		return
	}
	added := p.putConnDBLocked(c, nil)
	p.mu.Unlock()
	if !added {
		c.Close()
	}
}

// Satisfy a connRequest or put the Connection in the idle pool and return true
// or return false.
// putConnDBLocked will satisfy a connRequest if there is one, or it will
// return the *Connection to the freeConn list if err == nil and the idle
// connection limit will not be exceeded.
// If err != nil, the value of c is ignored.
// If err == nil, then c must not equal nil.
// If a connRequest was fulfilled or the *Connection was placed in the
// freeConn list, then true is returned, otherwise false is returned.
func (p *Pool) putConnDBLocked(c *Connection, err error) bool {
	if c == nil {
		return false
	}

	if n := len(p.connRequests); n > 0 {
		req := p.connRequests[0]
		// This copy is O(n) but in practice faster than a linked list.
		// TODO: consider compacting it down less often and
		// moving the base instead?
		copy(p.connRequests, p.connRequests[1:])
		p.connRequests = p.connRequests[:n-1]
		if err == nil {
			c.active = true
		}
		req <- connRequest{
			conn: c,
			err:  err,
		}
		return true
	} else if err == nil && !p.closed && p.maxIdleConns() > len(p.freeConn) {
		p.freeConn = append(p.freeConn, c)
		return true
	}
	return false
}

func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	close(p.openerCh)
	var err error
	fns := make([]func() error, 0, len(p.freeConn))
	for _, c := range p.freeConn {
		fns = append(fns, c.Close)
	}
	p.freeConn = nil
	p.closed = true
	for _, req := range p.connRequests {
		close(req)
	}
	p.mu.Unlock()
	for _, fn := range fns {
		err1 := fn()
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// Assumes p.mu is locked.
// If there are connRequests and the connection limit hasn't been reached,
// then tell the connectionOpener to open new connections.
func (p *Pool) maybeOpenNewConnections() {
	numRequests := len(p.connRequests) - p.pendingOpens
	if p.maxOpen > 0 {
		numCanOpen := p.maxOpen - (p.numOpen + p.pendingOpens)
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		p.pendingOpens++
		numRequests--
		p.openerCh <- struct{}{}
	}
}

// Runs in a separate goroutine, opens new connections when requested.
func (p *Pool) connectionOpener() {
	for _ = range p.openerCh {
		p.openNewConnection()
	}
}

// Open one new connection
func (p *Pool) openNewConnection() {
	c, err := NewConnection(p.opts)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		if err == nil {
			c.Close()
		}
		return
	}
	p.pendingOpens--
	if err != nil {
		p.putConnDBLocked(nil, err)
		return
	}
	if p.putConnDBLocked(c, err) {
		p.numOpen++
	} else {
		c.Close()
	}
}

// connRequest represents one request for a new connection
// When there are no idle connections available, p.conn will create
// a new connRequest and put it on the p.connRequests list.
type connRequest struct {
	conn *Connection
	err  error
}

// Access pool options

func (p *Pool) maxIdleConns() int {
	n := p.maxIdle
	switch {
	case n == 0:
		return defaultMaxIdleConns
	case n < 0:
		return 0
	case p.maxOpen < n:
		return p.maxOpen
	default:
		return n
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit
//
// If n <= 0, no idle connections are retained.
func (p *Pool) SetMaxIdleConns(n int) {
	p.mu.Lock()
	if n > 0 {
		p.maxIdle = n
	} else {
		// No idle connections.
		p.maxIdle = -1
	}
	// Make sure maxIdle doesn't exceed maxOpen
	if p.maxOpen > 0 && p.maxIdleConns() > p.maxOpen {
		p.maxIdle = p.maxOpen
	}
	var closing []*Connection
	idleCount := len(p.freeConn)
	maxIdle := p.maxIdleConns()
	if idleCount > maxIdle {
		closing = p.freeConn[maxIdle:]
		p.freeConn = p.freeConn[:maxIdle]
	}
	p.mu.Unlock()
	for _, c := range closing {
		c.Close()
	}
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (p *Pool) SetMaxOpenConns(n int) {
	p.mu.Lock()
	p.maxOpen = n
	if n < 0 {
		p.maxOpen = 0
	}
	syncMaxIdle := p.maxOpen > 0 && p.maxIdleConns() > p.maxOpen
	p.mu.Unlock()
	if syncMaxIdle {
		p.SetMaxIdleConns(n)
	}
}
