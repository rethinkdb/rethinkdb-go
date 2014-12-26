package gorethink

import (
	"errors"
	"sync"
	"time"
)

const defaultMaxIdleConns = 2

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

// depSet is a finalCloser's outstanding dependencies
type depSet map[interface{}]bool // set of true bools
// The finalCloser interface is used by (*Pool).addDep and related
// dependency reference counting.
type finalCloser interface {
	// finalClose is called when the reference count of an object
	// goes to zero. (*Pool).mu is not held while calling it.
	finalClose() error
}

type idleConn struct {
	c *poolConn
	t time.Time
}

type Pool struct {
	opts *ConnectOpts

	mu           sync.Mutex // protects following fields
	err          error      // the last error that occurred
	freeConn     []idleConn
	connRequests []chan connRequest
	numOpen      int
	pendingOpens int
	// Used to signal the need for new connections
	// a goroutine running connectionOpener() reads on this chan and
	// maybeOpenNewConnections sends on the chan (one send per needed connection)
	// It is closed during p.Close(). The close tells the connectionOpener
	// goroutine to exit.
	openerCh    chan struct{}
	closed      bool
	dep         map[finalCloser]depSet
	maxIdle     int // zero means defaultMaxIdleConns; negative means 0
	maxOpen     int // <= 0 means unlimited
	idleTimeout time.Duration
}

func NewPool(opts *ConnectOpts) (*Pool, error) {
	p := &Pool{
		opts: opts,

		openerCh: make(chan struct{}, connectionRequestQueueSize),
		lastPut:  make(map[*poolConn]string),
		maxIdle:  opts.MaxIdle,
	}
	go p.connectionOpener()
	return p, nil
}

func (p *Pool) GetConn() (*poolConn, error) {
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

	// Remove any stale idle connections
	if timeout := p.idleTimeout; timeout > 0 {
		for i := 0; i < len(p.freeConn); i++ {
			ic := p.freeConn[i]
			if ic.t.Add(timeout).After(time.Now()) {
				break
			}
			p.freeConn = p.freeConn[:i+copy(p.freeConn[i:], p.freeConn[i+1:])]
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	// Check for any free/idle connections
	if n := len(p.freeConn); n > 0 {
		c := p.freeConn[0].c
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
func (p *Pool) connIfFree(wanted *poolConn) (*poolConn, error) {
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
		if v.c == wanted {
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

func (p *Pool) PutConn(c *poolConn, err error, closed bool) {
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
	added := p.putConnPoolLocked(c, nil)
	p.mu.Unlock()
	if !added {
		c.Close()
	}
}

// Satisfy a connRequest or put the Connection in the idle pool and return true
// or return false.
// putConnPoolLocked will satisfy a connRequest if there is one, or it will
// return the *poolConn to the freeConn list if err == nil and the idle
// connection limit will not be exceeded.
// If err != nil, the value of c is ignored.
// If err == nil, then c must not equal nil.
// If a connRequest was fulfilled or the *poolConn was placed in the
// freeConn list, then true is returned, otherwise false is returned.
func (p *Pool) putConnPoolLocked(c *poolConn, err error) bool {
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
	} else if err == nil && !p.closed && p.maxIdleConnsLocked() > len(p.freeConn) {
		p.freeConn = append(p.freeConn, idleConn{c: c, t: time.Now()})
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
		fns = append(fns, c.c.Close)
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

// addDep notes that x now depends on dep, and x's finalClose won't be
// called until all of x's dependencies are removed with removeDep.
func (p *Pool) addDep(x finalCloser, dep interface{}) {
	//println(fmt.Sprintf("addDep(%T %p, %T %p)", x, x, dep, dep))
	p.mu.Lock()
	defer p.mu.Unlock()
	p.addDepLocked(x, dep)
}
func (p *Pool) addDepLocked(x finalCloser, dep interface{}) {
	if p.dep == nil {
		p.dep = make(map[finalCloser]depSet)
	}
	xdep := p.dep[x]
	if xdep == nil {
		xdep = make(depSet)
		p.dep[x] = xdep
	}
	xdep[dep] = true
}

// removeDep notes that x no longer depends on dep.
// If x still has dependencies, nil is returned.
// If x no longer has any dependencies, its finalClose method will be
// called and its error value will be returned.
func (p *Pool) removeDep(x finalCloser, dep interface{}) error {
	p.mu.Lock()
	fn := p.removeDepLocked(x, dep)
	p.mu.Unlock()
	return fn()
}
func (p *Pool) removeDepLocked(x finalCloser, dep interface{}) func() error {
	//println(fmt.Sprintf("removeDep(%T %p, %T %p)", x, x, dep, dep))
	xdep, ok := p.dep[x]
	if !ok {
		panic(fmt.Sprintf("unpaired removeDep: no deps for %T", x))
	}
	l0 := len(xdep)
	delete(xdep, dep)
	switch len(xdep) {
	case l0:
		// Nothing removed. Shouldn't happen.
		panic(fmt.Sprintf("unpaired removeDep: no %T dep on %T", dep, x))
	case 0:
		// No more dependencies.
		delete(p.dep, x)
		return x.finalClose
	default:
		// Dependencies remain.
		return func() error { return nil }
	}
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
		p.putConnPoolLocked(nil, err)
		return
	}
	if p.putConnPoolLocked(c, err) {
		p.numOpen++
	} else {
		c.Close()
	}
}

// connRequest represents one request for a new connection
// When there are no idle connections available, p.conn will create
// a new connRequest and put it on the p.connRequests list.
type connRequest struct {
	conn *poolConn
	err  error
}

// Access pool options

func (p *Pool) maxIdleConnsLocked() int {
	n := p.maxIdle
	switch {
	case n == 0:
		return defaultMaxIdleConns
	case n < 0:
		return 0
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
	if p.maxOpen > 0 && p.maxIdleConnsLocked() > p.maxOpen {
		p.maxIdle = p.maxOpen
	}
	var closing []idleConn
	idleCount := len(p.freeConn)
	maxIdle := p.maxIdleConnsLocked()
	if idleCount > maxIdle {
		closing = p.freeConn[maxIdle:]
		p.freeConn = p.freeConn[:maxIdle]
	}
	p.mu.Unlock()
	for _, c := range closing {
		c.c.Close()
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
	syncMaxIdle := p.maxOpen > 0 && p.maxIdleConnsLocked() > p.maxOpen
	p.mu.Unlock()
	if syncMaxIdle {
		p.SetMaxIdleConns(n)
	}
}
