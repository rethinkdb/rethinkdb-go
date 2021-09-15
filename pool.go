package rethinkdb

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/silentred/gid"
	"golang.org/x/net/context"
)

var (
	errPoolClosed = errors.New("rethinkdb: pool is closed")
)

const (
	poolIsNotClosed int32 = 0
	poolIsClosed    int32 = 1
)

// PoolBusyWaitFunc is called by the internal connection pool to wait when
// every connection in the pool is already in use.  If an error is returned
// the pool will give up.
type PoolBusyWaitFunc func(ctx context.Context, attempt int) error

// A Pool is used to store a pool of connections to a single RethinkDB server
type Pool struct {
	host Host
	opts *ConnectOpts

	mu           sync.Mutex // protects lazy creating connections
	countedConns []countedConn
	busyWait     PoolBusyWaitFunc

	closed int32

	connFactory connFactory

	afterAcquire  func(pos int)
	beforeRelease func(pos int)
}

type countedConn struct {
	conn        *Connection
	mu          int64
	refCount    int64
	goroutineID int64
}

// NewPool creates a new connection pool for the given host
func NewPool(host Host, opts *ConnectOpts) (*Pool, error) {
	return newPool(host, opts, NewConnection)
}

func newPool(host Host, opts *ConnectOpts, connFactory connFactory) (*Pool, error) {
	initialCap := opts.InitialCap
	if initialCap <= 0 {
		// Fallback to MaxIdle if InitialCap is zero, this should be removed
		// when MaxIdle is removed
		initialCap = opts.MaxIdle
	}

	maxOpen := opts.MaxOpen
	if maxOpen <= 0 {
		maxOpen = 1
	}

	busyWait := opts.BusyPoolWait
	if busyWait == nil {
		busyWait = func(ctx context.Context, attempt int) error {
			time.Sleep(500 * time.Millisecond)

			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}

			return nil
		}
	}

	countedConns := make([]countedConn, maxOpen)
	var err error

	for i := 0; i < opts.InitialCap; i++ {
		countedConns[i].conn, err = connFactory(host.String(), opts)
		if err != nil {
			return nil, err
		}
	}

	return &Pool{
		host:         host,
		opts:         opts,
		countedConns: countedConns,
		busyWait:     busyWait,
		connFactory:  connFactory,
		closed:       poolIsNotClosed,
	}, nil
}

// Ping verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (p *Pool) Ping() error {
	pos, err := p.acquire(nil)
	if err != nil {
		return err
	}
	defer p.release(pos)

	_, err = p.getConnection(pos)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the database, releasing any open resources.
//
// It is rare to Close a Pool, as the Pool handle is meant to be
// long-lived and shared between many goroutines.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed == poolIsClosed {
		return nil
	}
	p.closed = poolIsClosed

	for _, nConn := range p.countedConns {
		if nConn.conn != nil {
			err := nConn.conn.Close()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// SetInitialPoolCap sets the initial capacity of the connection pool.
//
// Deprecated: This value should only be set when connecting
func (p *Pool) SetInitialPoolCap(n int) {
	return
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// Deprecated: This value should only be set when connecting
func (p *Pool) SetMaxIdleConns(n int) {
	return
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
//
// Deprecated: This value should only be set when connecting
func (p *Pool) SetMaxOpenConns(n int) {
	return
}

// Query execution functions

// Exec executes a query but does not return the response.
func (p *Pool) Exec(ctx context.Context, q Query) error {
	pos, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	defer p.release(pos)

	conn, err := p.getConnection(pos)
	if err != nil {
		return err
	}

	_, _, err = conn.Query(ctx, q)

	return err
}

// Query executes a query and returns the response.
// The returned cursor should be closed when it is no longer needed.
func (p *Pool) Query(ctx context.Context, q Query) (*Cursor, error) {
	pos, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := p.getConnection(pos)
	if err != nil {
		p.release(pos)
		return nil, err
	}

	_, cursor, err := conn.Query(ctx, q)

	if err == nil && cursor != nil {
		cursor.releaseConn = func() error {
			p.release(pos)
			return nil
		}
	} else {
		p.release(pos)
	}

	return cursor, err
}

// Server returns the server name and server UUID being used by a connection.
func (p *Pool) Server() (ServerResponse, error) {
	var response ServerResponse

	pos, err := p.acquire(nil)
	if err != nil {
		return response, err
	}
	defer p.release(pos)

	conn, err := p.getConnection(pos)
	if err != nil {
		return response, err
	}

	response, err = conn.Server()

	return response, err
}

// getConnection returns a valid (usable) connection from the pool having the given index.
func (p *Pool) getConnection(pos int) (*Connection, error) {
	conn := &p.countedConns[pos].conn
	var err error

	if *conn == nil || (*conn).isBad() {
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.closed == poolIsClosed {
			return nil, errPoolClosed
		}

		*conn, err = p.connFactory(p.host.String(), p.opts)
		if err != nil {
			return nil, err
		}
	}

	return *conn, nil
}

// acquire returns the index of a pool entry the caller can use.  This will either be an unused entry or one
// already claimed by the goroutine.
func (p *Pool) acquire(ctx context.Context) (pos int, err error) {
	for attempt := 1; ; attempt++ {
		pos = p.tryAcquire()
		if pos >= 0 {
			return pos, nil
		}

		if p.busyWait != nil {
			err = p.busyWait(ctx, attempt)
			if err != nil {
				return -1, err
			}
		}
	}
}

func (p *Pool) tryAcquire() (pos int) {
	goroutineID := gid.Get()

	for i := range p.countedConns {
		c := &p.countedConns[i]

		// Prevent two goroutines from claiming the same unused entry if it was previously used by one of them,
		// ie. c.refCount == 0 && c.goroutineID == gid.Get().
		if !atomic.CompareAndSwapInt64(&c.mu, 0, 1) {
			continue
		}

		if atomic.CompareAndSwapInt64(&c.refCount, 0, 1) {
			// Unused; claim it for use by this goroutine.
			atomic.StoreInt64(&c.goroutineID, goroutineID)
			atomic.StoreInt64(&c.mu, 0)

			if p.afterAcquire != nil {
				p.afterAcquire(i)
			}

			return i
		}

		if atomic.LoadInt64(&c.goroutineID) == goroutineID {
			// Previously claimed for use by this goroutine.
			atomic.AddInt64(&c.refCount, 1)
			atomic.StoreInt64(&c.mu, 0)

			if p.afterAcquire != nil {
				p.afterAcquire(i)
			}

			return i
		}

		// Using this pool entry would break Connection's requirement it "should only be accessed be a single goroutine".
		atomic.StoreInt64(&c.mu, 0)
	}

	return -1
}

// release must be called once for each successful acquire() call but only after the caller no longer needs the connection.
func (p *Pool) release(pos int) {
	if p.beforeRelease != nil {
		p.beforeRelease(pos)
	}

	atomic.AddInt64(&p.countedConns[pos].refCount, -1)
}
