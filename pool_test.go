package rethinkdb

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/silentred/gid"
	"golang.org/x/sync/errgroup"
	test "gopkg.in/check.v1"
)

type PoolSuite struct{}

var _ = test.Suite(&PoolSuite{})

func (s *PoolSuite) TestConcurrency(c *test.C) {
	if testing.Short() {
		c.Skip("-short set")
	}

	const (
		poolCapacity  = 100
		numGoroutines = 2000
		testDuration  = 15 * time.Second
	)

	connFactory := func(host string, opts *ConnectOpts) (connection, error) {
		return &fakePoolConn{badProbability: 0.001}, nil
	}

	p, err := newPool(Host{}, &ConnectOpts{MaxOpen: poolCapacity}, connFactory)
	c.Assert(p, test.NotNil)
	c.Assert(err, test.IsNil)

	localState := [poolCapacity]struct {
		mu          sync.Mutex
		goroutineID int64
		refCount    int64
	}{}

	p.afterAcquire = func(pos int) {
		s := &localState[pos]

		// Don't use atomics here.  It's more important this test be correct than fast.
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.goroutineID != gid.Get() {
			if s.refCount == 0 {
				// First use of a connection, or reuse of an existing but unused connection
				s.goroutineID = gid.Get()
			} else {
				panic(fmt.Sprintf("A connection would be concurrently used by goroutines %v and %v", s.goroutineID, gid.Get()))
			}
		}

		s.refCount++
	}

	p.beforeRelease = func(pos int) {
		s := &localState[pos]

		s.mu.Lock()
		defer s.mu.Unlock()

		s.refCount--
	}

	q := testQuery(DB("db").Table("table").Get("id"))

	calls := []func(*Pool) error{
		func(p *Pool) error {
			return p.Ping()
		},

		func(p *Pool) error {
			return p.Exec(context.Background(), q)
		},

		func(p *Pool) error {
			cursor, err := p.Query(context.Background(), q)
			if err != nil {
				return err
			}

			// Pretend to read from the cursor
			time.Sleep(1 * time.Millisecond)
			cursor.finished = true

			return cursor.Close()
		},

		func(p *Pool) error {
			_, err := p.Server()
			return err
		},
	}

	testLoop := func() error {
		startTime := time.Now()
		for time.Since(startTime) < testDuration {
			i := rand.Intn(len(calls))
			err := calls[i](p)
			if err != nil {
				return err
			}
		}

		return nil
	}

	g := new(errgroup.Group)
	for i := 0; i < numGoroutines; i++ {
		g.Go(testLoop)
	}

	err = g.Wait()
	c.Assert(err, test.IsNil)
}

type fakePoolConn struct {
	bad            int32
	badProbability float32
}

func (c *fakePoolConn) Server() (ServerResponse, error) {
	if c.isBad() {
		return ServerResponse{}, errors.New("Server() was called even though the connection is known to be unusable")
	}

	if rand.Float32() < c.badProbability {
		atomic.StoreInt32(&c.bad, connBad)
	}

	return ServerResponse{}, nil
}

func (c *fakePoolConn) Query(ctx context.Context, q Query) (*Response, *Cursor, error) {
	if c.isBad() {
		return nil, nil, errors.New("Query() was called even though the connection is known to be unusable")
	}

	if rand.Float32() < c.badProbability {
		atomic.StoreInt32(&c.bad, connBad)
	}

	cursor := &Cursor{
		ctx:  context.Background(),
		conn: c,
	}

	return nil, cursor, nil
}

func (c *fakePoolConn) Close() error   { panic("not implemented") }
func (c *fakePoolConn) isBad() bool    { return atomic.LoadInt32(&c.bad) == connBad }
func (c *fakePoolConn) isClosed() bool { return false }
