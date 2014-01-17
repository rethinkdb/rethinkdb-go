package gorethink

// Code copied from redigo.  Gary did an awesome job with the connection pool so why
// reinvent what has aslready been done.
// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

import (
	"container/list"
	"errors"
	p "github.com/dancannon/gorethink/ql2"
	"sync"
	"time"
)

var nowFunc = time.Now // for testing

var ErrPoolExhausted = errors.New("gorethink: connection pool exhausted")
var errPoolClosed = errors.New("gorethink: connection pool closed")

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
type Pool struct {
	Session *Session

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// mu protects fields defined below.
	mu     sync.Mutex
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type idleConn struct {
	c *Connection
	t time.Time
}

// Get gets a connection from the pool.
func (p *Pool) Get() Conn {
	return &pooledConnection{p: p}
}

// ActiveCount returns the number of active connections in the pool.
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *Pool) get() (*Connection, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("gorethink: get on closed pool")
	}

	// Prune stale connections.
	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.active -= 1
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	// Prune dead connections
	for i, n := 0, p.idle.Len(); i < n; i++ {
		e := p.idle.Back()
		if e == nil {
			break
		}
		ic := e.Value.(idleConn)
		if !ic.c.closed {
			continue
		}
		p.idle.Remove(e)
		p.active -= 1
	}

	// Get idle connection.
	for i, n := 0, p.idle.Len(); i < n; i++ {
		e := p.idle.Front()
		if e == nil {
			break
		}
		ic := e.Value.(idleConn)
		p.idle.Remove(e)
		p.mu.Unlock()
		return ic.c, nil
	}

	if p.MaxActive > 0 && p.active >= p.MaxActive {
		p.mu.Unlock()
		return nil, ErrPoolExhausted
	}

	// No idle connection, create new.
	p.active += 1
	p.mu.Unlock()
	c, err := Dial(p.Session)
	if err != nil {
		p.mu.Lock()
		p.active -= 1
		p.mu.Unlock()
		c = nil
	}
	return c, err
}

func (p *Pool) put(c *Connection) error {
	p.mu.Lock()
	if !p.closed {
		p.idle.PushFront(idleConn{t: nowFunc(), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}

	p.mu.Unlock()
	if c != nil {
		p.mu.Lock()
		p.active -= 1
		p.mu.Unlock()
		return c.Close()
	}
	return nil
}

type pooledConnection struct {
	c   *Connection
	err error
	p   *Pool
}

func (c *pooledConnection) get() error {
	if c.err == nil && c.c == nil {
		c.c, c.err = c.p.get()
	}
	return c.err
}

func (c *pooledConnection) Close() (err error) {
	if c.c != nil {
		c.p.put(c.c)
		c.c = nil
		c.err = errPoolClosed
	}
	return err
}

func (c *pooledConnection) SendQuery(s *Session, q *p.Query, t RqlTerm, opts map[string]interface{}) (*ResultRows, error) {
	if err := c.get(); err != nil {
		return nil, err
	}
	return c.c.SendQuery(s, q, t, opts)

}
