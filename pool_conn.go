package gorethink

import (
	"errors"
	"sync"
)

type poolConn struct {
	p           *Pool
	sync.Mutex  // guards following
	ci          *Connection
	closed      bool
	finalClosed bool // ci.Close has been called
	// guarded by p.mu
	inUse     bool
	onPut     []func() // code (with p.mu held) run when conn is next returned
	pmuClosed bool     // same as closed, but guarded by p.mu, for connIfFree
}

func (pc *poolConn) releaseConn(err error) {
	pc.p.putConn(pc, err)
}

// the pc.p's Mutex is held.
func (pc *poolConn) closePoolLocked() func() error {
	pc.Lock()
	defer pc.Unlock()
	if pc.closed {
		return func() error { return errors.New("gorethink: duplicate driverConn close") }
	}
	pc.closed = true
	return pc.p.removeDepLocked(pc, pc)
}

func (pc *poolConn) Close() error {
	pc.Lock()
	if pc.closed {
		pc.Unlock()
		return errors.New("gorethink: duplicate driverConn close")
	}
	pc.closed = true
	pc.Unlock() // not defer; removeDep finalClose calls may need to lock
	// And now updates that require holding pc.mu.Lock.
	pc.p.mu.Lock()
	pc.pmuClosed = true
	fn := pc.p.removeDepLocked(pc, pc)
	pc.p.mu.Unlock()
	return fn()
}

func (pc *poolConn) finalClose() error {
	pc.Lock()
	err := pc.ci.Close()
	pc.ci = nil
	pc.finalClosed = true
	pc.Unlock()
	pc.p.mu.Lock()
	pc.p.numOpen--
	pc.p.maybeOpenNewConnections()
	pc.p.mu.Unlock()
	return err
}
