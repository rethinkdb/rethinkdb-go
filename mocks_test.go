package rethinkdb

import (
	"github.com/stretchr/testify/mock"
	"io"
	"net"
	"sync"
	"time"
)

type connMock struct {
	mock.Mock
	done      <-chan struct{}
	finalRead chan struct{}
	mu        sync.Mutex
}

func (m *connMock) setDone(done <-chan struct{}) {
	m.mu.Lock()
	m.done = done
	m.mu.Unlock()
}

func (m *connMock) waitFinalRead() {
	<-m.finalRead
}

func (m *connMock) waitDone() {
	m.mu.Lock()
	done := m.done
	m.mu.Unlock()
	<-done
}

func (m *connMock) onCloseReturn(err error) {
	closeChan := make(chan struct{})
	m.finalRead = make(chan struct{})
	m.On("Read", respHeaderLen).Return(nil, 0, io.EOF, nil).Once().Run(func(args mock.Arguments) {
		close(m.finalRead)
		<-closeChan
	})
	m.On("Close").Return(err).Once().Run(func(args mock.Arguments) {
		close(closeChan)
	})
}

func (m *connMock) Read(b []byte) (n int, err error) {
	args := m.Called(len(b))
	rbuf, ok := args.Get(0).([]byte)
	if ok {
		copy(b, rbuf)
	}
	timeout := args.Get(3)
	if timeout != nil {
		time.Sleep(timeout.(time.Duration))
	}
	return args.Int(1), args.Error(2)
}

func (m *connMock) Write(b []byte) (n int, err error) {
	args := m.Called(b)
	timeout := args.Get(2)
	if timeout != nil {
		time.Sleep(timeout.(time.Duration))
	}
	return args.Int(0), args.Error(1)
}

func (m *connMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *connMock) LocalAddr() net.Addr {
	args := m.Called()
	return args.Get(0).(net.Addr)
}

func (m *connMock) RemoteAddr() net.Addr {
	args := m.Called()
	return args.Get(0).(net.Addr)
}

func (m *connMock) SetDeadline(t time.Time) error {
	args := m.Called()
	return args.Error(0)
}

func (m *connMock) SetReadDeadline(t time.Time) error {
	args := m.Called()
	return args.Error(0)
}

func (m *connMock) SetWriteDeadline(t time.Time) error {
	args := m.Called()
	return args.Error(0)
}
