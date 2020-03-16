package rethinkdb

import (
	"github.com/stretchr/testify/mock"
	"io"
	"net"
	"time"
)

type connMock struct {
	mock.Mock
	done <-chan struct{}
}

func (m *connMock) waitDone() {
	<-m.done
}

func (m *connMock) onCloseReturn(err error) {
	closeChan := make(chan struct{})
	m.On("Read", respHeaderLen).Return(nil, 0, io.EOF, nil).Once().Run(func(args mock.Arguments) {
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
