package gorethink

import (
	"github.com/stretchr/testify/mock"
	"time"
	"net"
)

type connMock struct {
	mock.Mock
}

func (m* connMock) Read(b []byte) (n int, err error) {
	args := m.Called(len(b))
	rbuf, ok := args.Get(0).([]byte)
	if ok {
		copy(b, rbuf)
	}
	return args.Int(1), args.Error(2)
}

func (m* connMock) Write(b []byte) (n int, err error) {
	args := m.Called(b)
	return args.Int(0), args.Error(1)
}

func (m* connMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m* connMock) LocalAddr() net.Addr {
	args := m.Called()
	return args.Get(0).(net.Addr)
}

func (m* connMock) RemoteAddr() net.Addr {
	args := m.Called()
	return args.Get(0).(net.Addr)
}

func (m* connMock) SetDeadline(t time.Time) error {
	args := m.Called()
	return args.Error(0)
}

func (m* connMock) SetReadDeadline(t time.Time) error {
	args := m.Called()
	return args.Error(0)
}

func (m* connMock) SetWriteDeadline(t time.Time) error {
	args := m.Called()
	return args.Error(0)
}
