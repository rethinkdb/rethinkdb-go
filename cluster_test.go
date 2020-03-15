package rethinkdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/mock"
	test "gopkg.in/check.v1"
	"gopkg.in/rethinkdb/rethinkdb-go.v6/encoding"
	p "gopkg.in/rethinkdb/rethinkdb-go.v6/ql2"
	"io"
	"net"
	"time"
)

type ClusterSuite struct{}

var _ = test.Suite(&ClusterSuite{})

func (s *ClusterSuite) TestCluster_NewSingle_NoDiscover_Ok(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	node1 := "node1"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1)
	expectServerResponse(conn1, 1, node1)
	conn1.On("Close").Return(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Twice()

	opts := &ConnectOpts{}
	seeds := []Host{host1}
	cluster := &Cluster{
		hp:          newHostPool(opts),
		seeds:       seeds,
		opts:        opts,
		closed:      clusterWorking,
		connFactory: mockedConnectionFactory(dialMock),
	}

	err := cluster.run()
	c.Assert(err, test.IsNil)
	conn1.AssertExpectations(c)
	dialMock.AssertExpectations(c)
}

func (s *ClusterSuite) TestCluster_NewMultiple_NoDiscover_Ok(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	host2 := Host{Name: "host2", Port: 28015}
	node1 := "node1"
	node2 := "node2"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1)
	expectServerResponse(conn1, 1, node1)
	conn1.On("Close").Return(nil)
	conn2 := &connMock{}
	expectServerQuery(conn2, 1)
	expectServerResponse(conn2, 1, node2)
	conn2.On("Close").Return(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Twice()
	dialMock.On("Dial", host2.String()).Return(conn2, nil).Twice()

	opts := &ConnectOpts{}
	seeds := []Host{host1, host2}
	cluster := &Cluster{
		hp:          newHostPool(opts),
		seeds:       seeds,
		opts:        opts,
		closed:      clusterWorking,
		connFactory: mockedConnectionFactory(dialMock),
	}

	err := cluster.run()
	c.Assert(err, test.IsNil)
	conn1.AssertExpectations(c)
	conn2.AssertExpectations(c)
	dialMock.AssertExpectations(c)
}

func (s *ClusterSuite) TestCluster_NewSingle_NoDiscover_DialFail(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(nil, io.EOF).Once()

	opts := &ConnectOpts{}
	seeds := []Host{host1}
	cluster := &Cluster{
		hp:          newHostPool(opts),
		seeds:       seeds,
		opts:        opts,
		closed:      clusterWorking,
		connFactory: mockedConnectionFactory(dialMock),
	}

	err := cluster.run()
	c.Assert(err, test.Equals, io.EOF)
	dialMock.AssertExpectations(c)
}

func (s *ClusterSuite) TestCluster_NewMultiple_NoDiscover_DialHalfFail(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	host2 := Host{Name: "host2", Port: 28015}
	node1 := "node1"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1)
	expectServerResponse(conn1, 1, node1)
	conn1.On("Close").Return(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Twice()
	dialMock.On("Dial", host2.String()).Return(nil, io.EOF).Once()

	opts := &ConnectOpts{}
	seeds := []Host{host1, host2}
	cluster := &Cluster{
		hp:          newHostPool(opts),
		seeds:       seeds,
		opts:        opts,
		closed:      clusterWorking,
		connFactory: mockedConnectionFactory(dialMock),
	}

	err := cluster.run()
	c.Assert(err, test.IsNil)
	conn1.AssertExpectations(c)
	dialMock.AssertExpectations(c)
}

func (s *ClusterSuite) TestCluster_NewMultiple_NoDiscover_DialFail(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	host2 := Host{Name: "host2", Port: 28015}

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(nil, io.EOF).Once()
	dialMock.On("Dial", host2.String()).Return(nil, io.EOF).Once()

	opts := &ConnectOpts{}
	seeds := []Host{host1, host2}
	cluster := &Cluster{
		hp:          newHostPool(opts),
		seeds:       seeds,
		opts:        opts,
		closed:      clusterWorking,
		connFactory: mockedConnectionFactory(dialMock),
	}

	err := cluster.run()
	c.Assert(err, test.Equals, io.EOF)
	dialMock.AssertExpectations(c)
}

func (s *ClusterSuite) TestCluster_NewSingle_NoDiscover_ServerFail(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}

	conn1 := &connMock{}
	expectServerQuery(conn1, 1)
	expectServerResponseError(conn1, io.EOF)
	conn1.On("Close").Return(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Once()

	opts := &ConnectOpts{}
	seeds := []Host{host1}
	cluster := &Cluster{
		hp:          newHostPool(opts),
		seeds:       seeds,
		opts:        opts,
		closed:      clusterWorking,
		connFactory: mockedConnectionFactory(dialMock),
	}

	err := cluster.run()
	c.Assert(err, test.Equals, RQLConnectionError{rqlError(io.EOF.Error())})
	conn1.AssertExpectations(c)
	dialMock.AssertExpectations(c)
}

func (s *ClusterSuite) TestCluster_NewSingle_NoDiscover_PingFail(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	node1 := "node1"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1)
	expectServerResponse(conn1, 1, node1)
	conn1.On("Close").Return(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Once()
	dialMock.On("Dial", host1.String()).Return(nil, io.EOF).Once()

	opts := &ConnectOpts{}
	seeds := []Host{host1}
	cluster := &Cluster{
		hp:          newHostPool(opts),
		seeds:       seeds,
		opts:        opts,
		closed:      clusterWorking,
		connFactory: mockedConnectionFactory(dialMock),
	}

	err := cluster.run()
	c.Assert(err, test.Equals, io.EOF)
	conn1.AssertExpectations(c)
	dialMock.AssertExpectations(c)
}

func (s *ClusterSuite) TestCluster_NewSingle_Discover_Ok(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	host2 := Host{Name: "1.1.1.1", Port: 2222}
	host3 := Host{Name: "2.2.2.2", Port: 3333}
	node1 := "node1"
	node2 := "node2"
	node3 := "node3"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1)
	expectServerResponse(conn1, 1, node1)
	conn1.On("Close").Return(nil)
	conn2 := &connMock{}
	expectServerStatus(conn2, 1)
	expectServerStatusContinue(conn2, 1)
	expectServerStatusResponse(conn2, 1, []string{node1, node2, node3}, []Host{host1, host2, host3})
	conn2.On("Close").Return(nil)
	conn3 := &connMock{}
	expectRunRead(conn3)
	conn3.On("Close").Return(nil)
	conn4 := &connMock{} // doesn't need call Server() due to it's known through ServerStatus()
	expectRunRead(conn4)
	conn4.On("Close").Return(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Twice()
	dialMock.On("Dial", host1.String()).Return(conn2, nil).Once() // conn1 is bad cause expectServerResponse returns io.EOF
	dialMock.On("Dial", host2.String()).Return(conn3, nil).Once()
	dialMock.On("Dial", host3.String()).Return(conn4, nil).Once()

	opts := &ConnectOpts{DiscoverHosts: true}
	seeds := []Host{host1}
	cluster := &Cluster{
		hp:               newHostPool(opts),
		seeds:            seeds,
		opts:             opts,
		closed:           clusterWorking,
		connFactory:      mockedConnectionFactory(dialMock),
		discoverInterval: 10 * time.Second,
	}

	err := cluster.run()
	time.Sleep(10 * time.Millisecond) // wait for backgroud discover works
	_ = cluster.Close()
	c.Assert(err, test.IsNil)
	conn1.AssertExpectations(c)
	conn2.AssertExpectations(c)
	conn3.AssertExpectations(c)
	conn4.AssertExpectations(c)
	dialMock.AssertExpectations(c)
}

type mockDial struct {
	mock.Mock
}

func mockedConnectionFactory(dial *mockDial) connFactory {
	return func(host string, opts *ConnectOpts) (connection *Connection, err error) {
		args := dial.MethodCalled("Dial", host)
		err = args.Error(1)
		if err != nil {
			return nil, err
		}

		connection = newConnection(args.Get(0).(net.Conn), host, opts)
		connection.runConnection()
		return connection, nil
	}
}

func expectServerQuery(conn *connMock, token int64) {
	buf := &bytes.Buffer{}
	buf.Grow(respHeaderLen)
	buf.Write(buf.Bytes()[:respHeaderLen]) // reserve for header
	enc := json.NewEncoder(buf)

	q := Query{
		Token: token,
		Type:  p.Query_SERVER_INFO,
	}

	// Build query
	_ = enc.Encode(q.Build())

	b := buf.Bytes()

	// Write header
	binary.LittleEndian.PutUint64(b, uint64(q.Token))
	binary.LittleEndian.PutUint32(b[8:], uint32(len(b)-respHeaderLen))

	conn.On("Write", b).Return(0, nil, nil).Once()
}

func expectServerResponse(conn *connMock, token int64, nodeID string) {
	buf1 := &bytes.Buffer{}
	buf1.Grow(respHeaderLen)
	buf1.Write(buf1.Bytes()[:respHeaderLen]) // reserve for header

	buf2 := &bytes.Buffer{}
	enc := json.NewEncoder(buf2)

	coded, err := encoding.Encode(&ServerResponse{ID: nodeID})
	if err != nil {
		panic(fmt.Sprintf("failed to encode response: %v", err))
	}
	jresp, err := json.Marshal(coded)
	if err != nil {
		panic(fmt.Sprintf("failed to encode response: %v", err))
	}
	resp := Response{Token: token, Type: p.Response_SERVER_INFO, Responses: []json.RawMessage{jresp}}
	_ = enc.Encode(resp)

	b1 := buf1.Bytes()
	b2 := buf2.Bytes()
	// Write header
	binary.LittleEndian.PutUint64(b1, uint64(token))
	binary.LittleEndian.PutUint32(b1[8:], uint32(len(b2)))

	conn.On("Read", respHeaderLen).Return(b1, len(b1), nil, nil).Once()
	conn.On("Read", len(b2)).Return(b2, len(b2), nil, nil).Once()
	conn.On("Read", respHeaderLen).Return(nil, 0, io.EOF, nil)
}

func expectRunRead(conn *connMock) {
	conn.On("Read", respHeaderLen).Return(nil, 0, io.EOF, nil)
}

func expectServerResponseError(conn *connMock, err error) {
	conn.On("Read", respHeaderLen).Return(nil, 0, err, nil).Once()
	conn.On("Read", respHeaderLen).Return(nil, 0, io.EOF, nil)
}

func expectServerStatus(conn *connMock, token int64) {
	buf := &bytes.Buffer{}
	buf.Grow(respHeaderLen)
	buf.Write(buf.Bytes()[:respHeaderLen]) // reserve for header
	enc := json.NewEncoder(buf)

	t := DB(SystemDatabase).Table(ServerStatusSystemTable).Changes(ChangesOpts{IncludeInitial: true})
	q, _ := newQuery(t, map[string]interface{}{}, &ConnectOpts{})
	q.Token = token

	// Build query
	_ = enc.Encode(q.Build())

	b := buf.Bytes()

	// Write header
	binary.LittleEndian.PutUint64(b, uint64(q.Token))
	binary.LittleEndian.PutUint32(b[8:], uint32(len(b)-respHeaderLen))

	conn.On("Write", b).Return(0, nil, nil).Once()
}

func expectServerStatusContinue(conn *connMock, token int64) {
	buf := &bytes.Buffer{}
	buf.Grow(respHeaderLen)
	buf.Write(buf.Bytes()[:respHeaderLen]) // reserve for header
	enc := json.NewEncoder(buf)

	q := Query{Token: token, Type: p.Query_CONTINUE}

	// Build query
	_ = enc.Encode(q.Build())

	b := buf.Bytes()

	// Write header
	binary.LittleEndian.PutUint64(b, uint64(q.Token))
	binary.LittleEndian.PutUint32(b[8:], uint32(len(b)-respHeaderLen))

	conn.On("Write", b).Return(0, nil, nil).Once()
}

func expectServerStatusResponse(conn *connMock, token int64, nodeIDs []string, hosts []Host) {
	buf1 := &bytes.Buffer{}
	buf1.Grow(respHeaderLen)
	buf1.Write(buf1.Bytes()[:respHeaderLen]) // reserve for header

	buf2 := &bytes.Buffer{}
	enc := json.NewEncoder(buf2)

	type change struct {
		NewVal *nodeStatus `rethinkdb:"new_val"`
		OldVal *nodeStatus `rethinkdb:"old_val"`
	}
	jresps := make([]json.RawMessage, len(nodeIDs))
	for i := range nodeIDs {
		status := &nodeStatus{ID: nodeIDs[i], Network: nodeStatusNetwork{
			ReqlPort: int64(hosts[i].Port),
			CanonicalAddresses: []nodeStatusNetworkAddr{
				{Host: hosts[i].Name},
			},
		}}

		coded, err := encoding.Encode(&change{NewVal: status})
		if err != nil {
			panic(fmt.Sprintf("failed to encode response: %v", err))
		}
		jresps[i], err = json.Marshal(coded)
		if err != nil {
			panic(fmt.Sprintf("failed to encode response: %v", err))
		}
	}

	resp := Response{Token: token, Type: p.Response_SUCCESS_PARTIAL, Responses: jresps}
	_ = enc.Encode(resp)

	b1 := buf1.Bytes()
	b2 := buf2.Bytes()
	// Write header
	binary.LittleEndian.PutUint64(b1, uint64(token))
	binary.LittleEndian.PutUint32(b1[8:], uint32(len(b2)))

	conn.On("Read", respHeaderLen).Return(b1, len(b1), nil, nil).Once()
	conn.On("Read", len(b2)).Return(b2, len(b2), nil, nil).Once()
	conn.On("Read", respHeaderLen).Return(nil, 0, nil, 10*time.Second)
}
