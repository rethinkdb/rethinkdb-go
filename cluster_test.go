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
	expectServerQuery(conn1, 1, node1)
	conn1.onCloseReturn(nil)
	conn2 := &connMock{}
	conn2.onCloseReturn(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Once()
	dialMock.On("Dial", host1.String()).Return(conn2, nil).Once()

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
	conn1.waitDial()
	conn2.waitDial()
	err = cluster.Close()
	c.Assert(err, test.IsNil)
	conn1.waitDone()
	conn2.waitDone()
	mock.AssertExpectationsForObjects(c, dialMock, conn1, conn2)
}

func (s *ClusterSuite) TestCluster_NewMultiple_NoDiscover_Ok(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	host2 := Host{Name: "host2", Port: 28015}
	node1 := "node1"
	node2 := "node2"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1, node1)
	conn1.onCloseReturn(nil)
	conn2 := &connMock{}
	conn2.onCloseReturn(nil)
	conn3 := &connMock{}
	expectServerQuery(conn3, 1, node2)
	conn3.onCloseReturn(nil)
	conn4 := &connMock{}
	conn4.onCloseReturn(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Once()
	dialMock.On("Dial", host1.String()).Return(conn2, nil).Once()
	dialMock.On("Dial", host2.String()).Return(conn3, nil).Once()
	dialMock.On("Dial", host2.String()).Return(conn4, nil).Once()

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
	conn1.waitDial()
	conn2.waitDial()
	conn3.waitDial()
	conn4.waitDial()
	err = cluster.Close()
	c.Assert(err, test.IsNil)
	conn1.waitDone()
	conn2.waitDone()
	conn3.waitDone()
	conn4.waitDone()
	mock.AssertExpectationsForObjects(c, dialMock, conn1, conn2, conn3, conn4)
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
	mock.AssertExpectationsForObjects(c, dialMock)
}

func (s *ClusterSuite) TestCluster_NewMultiple_NoDiscover_DialHalfFail(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	host2 := Host{Name: "host2", Port: 28015}
	node1 := "node1"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1, node1)
	conn1.onCloseReturn(nil)
	conn2 := &connMock{}
	conn2.onCloseReturn(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Once()
	dialMock.On("Dial", host1.String()).Return(conn2, nil).Once()
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
	conn1.waitDial()
	conn2.waitDial()
	err = cluster.Close()
	c.Assert(err, test.IsNil)
	conn1.waitDone()
	conn2.waitDone()
	mock.AssertExpectationsForObjects(c, dialMock, conn1, conn2)
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
	mock.AssertExpectationsForObjects(c, dialMock)
}

func (s *ClusterSuite) TestCluster_NewSingle_NoDiscover_ServerFail(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}

	conn1 := &connMock{}
	expectServerQueryFail(conn1, 1, io.EOF)
	conn1.onCloseReturn(nil)

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
	c.Assert(err, test.NotNil)
	if _, ok := err.(RQLConnectionError); ok {
		c.Assert(err, test.Equals, RQLConnectionError{rqlError(io.EOF.Error())})
	} else {
		c.Assert(err, test.Equals, ErrConnectionClosed)
	}
	conn1.waitDone()
	mock.AssertExpectationsForObjects(c, dialMock, conn1)
}

func (s *ClusterSuite) TestCluster_NewSingle_NoDiscover_PingFail(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	node1 := "node1"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1, node1)
	conn1.onCloseReturn(nil)

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
	conn1.waitDone()
	mock.AssertExpectationsForObjects(c, dialMock, conn1)
}

func (s *ClusterSuite) TestCluster_NewSingle_Discover_Ok(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	host2 := Host{Name: "1.1.1.1", Port: 2222}
	host3 := Host{Name: "2.2.2.2", Port: 3333}
	node1 := "node1"
	node2 := "node2"
	node3 := "node3"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1, node1)
	conn1.onCloseReturn(nil)
	conn2 := &connMock{}
	expectServerStatus(conn2, 1, []string{node1, node2, node3}, []Host{host1, host2, host3})
	conn2.onCloseReturn(nil)
	conn3 := &connMock{}
	conn3.onCloseReturn(nil)
	conn4 := &connMock{} // doesn't need call Server() due to it's known through ServerStatus()
	conn4.onCloseReturn(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Once()
	dialMock.On("Dial", host1.String()).Return(conn2, nil).Once()
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
	c.Assert(err, test.IsNil)
	conn1.waitDial()
	conn2.waitDial()
	conn3.waitDial()
	conn4.waitDial()
	for !cluster.nodeExists(node2) || !cluster.nodeExists(node3) { // wait node to be added to list to be closed with cluster
		time.Sleep(time.Millisecond)
	}
	err = cluster.Close()
	c.Assert(err, test.IsNil)
	conn1.waitDone()
	conn2.waitDone()
	conn3.waitDone()
	conn4.waitDone()
	mock.AssertExpectationsForObjects(c, dialMock, conn1, conn2, conn3, conn4)
}

func (s *ClusterSuite) TestCluster_NewMultiple_Discover_Ok(c *test.C) {
	host1 := Host{Name: "host1", Port: 28015}
	host2 := Host{Name: "host2", Port: 28016}
	host3 := Host{Name: "2.2.2.2", Port: 3333}
	node1 := "node1"
	node2 := "node2"
	node3 := "node3"

	conn1 := &connMock{}
	expectServerQuery(conn1, 1, node1)
	conn1.onCloseReturn(nil)
	conn2 := &connMock{}
	expectServerStatus(conn2, 1, []string{node1, node2, node3}, []Host{host1, host2, host3})
	conn2.onCloseReturn(nil)
	conn3 := &connMock{}
	expectServerQuery(conn3, 1, node2)
	conn3.onCloseReturn(nil)
	conn4 := &connMock{}
	conn4.onCloseReturn(nil)
	conn5 := &connMock{} // doesn't need call Server() due to it's known through ServerStatus()
	conn5.onCloseReturn(nil)

	dialMock := &mockDial{}
	dialMock.On("Dial", host1.String()).Return(conn1, nil).Once()
	dialMock.On("Dial", host1.String()).Return(conn2, nil).Once()
	dialMock.On("Dial", host2.String()).Return(conn3, nil).Once()
	dialMock.On("Dial", host2.String()).Return(conn4, nil).Once()
	dialMock.On("Dial", host3.String()).Return(conn5, nil).Once()

	opts := &ConnectOpts{DiscoverHosts: true}
	seeds := []Host{host1, host2}
	cluster := &Cluster{
		hp:               newHostPool(opts),
		seeds:            seeds,
		opts:             opts,
		closed:           clusterWorking,
		connFactory:      mockedConnectionFactory(dialMock),
		discoverInterval: 10 * time.Second,
	}

	err := cluster.run()
	c.Assert(err, test.IsNil)
	conn1.waitDial()
	conn2.waitDial()
	conn3.waitDial()
	conn4.waitDial()
	conn5.waitDial()
	for !cluster.nodeExists(node3) { // wait node to be added to list to be closed with cluster
		time.Sleep(time.Millisecond)
	}
	err = cluster.Close()
	c.Assert(err, test.IsNil)
	conn1.waitDone()
	conn2.waitDone()
	conn3.waitDone()
	conn4.waitDone()
	conn5.waitDone()
	mock.AssertExpectationsForObjects(c, dialMock, conn1, conn2, conn3, conn4, conn5)
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
		done := runConnection(connection)

		m := args.Get(0).(*connMock)
		m.setDone(done)

		return connection, nil
	}
}

func expectServerQuery(conn *connMock, token int64, nodeID string) {
	writeChan := make(chan struct{})
	readChan := make(chan struct{})

	rawQ := makeServerQueryRaw(token)
	conn.On("Write", rawQ).Return(0, nil, nil).Once().Run(func(args mock.Arguments) {
		close(writeChan)
	})

	rawR := makeServerResponseRaw(token, nodeID)
	rawH := makeResponseHeaderRaw(token, len(rawR))

	conn.On("Read", respHeaderLen).Return(rawH, len(rawH), nil, nil).Once().Run(func(args mock.Arguments) {
		<-writeChan
		close(readChan)
	})
	conn.On("Read", len(rawR)).Return(rawR, len(rawR), nil, nil).Once().Run(func(args mock.Arguments) {
		<-readChan
	})
}

func expectServerQueryFail(conn *connMock, token int64, err error) {
	writeChan := make(chan struct{})

	rawQ := makeServerQueryRaw(token)
	conn.On("Write", rawQ).Return(0, nil, nil).Once().Run(func(args mock.Arguments) {
		close(writeChan)
	})

	conn.On("Read", respHeaderLen).Return(nil, 0, err, nil).Once().Run(func(args mock.Arguments) {
		<-writeChan
	})
}

func makeServerQueryRaw(token int64) []byte {
	buf := &bytes.Buffer{}
	buf.Grow(respHeaderLen)
	buf.Write(buf.Bytes()[:respHeaderLen])
	enc := json.NewEncoder(buf)

	q := Query{
		Token: token,
		Type:  p.Query_SERVER_INFO,
	}

	err := enc.Encode(q.Build())
	if err != nil {
		panic(fmt.Sprintf("must encode failed: %v", err))
	}
	b := buf.Bytes()
	binary.LittleEndian.PutUint64(b, uint64(q.Token))
	binary.LittleEndian.PutUint32(b[8:], uint32(len(b)-respHeaderLen))
	return b
}

func makeResponseHeaderRaw(token int64, respLen int) []byte {
	buf1 := &bytes.Buffer{}
	buf1.Grow(respHeaderLen)
	buf1.Write(buf1.Bytes()[:respHeaderLen]) // reserve for header
	b1 := buf1.Bytes()
	binary.LittleEndian.PutUint64(b1, uint64(token))
	binary.LittleEndian.PutUint32(b1[8:], uint32(respLen))
	return b1
}

func makeServerResponseRaw(token int64, nodeID string) []byte {
	buf2 := &bytes.Buffer{}
	enc := json.NewEncoder(buf2)

	coded, err := encoding.Encode(&ServerResponse{ID: nodeID})
	if err != nil {
		panic(fmt.Sprintf("must encode response failed: %v", err))
	}
	jresp, err := json.Marshal(coded)
	if err != nil {
		panic(fmt.Sprintf("must encode response failed: %v", err))
	}

	resp := Response{Token: token, Type: p.Response_SERVER_INFO, Responses: []json.RawMessage{jresp}}
	err = enc.Encode(resp)
	if err != nil {
		panic(fmt.Sprintf("must encode failed: %v", err))
	}

	return buf2.Bytes()
}

func expectServerStatus(conn *connMock, token int64, nodeIDs []string, hosts []Host) {
	writeChan := make(chan struct{})
	readHChan := make(chan struct{})
	readRChan := make(chan struct{})

	rawQ := makeServerStatusQueryRaw(token)
	conn.On("Write", rawQ).Return(0, nil, nil).Once().Run(func(args mock.Arguments) {
		close(writeChan)
	})

	rawR := makeServerStatusResponseRaw(token, nodeIDs, hosts)
	rawH := makeResponseHeaderRaw(token, len(rawR))

	conn.On("Read", respHeaderLen).Return(rawH, len(rawH), nil, nil).Once().Run(func(args mock.Arguments) {
		<-writeChan
		close(readHChan)
	})
	conn.On("Read", len(rawR)).Return(rawR, len(rawR), nil, nil).Once().Run(func(args mock.Arguments) {
		<-readHChan
		close(readRChan)
	})

	rawQ2 := makeContinueQueryRaw(token)
	// maybe - connection may be closed until cursor fetchs next batch
	conn.On("Write", rawQ2).Return(0, nil, nil).Maybe().Run(func(args mock.Arguments) {
		<-readRChan
	})
}

func makeServerStatusQueryRaw(token int64) []byte {
	buf := &bytes.Buffer{}
	buf.Grow(respHeaderLen)
	buf.Write(buf.Bytes()[:respHeaderLen]) // reserve for header
	enc := json.NewEncoder(buf)

	t := DB(SystemDatabase).Table(ServerStatusSystemTable).Changes(ChangesOpts{IncludeInitial: true})
	q, err := newQuery(t, map[string]interface{}{}, &ConnectOpts{})
	if err != nil {
		panic(fmt.Sprintf("must newQuery failed: %v", err))
	}
	q.Token = token

	err = enc.Encode(q.Build())
	if err != nil {
		panic(fmt.Sprintf("must encode failed: %v", err))
	}

	b := buf.Bytes()
	binary.LittleEndian.PutUint64(b, uint64(q.Token))
	binary.LittleEndian.PutUint32(b[8:], uint32(len(b)-respHeaderLen))
	return b
}

func makeServerStatusResponseRaw(token int64, nodeIDs []string, hosts []Host) []byte {
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
			panic(fmt.Sprintf("must encode response failed: %v", err))
		}
		jresps[i], err = json.Marshal(coded)
		if err != nil {
			panic(fmt.Sprintf("must encode response failed: %v", err))
		}
	}

	resp := Response{Token: token, Type: p.Response_SUCCESS_PARTIAL, Responses: jresps}
	err := enc.Encode(resp)
	if err != nil {
		panic(fmt.Sprintf("must encode failed: %v", err))
	}
	return buf2.Bytes()
}

func makeContinueQueryRaw(token int64) []byte {
	buf := &bytes.Buffer{}
	buf.Grow(respHeaderLen)
	buf.Write(buf.Bytes()[:respHeaderLen]) // reserve for header
	enc := json.NewEncoder(buf)

	q := Query{Token: token, Type: p.Query_CONTINUE}
	err := enc.Encode(q.Build())
	if err != nil {
		panic(fmt.Sprintf("must encode failed: %v", err))
	}

	b := buf.Bytes()
	binary.LittleEndian.PutUint64(b, uint64(q.Token))
	binary.LittleEndian.PutUint32(b[8:], uint32(len(b)-respHeaderLen))
	return b
}
