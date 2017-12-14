package gorethink

import (
	test "gopkg.in/check.v1"
	p "gopkg.in/gorethink/gorethink.v4/ql2"
	"golang.org/x/net/context"
	"encoding/binary"
	"encoding/json"
	"io"
	"time"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/opentracing/opentracing-go"
)

type ConnectionSuite struct{}

var _ = test.Suite(&ConnectionSuite{})

func (s *ConnectionSuite) TestConnection_Query_Ok(c *test.C) {
	ctx := context.Background()
	token := int64(1)
	q := testQuery(DB("db").Table("table").Get("id"))
	writeData := serializeQuery(token, q)
	respData := serializeAtomResponse()
	header := respHeader(token, respData)

	conn := &connMock{}
	conn.On("Write", writeData).Return(len(writeData), nil)
	conn.On("Read", respHeaderLen).Return(header, respHeaderLen, nil)
	conn.On("Read", len(respData)).Return(respData, len(respData), nil)
	conn.On("Close").Return(nil)

	connection := newConnection(conn, "addr", &ConnectOpts{})
	connection.runConnection()
	response, cursor, err := connection.Query(ctx, q)
	connection.Close()

	c.Assert(response, test.NotNil)
	c.Assert(response.Token, test.Equals, token)
	c.Assert(response.Type, test.Equals, p.Response_SUCCESS_ATOM)
	c.Assert(response.Responses, test.HasLen, 1)
	c.Assert(response.Responses[0], test.DeepEquals, json.RawMessage([]byte(`"response"`)))
	c.Assert(cursor, test.NotNil)
	c.Assert(cursor.token, test.Equals, token)
	c.Assert(cursor.conn, test.Equals, connection)
	c.Assert(cursor.ctx, test.Equals, ctx)
	c.Assert(cursor.responses, test.DeepEquals, response.Responses)
	c.Assert(err, test.IsNil)
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_Query_DefaultDBOk(c *test.C) {
	ctx := context.Background()
	token := int64(1)
	q := testQuery(Table("table").Get("id"),)
	q2 := q
	q2.Opts["db"], _ = DB("db").Build()
	writeData := serializeQuery(token, q2)
	respData := serializeAtomResponse()
	header := respHeader(token, respData)

	conn := &connMock{}
	conn.On("Write", writeData).Return(len(writeData), nil)
	conn.On("Read", respHeaderLen).Return(header, respHeaderLen, nil)
	conn.On("Read", len(respData)).Return(respData, len(respData), nil)
	conn.On("Close").Return(nil)

	connection := newConnection(conn, "addr", &ConnectOpts{Database: "db"})
	connection.runConnection()
	response, cursor, err := connection.Query(ctx, q)
	connection.Close()

	c.Assert(response, test.NotNil)
	c.Assert(response.Token, test.Equals, token)
	c.Assert(response.Type, test.Equals, p.Response_SUCCESS_ATOM)
	c.Assert(response.Responses, test.HasLen, 1)
	c.Assert(response.Responses[0], test.DeepEquals, json.RawMessage([]byte(`"response"`)))
	c.Assert(cursor, test.NotNil)
	c.Assert(cursor.token, test.Equals, token)
	c.Assert(cursor.conn, test.Equals, connection)
	c.Assert(cursor.ctx, test.Equals, ctx)
	c.Assert(cursor.responses, test.DeepEquals, response.Responses)
	c.Assert(err, test.IsNil)
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_Query_Nil(c *test.C) {
	response, cursor, err := (*Connection)(nil).Query(nil, Query{})
	c.Assert(err, test.Equals, ErrConnectionClosed)
	c.Assert(response, test.IsNil)
	c.Assert(cursor, test.IsNil)
}

func (s *ConnectionSuite) TestConnection_Query_NilConn(c *test.C) {
	connection := newConnection(nil, "addr", &ConnectOpts{Database: "db"})
	response, cursor, err := connection.Query(nil, Query{})
	c.Assert(err, test.Equals, ErrConnectionClosed)
	c.Assert(response, test.IsNil)
	c.Assert(cursor, test.IsNil)
}

func (s *ConnectionSuite) TestConnection_Query_SendFail(c *test.C) {
	ctx := context.Background()
	token := int64(1)
	q := testQuery(DB("db").Table("table").Get("id"))
	writeData := serializeQuery(token, q)

	conn := &connMock{}
	conn.On("Write", writeData).Return(0, io.EOF)

	connection := newConnection(conn, "addr", &ConnectOpts{})
	response, cursor, err := connection.Query(ctx, q)

	c.Assert(response, test.IsNil)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.Equals, RQLConnectionError{rqlError(io.EOF.Error())})
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_Query_NoReplyOk(c *test.C) {
	token := int64(1)
	q := testQuery(DB("db").Table("table").Get("id"))
	q.Opts["noreply"] = true
	writeData := serializeQuery(token, q)
	respData := serializeAtomResponse()
	header := respHeader(token, respData)

	conn := &connMock{}
	conn.On("Write", writeData).Return(len(writeData), nil)
	conn.On("Read", respHeaderLen).Return(header, respHeaderLen, nil)
	conn.On("Read", len(respData)).Return(respData, len(respData), nil)
	conn.On("Close").Return(nil)

	connection := newConnection(conn, "addr", &ConnectOpts{})
	connection.runConnection()
	response, cursor, err := connection.Query(nil, q)
	time.Sleep(5*time.Millisecond)
	connection.Close()

	c.Assert(response, test.IsNil)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.IsNil)
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_Query_NoReplyTimeoutWrite(c *test.C) {
	ctx, cancel := context.WithCancel(context.Background())
	token := int64(1)
	q := testQuery(DB("db").Table("table").Get("id"))
	q.Opts["noreply"] = true
	writeData := serializeQuery(token, q)
	stopData := serializeQuery(token, newStopQuery(token))

	conn := &connMock{}
	conn.On("Write", writeData).Return(len(writeData), nil)
	conn.On("Write", stopData).Return(len(stopData), nil)
	conn.On("SetWriteDeadline").Return(nil)

	connection := newConnection(conn, "addr", &ConnectOpts{ReadTimeout: time.Millisecond, WriteTimeout: time.Millisecond})
	connection.readRequestsChan = make(chan tokenAndPromise, 0)
	cancel()
	response, cursor, err := connection.Query(ctx, q)

	c.Assert(response, test.IsNil)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.Equals, ErrQueryTimeout)
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_Query_TimeoutWrite(c *test.C) {
	ctx, cancel := context.WithCancel(context.Background())
	token := int64(1)
	q := testQuery(DB("db").Table("table").Get("id"))
	writeData := serializeQuery(token, q)
	stopData := serializeQuery(token, newStopQuery(token))

	conn := &connMock{}
	conn.On("Write", writeData).Return(len(writeData), nil)
	conn.On("Write", stopData).Return(len(stopData), nil)
	conn.On("SetWriteDeadline").Return(nil)

	connection := newConnection(conn, "addr", &ConnectOpts{ReadTimeout: time.Millisecond, WriteTimeout: time.Millisecond})
	connection.readRequestsChan = make(chan tokenAndPromise, 0)
	cancel()
	response, cursor, err := connection.Query(ctx, q)

	c.Assert(response, test.IsNil)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.Equals, ErrQueryTimeout)
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_Query_TimeoutRead(c *test.C) {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Millisecond)
	token := int64(1)
	q := testQuery(DB("db").Table("table").Get("id"))
	writeData := serializeQuery(token, q)
	stopData := serializeQuery(token, newStopQuery(token))

	conn := &connMock{}
	conn.On("Write", writeData).Return(len(writeData), nil)
	conn.On("Write", stopData).Return(len(stopData), nil)
	conn.On("SetWriteDeadline").Return(nil)

	connection := newConnection(conn, "addr", &ConnectOpts{ReadTimeout: time.Millisecond, WriteTimeout: time.Millisecond})
	response, cursor, err := connection.Query(ctx, q)

	c.Assert(response, test.IsNil)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.Equals, ErrQueryTimeout)
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_Query_SendFailTracing(c *test.C) {
	tracer := mocktracer.New()
	rootSpan := tracer.StartSpan("root")
	ctx := opentracing.ContextWithSpan(context.Background(), rootSpan)
	token := int64(1)
	q := testQuery(DB("db").Table("table").Get("id"))
	writeData := serializeQuery(token, q)

	conn := &connMock{}
	conn.On("Write", writeData).Return(0, io.EOF)

	connection := newConnection(conn, "addr", &ConnectOpts{UseOpentracing: true})
	response, cursor, err := connection.Query(ctx, q)

	c.Assert(response, test.IsNil)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.Equals, RQLConnectionError{rqlError(io.EOF.Error())})
	conn.AssertExpectations(c)
	c.Assert(tracer.FinishedSpans(), test.HasLen, 2)
}

func (s *ConnectionSuite) TestConnection_processResponses_SocketErr(c *test.C) {
	promise1 := make(chan responseAndCursor, 1)
	promise2 := make(chan responseAndCursor, 1)
	promise3 := make(chan responseAndCursor, 1)

	conn := &connMock{}
	conn.On("Close").Return(nil)

	connection := newConnection(conn, "addr", &ConnectOpts{})

	go connection.processResponses()

	connection.readRequestsChan <- tokenAndPromise{query: &Query{Token: 1}, promise: promise1}
	connection.readRequestsChan <- tokenAndPromise{query: &Query{Token: 2}, promise: promise2}
	connection.readRequestsChan <- tokenAndPromise{query: &Query{Token: 2}, promise: promise3}
	time.Sleep(5*time.Millisecond)
	connection.responseChan <- responseAndError{err: io.EOF}
	time.Sleep(5*time.Millisecond)

	select {
	case f := <-promise1:
		c.Assert(f.err, test.Equals, io.EOF)
		c.Assert(f.response, test.IsNil)
	default:
		c.Fail()
	}
	select {
	case f := <-promise2:
		c.Assert(f.err, test.Equals, io.EOF)
		c.Assert(f.response, test.IsNil)
	default:
		c.Fail()
	}
	select {
	case f := <-promise3:
		c.Assert(f.err, test.Equals, io.EOF)
		c.Assert(f.response, test.IsNil)
	default:
		c.Fail()
	}
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_processResponses_StopOk(c *test.C) {
	promise1 := make(chan responseAndCursor, 1)

	connection := newConnection(nil, "addr", &ConnectOpts{})

	go connection.processResponses()

	connection.readRequestsChan <- tokenAndPromise{query: &Query{Token: 1}, promise: promise1}
	close(connection.responseChan)
	time.Sleep(5*time.Millisecond)
	close(connection.stopReadChan)
	time.Sleep(5*time.Millisecond)

	select {
	case f := <-promise1:
		c.Assert(f.err, test.Equals, ErrConnectionClosed)
		c.Assert(f.response, test.IsNil)
	default:
		c.Fail()
	}
}

func (s *ConnectionSuite) TestConnection_processResponses_ResponseFirst(c *test.C) {
	promise1 := make(chan responseAndCursor, 1)
	response1 := &Response{Token:1, Type: p.Response_RUNTIME_ERROR, ErrorType: p.Response_INTERNAL}

	conn := &connMock{}
	conn.On("Close").Return(nil)

	connection := newConnection(conn, "addr", &ConnectOpts{})

	go connection.processResponses()

	connection.responseChan <- responseAndError{response: response1}
	time.Sleep(5*time.Millisecond)
	connection.readRequestsChan <- tokenAndPromise{query: &Query{Token: 1}, promise: promise1}
	time.Sleep(5*time.Millisecond)
	connection.Close()
	time.Sleep(5*time.Millisecond)

	select {
	case f := <-promise1:
		c.Assert(f.err, test.FitsTypeOf, RQLInternalError{})
		c.Assert(f.response, test.Equals, response1)
		c.Assert(f.cursor, test.IsNil)
	default:
		c.Fail()
	}
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_readResponse_TimeoutHeader(c *test.C) {
	timeout := time.Second

	conn := &connMock{}
	conn.On("SetReadDeadline").Return(nil)
	conn.On("Read", respHeaderLen).Return(nil, 0, io.EOF)

	connection := newConnection(conn, "addr", &ConnectOpts{ReadTimeout: timeout})

	response, err := connection.readResponse()

	c.Assert(response, test.IsNil)
	c.Assert(err, test.FitsTypeOf, RQLConnectionError{})
	c.Assert(connection.isBad(), test.Equals, true)
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_readResponse_BodySocketErr(c *test.C) {
	token := int64(5)
	respData := serializeAtomResponse()
	header := respHeader(token, respData)

	conn := &connMock{}
	conn.On("Read", respHeaderLen).Return(header, len(header), nil)
	conn.On("Read", len(respData)).Return(nil, 0, io.EOF)

	connection := newConnection(conn, "addr", &ConnectOpts{})

	response, err := connection.readResponse()

	c.Assert(response, test.IsNil)
	c.Assert(err, test.FitsTypeOf, RQLConnectionError{})
	c.Assert(connection.isBad(), test.Equals, true)
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_readResponse_BodyUnmarshalErr(c *test.C) {
	token := int64(5)
	respData := serializeAtomResponse()
	header := respHeader(token, respData)

	conn := &connMock{}
	conn.On("Read", respHeaderLen).Return(header, len(header), nil)
	conn.On("Read", len(respData)).Return(make([]byte, len(respData)), len(respData), nil)

	connection := newConnection(conn, "addr", &ConnectOpts{})

	response, err := connection.readResponse()

	c.Assert(response, test.IsNil)
	c.Assert(err, test.FitsTypeOf, RQLDriverError{})
	c.Assert(connection.isBad(), test.Equals, true)
	conn.AssertExpectations(c)
}

func (s *ConnectionSuite) TestConnection_processResponse_ClientErrOk(c *test.C) {
	ctx := context.Background()
	token := int64(3)
	q := Query{Token: token}
	response := &Response{Token: token, Type: p.Response_CLIENT_ERROR}

	connection := newConnection(nil, "addr", &ConnectOpts{})

	resp, cursor, err := connection.processResponse(ctx, q, response, nil)

	c.Assert(resp, test.Equals, response)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.FitsTypeOf, RQLClientError{})
}

func (s *ConnectionSuite) TestConnection_processResponse_CompileErrOk(c *test.C) {
	ctx := context.Background()
	token := int64(3)
	q := Query{Token: token}
	response := &Response{Token: token, Type: p.Response_COMPILE_ERROR}

	connection := newConnection(nil, "addr", &ConnectOpts{})

	resp, cursor, err := connection.processResponse(ctx, q, response, nil)

	c.Assert(resp, test.Equals, response)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.FitsTypeOf, RQLCompileError{})
}

func (s *ConnectionSuite) TestConnection_processResponse_RuntimeErrOk(c *test.C) {
	tracer := mocktracer.New()
	rootSpan := tracer.StartSpan("root")
	ctx := opentracing.ContextWithSpan(context.Background(), rootSpan)
	qSpan := rootSpan.Tracer().StartSpan("q", opentracing.ChildOf(rootSpan.Context()))

	token := int64(3)
	term := Table("test")
	q := Query{Token: token, Term: &term}
	response := &Response{Token: token, Type: p.Response_RUNTIME_ERROR, Responses: []json.RawMessage{{'e', 'r', 'r'}}}

	connection := newConnection(nil, "addr", &ConnectOpts{})

	resp, cursor, err := connection.processResponse(ctx, q, response, qSpan)

	c.Assert(resp, test.Equals, response)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.FitsTypeOf, RQLRuntimeError{})
	c.Assert(tracer.FinishedSpans(), test.HasLen, 1)
	c.Assert(tracer.FinishedSpans()[0].Tags()["error"], test.Equals, true)
}

func (s *ConnectionSuite) TestConnection_processResponse_FirstPartialOk(c *test.C) {
	ctx := context.Background()
	token := int64(3)
	q := Query{Token: token}
	rawResponse1 := json.RawMessage{1,2,3}
	rawResponse2 := json.RawMessage{3,4,5}
	response := &Response{Token: token, Type: p.Response_SUCCESS_PARTIAL, Responses: []json.RawMessage{rawResponse1, rawResponse2}}

	connection := newConnection(nil, "addr", &ConnectOpts{})

	resp, cursor, err := connection.processResponse(ctx, q, response, nil)

	c.Assert(resp, test.Equals, response)
	c.Assert(cursor, test.NotNil)
	c.Assert(cursor.token, test.Equals, token)
	c.Assert(cursor.ctx, test.Equals, ctx)
	c.Assert(cursor.responses, test.HasLen, 2)
	c.Assert(cursor.responses[0], test.DeepEquals, rawResponse1)
	c.Assert(cursor.responses[1], test.DeepEquals, rawResponse2)
	c.Assert(cursor.conn, test.Equals, connection)
	c.Assert(err, test.IsNil)
	c.Assert(connection.cursors, test.HasLen, 1)
	c.Assert(connection.cursors[token], test.Equals, cursor)
}

func (s *ConnectionSuite) TestConnection_processResponse_PartialOk(c *test.C) {
	ctx := context.Background()
	token := int64(3)
	term := Table("test")
	q := Query{Token: token}
	rawResponse1 := json.RawMessage{1,2,3}
	rawResponse2 := json.RawMessage{3,4,5}
	response := &Response{Token: token, Type: p.Response_SUCCESS_PARTIAL, Responses: []json.RawMessage{rawResponse1, rawResponse2}}

	connection := newConnection(nil, "addr", &ConnectOpts{})
	oldCursor := newCursor(ctx, connection, "Cursor", token, &term, q.Opts)
	connection.cursors[token] = oldCursor

	resp, cursor, err := connection.processResponse(ctx, q, response, nil)

	c.Assert(resp, test.Equals, response)
	c.Assert(cursor, test.Equals, oldCursor)
	c.Assert(cursor.responses, test.HasLen, 2)
	c.Assert(cursor.responses[0], test.DeepEquals, rawResponse1)
	c.Assert(cursor.responses[1], test.DeepEquals, rawResponse2)
	c.Assert(err, test.IsNil)
	c.Assert(connection.cursors, test.HasLen, 1)
	c.Assert(connection.cursors[token], test.Equals, cursor)
}

func (s *ConnectionSuite) TestConnection_processResponse_SequenceOk(c *test.C) {
	tracer := mocktracer.New()
	rootSpan := tracer.StartSpan("root")
	ctx := opentracing.ContextWithSpan(context.Background(), rootSpan)
	qSpan := rootSpan.Tracer().StartSpan("q", opentracing.ChildOf(rootSpan.Context()))

	token := int64(3)
	q := Query{Token: token}
	rawResponse1 := json.RawMessage{1,2,3}
	rawResponse2 := json.RawMessage{3,4,5}
	response := &Response{Token: token, Type: p.Response_SUCCESS_SEQUENCE, Responses: []json.RawMessage{rawResponse1, rawResponse2}}

	connection := newConnection(nil, "addr", &ConnectOpts{})

	resp, cursor, err := connection.processResponse(ctx, q, response, qSpan)

	c.Assert(resp, test.Equals, response)
	c.Assert(cursor, test.NotNil)
	c.Assert(cursor.token, test.Equals, token)
	c.Assert(cursor.ctx, test.Equals, ctx)
	c.Assert(cursor.responses, test.HasLen, 2)
	c.Assert(cursor.responses[0], test.DeepEquals, rawResponse1)
	c.Assert(cursor.responses[1], test.DeepEquals, rawResponse2)
	c.Assert(cursor.conn, test.Equals, connection)
	c.Assert(err, test.IsNil)
	c.Assert(connection.cursors, test.HasLen, 0)
	c.Assert(tracer.FinishedSpans(), test.HasLen, 1)
	c.Assert(tracer.FinishedSpans()[0].Tags(), test.HasLen, 0)
}

func (s *ConnectionSuite) TestConnection_processResponse_WaitOk(c *test.C) {
	ctx := context.Background()
	token := int64(3)
	q := Query{Token: token}
	response := &Response{Token: token, Type: p.Response_WAIT_COMPLETE}

	connection := newConnection(nil, "addr", &ConnectOpts{})
	connection.cursors[token] = &Cursor{}

	resp, cursor, err := connection.processResponse(ctx, q, response, nil)

	c.Assert(resp, test.Equals, response)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.IsNil)
	c.Assert(connection.cursors, test.HasLen, 0)
}

func (s *ConnectionSuite) TestConnection_processResponse_UnexpectedOk(c *test.C) {
	ctx := context.Background()
	token := int64(3)
	q := Query{Token: token}
	response := &Response{Token: token, Type: 99}

	connection := newConnection(nil, "addr", &ConnectOpts{})

	resp, cursor, err := connection.processResponse(ctx, q, response, nil)

	c.Assert(resp, test.IsNil)
	c.Assert(cursor, test.IsNil)
	c.Assert(err, test.FitsTypeOf, RQLDriverError{})
}

func testQuery(t Term) Query {
	q, _ := newQuery(
		t,
		map[string]interface{}{},
		&ConnectOpts{},
	)
	return q
}

func respHeader(token int64, msg []byte) []byte {
	header := [respHeaderLen]byte{}
	binary.LittleEndian.PutUint64(header[:], uint64(token))
	binary.LittleEndian.PutUint32(header[8:], uint32(len(msg)))
	return header[:]
}

func serializeQuery(token int64, q Query) []byte {
	b, _ := json.Marshal(q.Build())
	msg := make([]byte, len(b)+respHeaderLen+1)
	binary.LittleEndian.PutUint64(msg, uint64(token))
	binary.LittleEndian.PutUint32(msg[8:], uint32(len(b)+1))
	copy(msg[respHeaderLen:], b)
	msg[len(msg)-1] = '\n' // encoder.Marshal do this, json.Marshal doesn't
	return msg
}

func serializeAtomResponse() []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"t": p.Response_SUCCESS_ATOM,
		"r": []interface{}{"response"},
	})
	return b
}
