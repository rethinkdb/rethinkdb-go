package gorethink

import (
	test "gopkg.in/check.v1"
	p "gopkg.in/gorethink/gorethink.v3/ql2"
	"golang.org/x/net/context"
	"encoding/binary"
	"encoding/json"
)

type ConnectionSuite struct{}

var _ = test.Suite(&ConnectionSuite{})

func (s *ConnectionSuite) TestConnection_Query_Ok(c *test.C) {
	ctx := context.Background()
	token := int64(1)
	q := testQuery()
	writeData := serializeQuery(token, q)
	respData := serializeAtomResponse()
	header := respHeader(token, respData)

	conn := &connMock{}
	conn.On("Write", writeData).Return(len(writeData), nil)
	conn.On("Read", respHeaderLen).Return(header, respHeaderLen, nil)
	conn.On("Read", len(respData)).Return(respData, len(respData), nil)

	connection := newConnection(conn, "addr", &ConnectOpts{})
	connection.runConnection()
	response, cursor, err := connection.Query(ctx, q)

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

func testQuery() Query {
	q, _ := newQuery(
		DB("db").Table("table").Get("id"),
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
