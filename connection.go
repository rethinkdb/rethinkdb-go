package rethinkgo

import (
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"fmt"
	p "github.com/dancannon/gorethink/ql2"
	"io"
	"net"
	"sync/atomic"
	"time"
)

type Connection struct {
	// The underlying Connection
	conn net.Conn

	token      int64
	address    string
	database   string
	timeout    time.Duration
	authkey    string
	timeFormat string
	debug      bool
	closed     bool
}

func newConnection(args map[string]interface{}) *Connection {
	c := &Connection{}

	if token, ok := args["token"]; ok {
		c.token = token.(int64)
	}
	if address, ok := args["address"]; ok {
		c.address = address.(string)
	}
	if database, ok := args["database"]; ok {
		c.database = database.(string)
	}
	if timeout, ok := args["timeout"]; ok {
		c.timeout = timeout.(time.Duration)
	}
	if authkey, ok := args["authkey"]; ok {
		c.authkey = authkey.(string)
	}
	if debug, ok := args["debug"]; ok {
		c.debug = debug.(bool)
	}

	return c
}

func Connect(args map[string]interface{}) (*Connection, error) {
	c := newConnection(args)
	err := c.Reconnect()

	return c, err
}

func (c *Connection) Reconnect() error {
	var err error
	if err = c.Close(); err != nil {
		return err
	}

	conn, err := net.Dial("tcp", c.address)
	if err != nil {
		return err
	}

	if err := binary.Write(conn, binary.LittleEndian, p.VersionDummy_V0_2); err != nil {
		return err
	}

	// authorization key
	if err := binary.Write(conn, binary.LittleEndian, uint32(len(c.authkey))); err != nil {
		return err
	}

	if err := binary.Write(conn, binary.BigEndian, []byte(c.authkey)); err != nil {
		return err
	}

	// read server response to authorization key (terminated by NUL)
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\x00')
	if err != nil {
		return err
	}
	// convert to string and remove trailing NUL byte
	response := string(line[:len(line)-1])
	if response != "SUCCESS" {
		// we failed authorization or something else terrible happened
		return fmt.Errorf("Failed to connect to server: %v", response)
	}

	c.conn = conn
	c.closed = false

	return nil
}

func (c *Connection) Close() error {
	if c.conn == nil || c.closed {
		return nil
	}

	err := c.conn.Close()
	c.closed = true

	return err
}

func (c *Connection) Use(database string) {
	c.database = database
}

// getToken generates the next query token, used to number requests and match
// responses with requests.
func (c *Connection) nextToken() int64 {
	return atomic.AddInt64(&c.token, 1)
}

func (c *Connection) startQuery(t RqlTerm, opts map[string]interface{}) (*Rows, error) {
	token := c.nextToken()

	// Build query tree
	pt := t.build()

	// Construct query
	query := &p.Query{
		Type:  p.Query_START.Enum(),
		Token: proto.Int64(token),
		Query: pt,
	}

	// Set global defaults
	// TODO:

	return c.send(query, t, opts)
}

func (c *Connection) continueQuery(q *p.Query, t RqlTerm, opts map[string]interface{}) (*Rows, error) {
	nq := &p.Query{
		Type:  p.Query_CONTINUE.Enum(),
		Token: q.Token,
	}

	return c.send(nq, t, opts)
}

func (c *Connection) stopQuery(q *p.Query, t RqlTerm, opts map[string]interface{}) (*Rows, error) {
	nq := &p.Query{
		Type:  p.Query_STOP.Enum(),
		Token: q.Token,
	}

	return c.send(nq, t, opts)
}

func (c *Connection) send(q *p.Query, t RqlTerm, opts map[string]interface{}) (*Rows, error) {
	var data []byte
	var err error

	// Ensure that the connection is not closed
	if c.closed {
		return nil, fmt.Errorf("Connection is closed.")
	}

	// Send query
	if data, err = proto.Marshal(q); err != nil {
		return nil, err
	}
	if err = binary.Write(c.conn, binary.LittleEndian, uint32(len(data))); err != nil {
		return nil, err
	}

	if err = binary.Write(c.conn, binary.BigEndian, data); err != nil {
		return nil, err
	}

	// Read response
	var messageLength uint32
	if err := binary.Read(c.conn, binary.LittleEndian, &messageLength); err != nil {
		return nil, err
	}

	buffer := make([]byte, messageLength)
	_, err = io.ReadFull(c.conn, buffer)
	if err != nil {
		return nil, err
	}

	r := &p.Response{}
	err = proto.Unmarshal(buffer, r)
	if err != nil {
		return nil, err
	}

	// Ensure that this is the response we were expecting
	if q.GetToken() != r.GetToken() {
		return &Rows{}, fmt.Errorf("Unexpected response received.")
	}

	// Deconstruct datum and return the result
	switch r.GetType() {
	case p.Response_SUCCESS_ATOM:
		if len(r.GetResponse()) < 1 {
			return &Rows{}, nil
		} else {
			return &Rows{
				conn:         c,
				query:        q,
				term:         t,
				opts:         opts,
				buffer:       r.GetResponse(),
				end:          len(r.GetResponse()),
				token:        q.GetToken(),
				responseType: r.GetType(),
			}, nil
		}
	case p.Response_SUCCESS_PARTIAL, p.Response_SUCCESS_SEQUENCE:
		return &Rows{
			conn:         c,
			query:        q,
			term:         t,
			opts:         opts,
			buffer:       r.GetResponse(),
			end:          len(r.GetResponse()),
			token:        q.GetToken(),
			responseType: r.GetType(),
		}, nil
	default:
		data, err := deconstructDatum(r.GetResponse()[0], opts)
		return nil, fmt.Errorf("%v, %v", data, err)
	}

	return nil, nil
}
