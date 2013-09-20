package gorethink

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

	return c
}

// Connect opens a connection between the driver and the client
func Connect(args map[string]interface{}) (*Connection, error) {
	c := newConnection(args)
	err := c.Reconnect()

	return c, err
}

// Reconnect closes the previous connection and attempts to connect again.
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
		return RqlDriverError{fmt.Sprintf("Server dropped connection with message: \"%s\"", response)}
	}

	c.conn = conn
	c.closed = false

	return nil
}

// Close closes the connection
func (c *Connection) Close() error {
	if c.conn == nil || c.closed {
		return nil
	}

	err := c.conn.Close()
	c.closed = true

	return err
}

// Use changes the default database used
func (c *Connection) Use(database string) {
	c.database = database
}

// getToken generates the next query token, used to number requests and match
// responses with requests.
func (c *Connection) nextToken() int64 {
	return atomic.AddInt64(&c.token, 1)
}

// startQuery creates a query from the term given and sends it to the server.
// The result from the server is returned as ResultRows
func (c *Connection) startQuery(t RqlTerm, opts map[string]interface{}) (*ResultRows, error) {
	token := c.nextToken()

	// Build query tree
	pt := t.build()

	// Build global options
	globalOpts := []*p.Query_AssocPair{}
	for k, v := range opts {
		if k == "db" {
			globalOpts = append(globalOpts, &p.Query_AssocPair{
				Key: proto.String("db"),
				Val: Db(v).build(),
			})
		} else if k == "use_outdated" {
			globalOpts = append(globalOpts, &p.Query_AssocPair{
				Key: proto.String("use_outdated"),
				Val: Expr(v).build(),
			})
		} else if k == "noreply" {
			globalOpts = append(globalOpts, &p.Query_AssocPair{
				Key: proto.String("noreply"),
				Val: Expr(v).build(),
			})
		}
	}
	// If no DB option was set default to the value set in the connection
	if _, ok := opts["db"]; !ok {
		globalOpts = append(globalOpts, &p.Query_AssocPair{
			Key: proto.String("db"),
			Val: Db(c.database).build(),
		})
	}

	// Construct query
	query := &p.Query{
		Type:          p.Query_START.Enum(),
		Token:         proto.Int64(token),
		Query:         pt,
		GlobalOptargs: globalOpts,
	}

	return c.send(query, t, opts)
}

// continueQuery continues a previously run query.
func (c *Connection) continueQuery(q *p.Query, t RqlTerm, opts map[string]interface{}) (*ResultRows, error) {
	nq := &p.Query{
		Type:  p.Query_CONTINUE.Enum(),
		Token: q.Token,
	}

	return c.send(nq, t, opts)
}

// stopQuery sends closes a query by sending Query_STOP to the server.
func (c *Connection) stopQuery(q *p.Query, t RqlTerm, opts map[string]interface{}) (*ResultRows, error) {
	nq := &p.Query{
		Type:  p.Query_STOP.Enum(),
		Token: q.Token,
	}

	return c.send(nq, t, opts)
}

func (c *Connection) send(q *p.Query, t RqlTerm, opts map[string]interface{}) (*ResultRows, error) {
	var data []byte
	var err error

	// Ensure that the connection is not closed
	if c.closed {
		return nil, RqlDriverError{"Connection is closed"}
	}

	// Set timeout
	if c.timeout == 0 {
		c.conn.SetDeadline(time.Time{})
	} else {
		c.conn.SetDeadline(time.Now().Add(c.timeout))
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

	// Return immediately if the noreply option was set
	if noreply, ok := opts["noreply"]; ok && noreply.(bool) {
		return nil, nil
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
		return nil, RqlDriverError{"Unexpected response received."}
	}

	// De-construct datum and return the result
	switch r.GetType() {
	case p.Response_CLIENT_ERROR:
		return nil, RqlClientError{rqlResponseError{r, t}}
	case p.Response_COMPILE_ERROR:
		return nil, RqlCompileError{rqlResponseError{r, t}}
	case p.Response_RUNTIME_ERROR:
		return nil, RqlRuntimeError{rqlResponseError{r, t}}
	case p.Response_SUCCESS_PARTIAL, p.Response_SUCCESS_SEQUENCE:
		return &ResultRows{
			conn:         c,
			query:        q,
			term:         t,
			opts:         opts,
			buffer:       r.GetResponse(),
			end:          len(r.GetResponse()),
			token:        q.GetToken(),
			responseType: r.GetType(),
		}, nil
	case p.Response_SUCCESS_ATOM:
		return &ResultRows{
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
		return nil, RqlDriverError{fmt.Sprintf("Unexpected response type received: %s", r.GetType())}
	}
}
