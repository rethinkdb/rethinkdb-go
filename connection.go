package gorethink

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"sync/atomic"
	"time"

	p "github.com/dancannon/gorethink/ql2"
)

const (
	respHeaderLen = 12
)

type Response struct {
	Token     int64
	Type      p.Response_ResponseType `json:"t"`
	Responses []json.RawMessage       `json:"r"`
	Backtrace []interface{}           `json:"b"`
	Profile   interface{}             `json:"p"`
}

// Connection is a connection to a rethinkdb database. Connection is not thread
// safe and should only be accessed be a single goroutine
type Connection struct {
	conn    net.Conn
	opts    *ConnectOpts
	token   int64
	cursors map[int64]*Cursor
	bad     bool

	headerBuf [respHeaderLen]byte
	buf       buffer
}

// Dial closes the previous connection and attempts to connect again.
func NewConnection(opts *ConnectOpts) (*Connection, error) {
	var err error

	// New mysqlConn
	c := &Connection{
		opts:    opts,
		cursors: make(map[int64]*Cursor),
	}

	// Connect to Server
	nd := net.Dialer{Timeout: c.opts.Timeout}
	c.conn, err = nd.Dial("tcp", c.opts.Address)
	if err != nil {
		return nil, err
	}

	// Enable TCP Keepalives on TCP connections
	if tc, ok := c.conn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			// Don't send COM_QUIT before handshake.
			c.conn.Close()
			c.conn = nil
			return nil, err
		}
	}

	c.buf = newBuffer(c.conn)

	// Send handshake request
	if err = c.writeHandshakeReq(); err != nil {
		c.Close()
		return nil, err
	}

	// Read handshake response
	err = c.readHandshakeSuccess()
	if err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

// Close closes the underlying net.Conn
func (c *Connection) Close() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	c.cursors = nil
	c.opts = nil

	return nil
}

func (c *Connection) Query(q Query) (*Response, *Cursor, error) {
	if c == nil {
		return nil, nil, nil
	}
	if c.conn == nil {
		c.bad = true
		return nil, nil, nil
	}

	// Add token if query is a START/NOREPLY_WAIT
	if q.Type == p.Query_START || q.Type == p.Query_NOREPLY_WAIT {
		q.Token = c.nextToken()
		if c.opts.Database != "" {
			q.Opts["db"] = Db(c.opts.Database).build()
		}
	}

	err := c.sendQuery(q)
	if err != nil {
		return nil, nil, err
	}

	if noreply, ok := q.Opts["noreply"]; ok && noreply.(bool) {
		return nil, nil, nil
	}

	for {
		response, err := c.readResponse()
		if err != nil {
			return nil, nil, err
		}

		if response.Token == q.Token {
			// If this was the requested response process and return
			return c.processResponse(q, response)
		} else if _, ok := c.cursors[response.Token]; ok {
			// If the token is in the cursor cache then process the response
			c.processResponse(q, response)
		} else {
			putResponse(response)
		}
	}
}

func (c *Connection) sendQuery(q Query) error {
	// Build query
	b, err := json.Marshal(q.build())
	if err != nil {
		return RqlDriverError{"Error building query"}
	}

	// Set timeout
	if c.opts.Timeout == 0 {
		c.conn.SetDeadline(time.Time{})
	} else {
		c.conn.SetDeadline(time.Now().Add(c.opts.Timeout))
	}

	// Send the JSON encoding of the query itself.
	if err = c.writeQuery(q.Token, b); err != nil {
		c.bad = true
		return RqlConnectionError{err.Error()}
	}

	return nil
}

// getToken generates the next query token, used to number requests and match
// responses with requests.
func (c *Connection) nextToken() int64 {
	return atomic.AddInt64(&c.token, 1)
}

func (c *Connection) readResponse() (*Response, error) {
	// Read response header (token+length)
	_, err := io.ReadFull(c.conn, c.headerBuf[:respHeaderLen])
	if err != nil {
		return nil, err
	}

	responseToken := int64(binary.LittleEndian.Uint64(c.headerBuf[:8]))
	messageLength := binary.LittleEndian.Uint32(c.headerBuf[8:])

	// Read the JSON encoding of the Response itself.
	b := c.buf.takeBuffer(int(messageLength))
	if _, err := io.ReadFull(c.conn, b[:]); err != nil {
		c.bad = true
		return nil, RqlConnectionError{err.Error()}
	}

	// Decode the response
	var response = newCachedResponse()
	if err := json.Unmarshal(b, response); err != nil {
		c.bad = true
		return nil, RqlDriverError{err.Error()}
	}
	response.Token = responseToken

	return response, nil
}

func (c *Connection) processResponse(q Query, response *Response) (*Response, *Cursor, error) {
	switch response.Type {
	case p.Response_CLIENT_ERROR:
		return c.processErrorResponse(q, response, RqlClientError{rqlResponseError{response, q.Term}})
	case p.Response_COMPILE_ERROR:
		return c.processErrorResponse(q, response, RqlCompileError{rqlResponseError{response, q.Term}})
	case p.Response_RUNTIME_ERROR:
		return c.processErrorResponse(q, response, RqlRuntimeError{rqlResponseError{response, q.Term}})
	case p.Response_SUCCESS_ATOM:
		return c.processAtomResponse(q, response)
	case p.Response_SUCCESS_FEED, p.Response_SUCCESS_ATOM_FEED:
		return c.processFeedResponse(q, response)
	case p.Response_SUCCESS_PARTIAL:
		return c.processPartialResponse(q, response)
	case p.Response_SUCCESS_SEQUENCE:
		return c.processSequenceResponse(q, response)
	case p.Response_WAIT_COMPLETE:
		return c.processWaitResponse(q, response)
	default:
		putResponse(response)
		return nil, nil, RqlDriverError{"Unexpected response type"}
	}
}

func (c *Connection) processErrorResponse(q Query, response *Response, err error) (*Response, *Cursor, error) {
	cursor := c.cursors[response.Token]

	delete(c.cursors, response.Token)

	return response, cursor, err
}

func (c *Connection) processAtomResponse(q Query, response *Response) (*Response, *Cursor, error) {
	// Create cursor
	cursor := newCursor(c, response.Token, q.Term, q.Opts)
	cursor.profile = response.Profile

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processFeedResponse(q Query, response *Response) (*Response, *Cursor, error) {
	var cursor *Cursor
	if _, ok := c.cursors[response.Token]; !ok {
		// Create a new cursor if needed
		cursor = newCursor(c, response.Token, q.Term, q.Opts)
		cursor.profile = response.Profile
		c.cursors[response.Token] = cursor
	} else {
		cursor = c.cursors[response.Token]
	}

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processPartialResponse(q Query, response *Response) (*Response, *Cursor, error) {
	cursor, ok := c.cursors[response.Token]
	if !ok {
		// Create a new cursor if needed
		cursor = newCursor(c, response.Token, q.Term, q.Opts)
		cursor.profile = response.Profile

		c.cursors[response.Token] = cursor
	}

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processSequenceResponse(q Query, response *Response) (*Response, *Cursor, error) {
	cursor, ok := c.cursors[response.Token]
	if !ok {
		// Create a new cursor if needed
		cursor = newCursor(c, response.Token, q.Term, q.Opts)
		cursor.profile = response.Profile
	}

	delete(c.cursors, response.Token)

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processWaitResponse(q Query, response *Response) (*Response, *Cursor, error) {
	delete(c.cursors, response.Token)

	return response, nil, nil
}
