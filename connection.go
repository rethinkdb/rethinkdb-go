package gorethink

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	p "github.com/dancannon/gorethink/ql2"
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
}

// Dial closes the previous connection and attempts to connect again.
func NewConnection(opts *ConnectOpts) (*Connection, error) {
	conn, err := net.Dial("tcp", opts.Address)
	if err != nil {
		return nil, RqlConnectionError{err.Error()}
	}

	// Send the protocol version to the server as a 4-byte little-endian-encoded integer
	if err := binary.Write(conn, binary.LittleEndian, p.VersionDummy_V0_3); err != nil {
		return nil, RqlConnectionError{err.Error()}
	}

	// Send the length of the auth key to the server as a 4-byte little-endian-encoded integer
	if err := binary.Write(conn, binary.LittleEndian, uint32(len(opts.AuthKey))); err != nil {
		return nil, RqlConnectionError{err.Error()}
	}

	// Send the auth key as an ASCII string
	// If there is no auth key, skip this step
	if opts.AuthKey != "" {
		if _, err := io.WriteString(conn, opts.AuthKey); err != nil {
			return nil, RqlConnectionError{err.Error()}
		}
	}

	// Send the protocol type as a 4-byte little-endian-encoded integer
	if err := binary.Write(conn, binary.LittleEndian, p.VersionDummy_JSON); err != nil {
		return nil, RqlConnectionError{err.Error()}
	}

	// read server response to authorization key (terminated by NUL)
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\x00')
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("Unexpected EOF: %s", string(line))
		}
		return nil, RqlConnectionError{err.Error()}
	}
	// convert to string and remove trailing NUL byte
	response := string(line[:len(line)-1])
	if response != "SUCCESS" {
		// we failed authorization or something else terrible happened
		return nil, RqlDriverError{fmt.Sprintf("Server dropped connection with message: \"%s\"", response)}
	}

	c := &Connection{
		opts:    opts,
		conn:    conn,
		cursors: make(map[int64]*Cursor),
	}

	c.conn.SetDeadline(time.Time{})

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

	var response *Response
	for {
		response, err = c.readResponse()
		if err != nil {
			return nil, nil, err
		}

		if response.Token == q.Token {
			// If this was the requested response process and return
			return c.processResponse(q, response)
		} else if _, ok := c.cursors[response.Token]; ok {
			// If the token is in the cursor cache then process the response
			c.processResponse(q, response)
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

	// Send a unique 8-byte token
	if err = binary.Write(c.conn, binary.LittleEndian, q.Token); err != nil {
		c.bad = true
		return RqlConnectionError{err.Error()}
	}

	// Send the length of the JSON-encoded query as a 4-byte
	// little-endian-encoded integer.
	if err = binary.Write(c.conn, binary.LittleEndian, uint32(len(b))); err != nil {
		c.bad = true
		return RqlConnectionError{err.Error()}
	}

	// Send the JSON encoding of the query itself.
	if err = binary.Write(c.conn, binary.BigEndian, b); err != nil {
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
	// Read the 8-byte token of the query the response corresponds to.
	var responseToken int64
	if err := binary.Read(c.conn, binary.LittleEndian, &responseToken); err != nil {
		c.bad = true
		return nil, RqlConnectionError{err.Error()}
	}

	// Read the length of the JSON-encoded response as a 4-byte
	// little-endian-encoded integer.
	var messageLength uint32
	if err := binary.Read(c.conn, binary.LittleEndian, &messageLength); err != nil {
		c.bad = true
		return nil, RqlConnectionError{err.Error()}
	}

	// Read the JSON encoding of the Response itself.
	b := make([]byte, messageLength)
	if _, err := io.ReadFull(c.conn, b); err != nil {
		c.bad = true
		return nil, RqlConnectionError{err.Error()}
	}

	// Decode the response
	var response = new(Response)
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
