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

const (
	writerBufferSize = 4096
	readerBufferSize = 4096
)

type Request struct {
	Query   Query
	Options map[string]interface{}
}

type Response struct {
	Token     int64
	Type      p.Response_ResponseType `json:"t"`
	Responses []json.RawMessage       `json:"r"`
	Backtrace []interface{}           `json:"b"`
	Profile   interface{}             `json:"p"`
}

// connection is a connection to a rethinkdb database
type Connection struct {
	conn    net.Conn
	opts    *ConnectOpts
	token   int64
	cursors map[int64]*Cursor

	br *bufio.Reader
	bw *bufio.Writer
}

// Dial closes the previous connection and attempts to connect again.
func NewConnection(opts *ConnectOpts) (*Connection, error) {
	c, err := net.Dial("tcp", opts.Address)
	if err != nil {
		return nil, ErrBadConn
	}

	// Send the protocol version to the server as a 4-byte little-endian-encoded integer
	if err := binary.Write(c, binary.LittleEndian, p.VersionDummy_V0_3); err != nil {
		return nil, ErrBadConn
	}

	// Send the length of the auth key to the server as a 4-byte little-endian-encoded integer
	if err := binary.Write(c, binary.LittleEndian, uint32(len(opts.AuthKey))); err != nil {
		return nil, ErrBadConn
	}

	// Send the auth key as an ASCII string
	// If there is no auth key, skip this step
	if opts.AuthKey != "" {
		if _, err := io.WriteString(c, opts.AuthKey); err != nil {
			return nil, ErrBadConn
		}
	}

	// Send the protocol type as a 4-byte little-endian-encoded integer
	if err := binary.Write(c, binary.LittleEndian, p.VersionDummy_JSON); err != nil {
		return nil, ErrBadConn
	}

	// read server response to authorization key (terminated by NUL)
	reader := bufio.NewReader(c)
	line, err := reader.ReadBytes('\x00')
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("Unexpected EOF: %s", string(line))
		}
		return nil, RqlDriverError{err.Error()}
	}
	// convert to string and remove trailing NUL byte
	response := string(line[:len(line)-1])
	if response != "SUCCESS" {
		// we failed authorization or something else terrible happened
		return nil, RqlDriverError{fmt.Sprintf("Server dropped connection with message: \"%s\"", response)}
	}

	conn := &Connection{
		opts:    opts,
		conn:    c,
		cursors: make(map[int64]*Cursor),
		bw:      bufio.NewWriterSize(c, writerBufferSize),
		br:      bufio.NewReaderSize(c, readerBufferSize),
	}

	return conn, nil
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

func (c *Connection) Query(q Query, opts map[string]interface{}) (*Response, *Cursor, error) {
	if c.conn == nil {
		return nil, nil, ErrBadConn
	}

	// Add token if query is a START/NOREPLY_WAIT
	if q.Type == p.Query_START || q.Type == p.Query_NOREPLY_WAIT {
		q.Token = c.nextToken()
	}

	// If no DB option was set default to the value set in the connection
	if _, ok := opts["db"]; !ok {
		opts["db"] = Db(c.opts.Database).build()
	}

	request := Request{
		Query:   q,
		Options: opts,
	}

	err := c.sendQuery(request)
	if err != nil {
		return nil, nil, err
	}

	if noreply, ok := opts["noreply"]; ok && noreply.(bool) {
		return nil, nil, nil
	}

	var response *Response
	for {
		response, err = c.readResponse()
		if err != nil {
			return nil, nil, err
		}

		if response.Token == request.Query.Token {
			// If this was the requested response process and return
			return c.processResponse(request, response)
		} else if _, ok := c.cursors[response.Token]; ok {
			// If the token is in the cursor cache then process the response
			c.processResponse(request, response)
		}
	}
}

func (c *Connection) sendQuery(request Request) error {
	// Build query
	b, err := json.Marshal(request.Query.build())
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
	if err = binary.Write(c.bw, binary.LittleEndian, request.Query.Token); err != nil {
		return RqlDriverError{err.Error()}
	}

	// Send the length of the JSON-encoded query as a 4-byte
	// little-endian-encoded integer.
	if err = binary.Write(c.bw, binary.LittleEndian, uint32(len(b))); err != nil {
		return RqlDriverError{err.Error()}
	}

	// Send the JSON encoding of the query itself.
	if err = binary.Write(c.bw, binary.BigEndian, b); err != nil {
		return RqlDriverError{err.Error()}
	}

	// Flush buffer
	if err := c.bw.Flush(); err != nil {
		return ErrBadConn
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
	if err := binary.Read(c.br, binary.LittleEndian, &responseToken); err != nil {
		return nil, ErrBadConn
	}

	// Read the length of the JSON-encoded response as a 4-byte
	// little-endian-encoded integer.
	var messageLength uint32
	if err := binary.Read(c.br, binary.LittleEndian, &messageLength); err != nil {
		return nil, ErrBadConn
	}

	// Read the JSON encoding of the Response itself.
	b := make([]byte, messageLength)
	if _, err := io.ReadFull(c.br, b); err != nil {
		return nil, ErrBadConn
	}

	// Decode the response
	var response = new(Response)
	if err := json.Unmarshal(b, response); err != nil {
		return nil, RqlDriverError{err.Error()}
	}
	response.Token = responseToken

	return response, nil
}

func (c *Connection) processResponse(request Request, response *Response) (*Response, *Cursor, error) {
	switch response.Type {
	case p.Response_CLIENT_ERROR:
		return c.processErrorResponse(request, response, RqlClientError{rqlResponseError{response, request.Query.Term}})
	case p.Response_COMPILE_ERROR:
		return c.processErrorResponse(request, response, RqlCompileError{rqlResponseError{response, request.Query.Term}})
	case p.Response_RUNTIME_ERROR:
		return c.processErrorResponse(request, response, RqlRuntimeError{rqlResponseError{response, request.Query.Term}})
	case p.Response_SUCCESS_ATOM:
		return c.processAtomResponse(request, response)
	case p.Response_SUCCESS_FEED:
		return c.processFeedResponse(request, response)
	case p.Response_SUCCESS_PARTIAL:
		return c.processPartialResponse(request, response)
	case p.Response_SUCCESS_SEQUENCE:
		return c.processSequenceResponse(request, response)
	case p.Response_WAIT_COMPLETE:
		return c.processWaitResponse(request, response)
	default:
		return nil, nil, RqlDriverError{"Unexpected response type"}
	}
}

func (c *Connection) processErrorResponse(request Request, response *Response, err error) (*Response, *Cursor, error) {
	cursor := c.cursors[response.Token]

	delete(c.cursors, response.Token)

	return response, cursor, err
}

func (c *Connection) processAtomResponse(request Request, response *Response) (*Response, *Cursor, error) {
	// Create cursor
	cursor := newCursor(c, response.Token, request.Query.Term, request.Options)
	cursor.profile = response.Profile

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processFeedResponse(request Request, response *Response) (*Response, *Cursor, error) {
	var cursor *Cursor
	if _, ok := c.cursors[response.Token]; !ok {
		// Create a new cursor if needed
		cursor = newCursor(c, response.Token, request.Query.Term, request.Options)
		cursor.profile = response.Profile
		c.cursors[response.Token] = cursor
	} else {
		cursor = c.cursors[response.Token]
	}

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processPartialResponse(request Request, response *Response) (*Response, *Cursor, error) {
	cursor, ok := c.cursors[response.Token]
	if !ok {
		// Create a new cursor if needed
		cursor = newCursor(c, response.Token, request.Query.Term, request.Options)
		cursor.profile = response.Profile

		c.cursors[response.Token] = cursor
	}

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processSequenceResponse(request Request, response *Response) (*Response, *Cursor, error) {
	cursor, ok := c.cursors[response.Token]
	if !ok {
		// Create a new cursor if needed
		cursor = newCursor(c, response.Token, request.Query.Term, request.Options)
		cursor.profile = response.Profile
	}

	delete(c.cursors, response.Token)

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processWaitResponse(request Request, response *Response) (*Response, *Cursor, error) {
	delete(c.cursors, response.Token)

	return response, nil, nil
}
