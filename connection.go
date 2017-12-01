package gorethink

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	p "gopkg.in/gorethink/gorethink.v3/ql2"
)

const (
	respHeaderLen          = 12
	defaultKeepAlivePeriod = time.Second * 30

	notBad = 0
	bad    = 1
)

// Response represents the raw response from a query, most of the time you
// should instead use a Cursor when reading from the database.
type Response struct {
	Token     int64
	Type      p.Response_ResponseType   `json:"t"`
	ErrorType p.Response_ErrorType      `json:"e"`
	Notes     []p.Response_ResponseNote `json:"n"`
	Responses []json.RawMessage         `json:"r"`
	Backtrace []interface{}             `json:"b"`
	Profile   interface{}               `json:"p"`
}

// Connection is a connection to a rethinkdb database. Connection is not thread
// safe and should only be accessed be a single goroutine
type Connection struct {
	net.Conn

	address string
	opts    *ConnectOpts

	_                [4]byte
	token            int64
	cursors          map[int64]*Cursor
	bad              int32 // 0 - not bad, 1 - bad
	closed           bool
	stopReadChan     chan bool
	readRequestsChan chan tokenAndPromise
}

type responseAndCursor struct {
	response *Response
	cursor   *Cursor
	err error
}

type tokenAndPromise struct {
	ctx context.Context
	token   int64
	query *Query
	promise chan responseAndCursor
}

// NewConnection creates a new connection to the database server
func NewConnection(address string, opts *ConnectOpts) (*Connection, error) {
	var err error
	c := &Connection{
		address:          address,
		opts:             opts,
		cursors:          make(map[int64]*Cursor),
		stopReadChan:     make(chan bool, 1),
		bad:              notBad,
		readRequestsChan: make(chan tokenAndPromise, 16),
	}

	keepAlivePeriod := defaultKeepAlivePeriod
	if opts.KeepAlivePeriod > 0 {
		keepAlivePeriod = opts.KeepAlivePeriod
	}

	// Connect to Server
	nd := net.Dialer{Timeout: c.opts.Timeout, KeepAlive: keepAlivePeriod}
	if c.opts.TLSConfig == nil {
		c.Conn, err = nd.Dial("tcp", address)
	} else {
		c.Conn, err = tls.DialWithDialer(&nd, "tcp", address, c.opts.TLSConfig)
	}
	if err != nil {
		return nil, RQLConnectionError{rqlError(err.Error())}
	}

	// Send handshake
	handshake, err := c.handshake(opts.HandshakeVersion)
	if err != nil {
		return nil, err
	}

	if err = handshake.Send(); err != nil {
		return nil, err
	}

	go c.processResponses()

	return c, nil
}

// Close closes the underlying net.Conn
func (c *Connection) Close() error {
	var err error

	if !c.closed {
		c.stopReadChan <- true
		c.closed = true
		err = c.Conn.Close()
		c.cursors = nil
	}

	return err
}

// Query sends a Query to the database, returning both the raw Response and a
// Cursor which should be used to view the query's response.
//
// This function is used internally by Run which should be used for most queries.
func (c *Connection) Query(ctx context.Context, q Query) (*Response, *Cursor, error) {
	if ctx == nil {
		ctx = c.contextFromConnectionOpts()
	}

	if c == nil {
		return nil, nil, ErrConnectionClosed
	}
	if c.Conn == nil {
		c.setBad()
		return nil, nil, ErrConnectionClosed
	}

	// Add token if query is a START/NOREPLY_WAIT
	if q.Type == p.Query_START || q.Type == p.Query_NOREPLY_WAIT || q.Type == p.Query_SERVER_INFO {
		q.Token = c.nextToken()
	}
	if q.Type == p.Query_START || q.Type == p.Query_NOREPLY_WAIT {
		if c.opts.Database != "" {
			var err error
			q.Opts["db"], err = DB(c.opts.Database).Build()
			if err != nil {
				return nil, nil, RQLDriverError{rqlError(err.Error())}
			}
		}
	}

	err := c.sendQuery(q)
	if err != nil {
		return nil, nil, err
	}

	if noreply, ok := q.Opts["noreply"]; ok && noreply.(bool) {
		return nil, nil, nil
	}

	promise := make(chan responseAndCursor, 1)

	c.readRequestsChan <- tokenAndPromise{
		ctx: ctx,
		token: q.Token,
		query: &q,
		promise: promise,
	}

	select {
	case future := <-promise:
		return future.response, future.cursor, future.err
	case <-ctx.Done():
		if q.Type != p.Query_STOP {
			stopQuery := newStopQuery(q.Token)
			c.Query(c.contextFromConnectionOpts(), stopQuery)
		}
		return nil, nil, ErrQueryTimeout
	}
}

func (c *Connection) processResponses() {
	readRequests := make(map[int64]tokenAndPromise, 16)
	for {
		response, err := c.readResponse()
		if err != nil {
			if len(readRequests) > 0 {
				// return error to all queries
				// because it's socket or protocol error, no more queries can be processed
				for _, rr := range readRequests {
					rr.promise <- responseAndCursor{err: err}
					delete(readRequests, rr.token)
				}
			}
			if c.closed {
				return
			}
			continue
		}

		stop := false
		for !stop {
			select {
			case readRequest := <-c.readRequestsChan:
				readRequests[readRequest.token] = readRequest
			case <-c.stopReadChan:
				return
			default:
				stop = true // stop when both chans are empty
			}
		}

		rr, ok := readRequests[response.Token]
		if ok {
			response, cursor, err := c.processInitialResponse(rr.ctx, *rr.query, response)
			rr.promise <- responseAndCursor{response: response, cursor: cursor, err: err}
			delete(readRequests, rr.token)
		} else {
			err := c.processSubsequentResponse(response)
			if err != nil {
				Log.Errorf("Failed to fill cached cursor: %v", err)
			}
		}
	}
}

type ServerResponse struct {
	ID   string `gorethink:"id"`
	Name string `gorethink:"name"`
}

// Server returns the server name and server UUID being used by a connection.
func (c *Connection) Server() (ServerResponse, error) {
	var response ServerResponse

	_, cur, err := c.Query(c.contextFromConnectionOpts(), Query{
		Type: p.Query_SERVER_INFO,
	})
	if err != nil {
		return response, err
	}

	if err = cur.One(&response); err != nil {
		return response, err
	}

	if err = cur.Close(); err != nil {
		return response, err
	}

	return response, nil
}

// sendQuery marshals the Query and sends the JSON to the server.
func (c *Connection) sendQuery(q Query) error {
	// Build query
	b, err := json.Marshal(q.Build())
	if err != nil {
		return RQLDriverError{rqlError(fmt.Sprintf("Error building query: %s", err.Error()))}
	}

	// Set timeout
	if c.opts.WriteTimeout == 0 {
		c.Conn.SetWriteDeadline(time.Time{})
	} else {
		c.Conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	}

	// Send the JSON encoding of the query itself.
	if err = c.writeQuery(q.Token, b); err != nil {
		c.setBad()
		return RQLConnectionError{rqlError(err.Error())}
	}

	return nil
}

// getToken generates the next query token, used to number requests and match
// responses with requests.
func (c *Connection) nextToken() int64 {
	// requires c.token to be 64-bit aligned on ARM
	return atomic.AddInt64(&c.token, 1)
}

// readResponse attempts to read a Response from the server, if no response
// could be read then an error is returned.
func (c *Connection) readResponse() (*Response, error) {
	// Set timeout
	if c.opts.ReadTimeout == 0 {
		c.Conn.SetReadDeadline(time.Time{})
	} else {
		c.Conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
	}

	// Read response header (token+length)
	headerBuf := [respHeaderLen]byte{}
	if _, err := c.read(headerBuf[:]); err != nil {
		c.setBad()
		return nil, RQLConnectionError{rqlError(err.Error())}
	}

	responseToken := int64(binary.LittleEndian.Uint64(headerBuf[:8]))
	messageLength := binary.LittleEndian.Uint32(headerBuf[8:])

	// Read the JSON encoding of the Response itself.
	b := make([]byte, int(messageLength))

	if _, err := c.read(b); err != nil {
		c.setBad()
		return nil, RQLConnectionError{rqlError(err.Error())}
	}

	// Decode the response
	var response = new(Response)
	if err := json.Unmarshal(b, response); err != nil {
		c.setBad()
		return nil, RQLDriverError{rqlError(err.Error())}
	}
	response.Token = responseToken

	return response, nil
}

// Called to fill response for the query
func (c *Connection) processInitialResponse(ctx context.Context, q Query, response *Response) (*Response, *Cursor, error) {
	switch response.Type {
	case p.Response_CLIENT_ERROR:
		c.processErrorResponse(response)
		return response, nil, createClientError(response, q.Term)
	case p.Response_COMPILE_ERROR:
		c.processErrorResponse(response)
		return response, nil, createCompileError(response, q.Term)
	case p.Response_RUNTIME_ERROR:
		c.processErrorResponse(response)
		return response, nil, createRuntimeError(response.ErrorType, response, q.Term)
	case p.Response_SUCCESS_ATOM, p.Response_SERVER_INFO:
		return c.processAtomResponse(ctx, q, response)
	case p.Response_SUCCESS_PARTIAL:
		return c.processInitialPartialResponse(ctx, q, response)
	case p.Response_SUCCESS_SEQUENCE:
		return c.processInitialSequenceResponse(ctx, q, response)
	case p.Response_WAIT_COMPLETE:
		return c.processWaitResponse(response), nil, nil
	default:
		return nil, nil, RQLDriverError{rqlError("Unexpected response type")}
	}
}

// Called to fill cursor in background
func (c *Connection) processSubsequentResponse(response *Response) error {
	switch response.Type {
	case p.Response_CLIENT_ERROR, p.Response_COMPILE_ERROR, p.Response_RUNTIME_ERROR:
		cursor := c.processErrorResponse(response)
		if cursor == nil {
			return RQLDriverError{rqlError("Internal error: no cached cursor in a streaming response")}
		}
		switch response.Type {
		case p.Response_CLIENT_ERROR:
			return createClientError(response, cursor.term)
		case p.Response_COMPILE_ERROR:
			return createCompileError(response, cursor.term)
		default: // case p.Response_RUNTIME_ERROR:
			return createRuntimeError(response.ErrorType, response, cursor.term)
		}
	case p.Response_SUCCESS_PARTIAL:
		return c.processSubsequentPartialResponse(response)
	case p.Response_SUCCESS_SEQUENCE:
		return c.processSubsequentSequenceResponse(response)
	case p.Response_WAIT_COMPLETE:
		c.processWaitResponse(response)
		return nil
	default:
		return RQLDriverError{rqlError("Unexpected response type")}
	}
}

func (c *Connection) processErrorResponse(response *Response) *Cursor {
	cursor := c.cursors[response.Token]
	delete(c.cursors, response.Token)
	return cursor
}

func (c *Connection) processAtomResponse(ctx context.Context, q Query, response *Response) (*Response, *Cursor, error) {
	cursor := newCursor(ctx, c, "Cursor", response.Token, q.Term, q.Opts)
	cursor.profile = response.Profile
	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processInitialPartialResponse(ctx context.Context, q Query, response *Response) (*Response, *Cursor, error) {
	cursorType := "Cursor"
	if len(response.Notes) > 0 {
		switch response.Notes[0] {
		case p.Response_SEQUENCE_FEED:
			cursorType = "Feed"
		case p.Response_ATOM_FEED:
			cursorType = "AtomFeed"
		case p.Response_ORDER_BY_LIMIT_FEED:
			cursorType = "OrderByLimitFeed"
		case p.Response_UNIONED_FEED:
			cursorType = "UnionedFeed"
		case p.Response_INCLUDES_STATES:
			cursorType = "IncludesFeed"
		}
	}

	cursor, ok := c.cursors[response.Token]
	if !ok {
		// Create a new cursor if needed
		cursor = newCursor(ctx, c, cursorType, response.Token, q.Term, q.Opts)
		cursor.profile = response.Profile

		c.cursors[response.Token] = cursor
	}

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processSubsequentPartialResponse(response *Response) error {
	cursor, ok := c.cursors[response.Token]
	if !ok {
		return RQLDriverError{rqlError("Internal error: no cached cursor in a streaming response")}
	}

	cursor.extend(response)
	return nil
}

func (c *Connection) processInitialSequenceResponse(ctx context.Context, q Query, response *Response) (*Response, *Cursor, error) {
	cursor, ok := c.cursors[response.Token]
	if !ok {
		// Create a new cursor if needed
		cursor = newCursor(ctx, c, "Cursor", response.Token, q.Term, q.Opts)
		cursor.profile = response.Profile
	}
	delete(c.cursors, response.Token)

	cursor.extend(response)

	return response, cursor, nil
}

func (c *Connection) processSubsequentSequenceResponse(response *Response) error {
	cursor, ok := c.cursors[response.Token]
	delete(c.cursors, response.Token)
	if !ok {
		return RQLDriverError{rqlError("Internal error: no cached cursor in a streaming response")}
	}

	cursor.extend(response)
	return nil
}

func (c *Connection) processWaitResponse(response *Response) *Response {
	delete(c.cursors, response.Token)
	return response
}

func (c *Connection) setBad() {
	atomic.StoreInt32(&c.bad, bad)
}

func (c *Connection) isBad() bool {
	return atomic.LoadInt32(&c.bad) == bad
}
