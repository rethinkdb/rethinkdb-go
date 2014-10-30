package gorethink

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/fatih/pool.v2"

	p "github.com/dancannon/gorethink/ql2"
)

type responseFunc func(error, *Response, *Cursor)

type Response struct {
	Token     int64
	Type      p.Response_ResponseType `json:"t"`
	Responses []interface{}           `json:"r"`
	Backtrace []interface{}           `json:"b"`
	Profile   interface{}             `json:"p"`
}

type Conn interface {
	SendQuery(s *Session, q *p.Query, t Term, opts map[string]interface{}) (*Cursor, error)
	ReadResponse(s *Session, token int64) (*Response, error)
	Close() error
}

// connection is a connection to a rethinkdb database
type Connection struct {
	sync.Mutex
	conn    net.Conn
	session *Session
	token   int64
	closed  bool

	responseFuncs map[int64]responseFunc
	termCache     map[int64]*Term
	optionCache   map[int64]map[string]interface{}
	cursorCache   map[int64]*Cursor
}

// Dial closes the previous connection and attempts to connect again.
func Dial(s *Session) pool.Factory {
	return func() (net.Conn, error) {
		conn, err := net.Dial("tcp", s.address)
		if err != nil {
			return nil, RqlConnectionError{err.Error()}
		}

		// Send the protocol version to the server as a 4-byte little-endian-encoded integer
		if err := binary.Write(conn, binary.LittleEndian, p.VersionDummy_V0_3); err != nil {
			return nil, RqlConnectionError{err.Error()}
		}

		// Send the length of the auth key to the server as a 4-byte little-endian-encoded integer
		if err := binary.Write(conn, binary.LittleEndian, uint32(len(s.authkey))); err != nil {
			return nil, RqlConnectionError{err.Error()}
		}

		// Send the auth key as an ASCII string
		// If there is no auth key, skip this step
		if s.authkey != "" {
			if _, err := io.WriteString(conn, s.authkey); err != nil {
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
			return nil, RqlDriverError{err.Error()}
		}
		// convert to string and remove trailing NUL byte
		response := string(line[:len(line)-1])
		if response != "SUCCESS" {
			// we failed authorization or something else terrible happened
			return nil, RqlDriverError{fmt.Sprintf("Server dropped connection with message: \"%s\"", response)}
		}

		return conn, nil
	}
}

func newConnection(s *Session, c net.Conn) *Connection {
	conn := &Connection{
		conn:    c,
		session: s,

		responseFuncs: make(map[int64]responseFunc),
		termCache:     make(map[int64]*Term),
		optionCache:   make(map[int64]map[string]interface{}),
		cursorCache:   make(map[int64]*Cursor),
	}

	go conn.readLoop()

	return conn
}

func (c *Connection) StartQuery(t Term, opts map[string]interface{}) (*Cursor, error) {
	token := c.nextToken()

	// Build global options
	globalOpts := map[string]interface{}{}
	for k, v := range opts {
		globalOpts[k] = Expr(v).build()
	}

	// If no DB option was set default to the value set in the connection
	if _, ok := opts["db"]; !ok {
		globalOpts["db"] = Db(c.session.database).build()
	}

	// Construct query
	q := Query{
		Type:       p.Query_START,
		Token:      token,
		Term:       &t,
		GlobalOpts: globalOpts,
	}

	_, cursor, err := c.SendQuery(q, map[string]interface{}{})
	return cursor, err
}

func (c *Connection) ContinueQuery(token int64) error {
	q := Query{
		Type:  p.Query_CONTINUE,
		Token: token,
	}

	_, _, err := c.SendQuery(q, map[string]interface{}{})
	return err
}

func (c *Connection) AsyncContinueQuery(token int64) error {
	q := Query{
		Type:  p.Query_CONTINUE,
		Token: token,
	}

	// Send query and wait for response
	return c.sendQuery(q, map[string]interface{}{}, func(err error, _ *Response, cursor *Cursor) {
		if cursor != nil {
			cursor.mu.Lock()
			if err != nil {
				cursor.err = err
			}
			cursor.mu.Unlock()
		}
	})
}

func (c *Connection) StopQuery(token int64) error {
	q := Query{
		Type:  p.Query_STOP,
		Token: token,
	}

	_, _, err := c.SendQuery(q, map[string]interface{}{})
	return err
}

func (c *Connection) NoReplyWait() error {
	q := Query{
		Type:  p.Query_NOREPLY_WAIT,
		Token: c.nextToken(),
	}

	_, _, err := c.SendQuery(q, map[string]interface{}{})
	return err
}

func (c *Connection) SendQuery(q Query, opts map[string]interface{}) (response *Response, cursor *Cursor, err error) {
	var wait, change sync.Mutex
	var done bool

	var rErr error
	var rResponse *Response
	var rCursor *Cursor

	wait.Lock()
	sendErr := c.sendQuery(q, map[string]interface{}{}, func(err error, response *Response, cursor *Cursor) {
		change.Lock()
		if !done {
			done = true
			rErr = err
			rResponse = response
			rCursor = cursor

			if cursor != nil {
				cursor.mu.Lock()
				if err != nil {
					cursor.err = err
				}
				cursor.mu.Unlock()
			}
		}
		change.Unlock()
		wait.Unlock()
	})
	if sendErr != nil {
		return nil, nil, sendErr
	}
	wait.Lock()
	change.Lock()
	response = rResponse
	cursor = rCursor
	err = rErr
	change.Unlock()

	return response, cursor, err
}

func (c *Connection) sendQuery(q Query, opts map[string]interface{}, cb responseFunc) error {
	//c.Lock()
	closed := c.closed
	//c.Unlock()

	if closed {
		err := errors.New("connection closed")
		cb(err, nil, nil)
		return err
	}
	// Build query
	b, err := json.Marshal(q.build())
	if err != nil {
		err := RqlDriverError{"Error building query"}
		cb(err, nil, nil)
		return err
	}

	//c.Lock()

	// Set timeout
	if c.session.timeout == 0 {
		c.conn.SetDeadline(time.Time{})
	} else {
		c.conn.SetDeadline(time.Now().Add(c.session.timeout))
	}

	// Setup response handler/query caches
	fmt.Printf("send: %p, %d\n", c, q.Token)
	c.termCache[q.Token] = q.Term
	c.optionCache[q.Token] = opts
	c.responseFuncs[q.Token] = cb

	// Send a unique 8-byte token
	if err = binary.Write(c.conn, binary.LittleEndian, q.Token); err != nil {
		//c.Unlock()
		err := RqlConnectionError{err.Error()}
		cb(err, nil, nil)
		return err
	}

	// Send the length of the JSON-encoded query as a 4-byte
	// little-endian-encoded integer.
	if err = binary.Write(c.conn, binary.LittleEndian, uint32(len(b))); err != nil {
		//c.Unlock()
		err := RqlConnectionError{err.Error()}
		cb(err, nil, nil)
		return err
	}

	// Send the JSON encoding of the query itself.
	if err = binary.Write(c.conn, binary.BigEndian, b); err != nil {
		//c.Unlock()
		err := RqlConnectionError{err.Error()}
		cb(err, nil, nil)
		return err
	}

	//c.Unlock()

	// Return immediately if the noreply option was set
	if noreply, ok := opts["noreply"]; ok && noreply.(bool) {
		c.Close()

		return nil
	}

	return nil
}

func (c *Connection) kill(err error) error {
	fmt.Println(err)
	if !c.closed {
		if err := c.Close(); err != nil {
			return err
		}
	}

	return err
}

func (c *Connection) Close() error {
	if !c.closed {
		fmt.Println("closing")
		err := c.conn.Close()
		fmt.Println("closed")

		return err
	}

	return nil
}

// getToken generates the next query token, used to number requests and match
// responses with requests.
func (c *Connection) nextToken() int64 {
	return atomic.AddInt64(&c.session.token, 1)
}

func (c *Connection) processResponse(response *Response) {
	//c.Lock()
	t := c.termCache[response.Token]
	fmt.Printf("recv: %p, %d, %v\n", c, response.Token, c.responseFuncs)
	//c.Unlock()

	switch response.Type {
	case p.Response_CLIENT_ERROR:
		c.processErrorResponse(response, RqlClientError{rqlResponseError{response, t}})
	case p.Response_COMPILE_ERROR:
		c.processErrorResponse(response, RqlCompileError{rqlResponseError{response, t}})
	case p.Response_RUNTIME_ERROR:
		c.processErrorResponse(response, RqlRuntimeError{rqlResponseError{response, t}})
	case p.Response_SUCCESS_ATOM:
		c.processAtomResponse(response)
	case p.Response_SUCCESS_FEED:
		c.processFeedResponse(response)
	case p.Response_SUCCESS_PARTIAL:
		c.processPartialResponse(response)
	case p.Response_SUCCESS_SEQUENCE:
		c.processSequenceResponse(response)
	case p.Response_WAIT_COMPLETE:
		c.processWaitResponse(response)
	default:
		panic(RqlDriverError{"Unexpected response type"})
	}
}

func (c *Connection) processErrorResponse(response *Response, err error) {
	c.Close()

	//c.Lock()
	cb, ok := c.responseFuncs[response.Token]
	cursor := c.cursorCache[response.Token]
	//c.Unlock()
	if ok {
		cb(err, response, cursor)
	}

	//c.Lock()
	delete(c.responseFuncs, response.Token)
	delete(c.termCache, response.Token)
	delete(c.optionCache, response.Token)
	delete(c.cursorCache, response.Token)
	//c.Unlock()
}

func (c *Connection) processAtomResponse(response *Response) {
	c.Close()

	// Create cursor
	var value []interface{}
	if len(response.Responses) < 1 {
		value = []interface{}{}
	} else {
		var v = response.Responses[0]
		if sv, ok := v.([]interface{}); ok {
			value = sv
		} else if v == nil {
			value = []interface{}{nil}
		} else {
			value = []interface{}{v}
		}
	}

	//c.Lock()
	t := c.termCache[response.Token]
	opts := c.optionCache[response.Token]
	//c.Unlock()

	cursor := newCursor(c.session, c, response.Token, t, opts)
	cursor.profile = response.Profile
	cursor.buffer = value
	cursor.finished = true

	// Return response
	//c.Lock()
	cb, ok := c.responseFuncs[response.Token]
	//c.Unlock()
	if ok {
		go cb(nil, response, cursor)
	}

	//c.Lock()
	delete(c.responseFuncs, response.Token)
	delete(c.termCache, response.Token)
	delete(c.optionCache, response.Token)
	//c.Unlock()
}

func (c *Connection) processFeedResponse(response *Response) {
	//c.Lock()
	cb, ok := c.responseFuncs[response.Token]
	if ok {
		delete(c.responseFuncs, response.Token)

		var cursor *Cursor
		if _, ok := c.cursorCache[response.Token]; !ok {
			// Create a new cursor if needed
			cursor = newCursor(c.session, c, response.Token, c.termCache[response.Token], c.optionCache[response.Token])
			cursor.profile = response.Profile
			c.cursorCache[response.Token] = cursor
		} else {
			cursor = c.cursorCache[response.Token]
		}
		//c.Unlock()

		cursor.extend(response)
		cb(nil, response, cursor)
	}
}

func (c *Connection) processPartialResponse(response *Response) {
	//c.Lock()
	cb, ok := c.responseFuncs[response.Token]
	if ok {
		delete(c.responseFuncs, response.Token)

		var cursor *Cursor
		if _, ok := c.cursorCache[response.Token]; !ok {
			// Create a new cursor if needed
			cursor = newCursor(c.session, c, response.Token, c.termCache[response.Token], c.optionCache[response.Token])
			cursor.profile = response.Profile
			c.cursorCache[response.Token] = cursor
		} else {
			cursor = c.cursorCache[response.Token]
		}
		//c.Unlock()

		cursor.extend(response)
		cb(nil, response, cursor)
	}
}

func (c *Connection) processSequenceResponse(response *Response) {
	c.Close()

	//c.Lock()
	cb, ok := c.responseFuncs[response.Token]
	if ok {
		delete(c.responseFuncs, response.Token)

		var cursor *Cursor
		if _, ok := c.cursorCache[response.Token]; !ok {
			// Create a new cursor if needed
			cursor = newCursor(c.session, c, response.Token, c.termCache[response.Token], c.optionCache[response.Token])
			cursor.profile = response.Profile
			c.cursorCache[response.Token] = cursor
		} else {
			cursor = c.cursorCache[response.Token]
		}
		//c.Unlock()

		cursor.extend(response)
		cb(nil, response, cursor)

		//c.Lock()
	}

	delete(c.responseFuncs, response.Token)
	delete(c.termCache, response.Token)
	delete(c.optionCache, response.Token)
	delete(c.cursorCache, response.Token)
	//c.Unlock()
}

func (c *Connection) processWaitResponse(response *Response) {
	c.Close()

	//c.Lock()
	cb, ok := c.responseFuncs[response.Token]
	//c.Unlock()
	if ok {
		cb(nil, response, nil)
	}

	//c.Lock()
	delete(c.responseFuncs, response.Token)
	delete(c.termCache, response.Token)
	delete(c.optionCache, response.Token)
	delete(c.cursorCache, response.Token)
	//c.Unlock()
}

func (c *Connection) readLoop() {
	for {
		// Read the 8-byte token of the query the response corresponds to.
		var responseToken int64
		if err := binary.Read(c.conn, binary.LittleEndian, &responseToken); err != nil {
			c.kill(RqlConnectionError{err.Error()})
			return
		}

		// Read the length of the JSON-encoded response as a 4-byte
		// little-endian-encoded integer.
		var messageLength uint32
		if err := binary.Read(c.conn, binary.LittleEndian, &messageLength); err != nil {
			c.kill(RqlConnectionError{err.Error()})
			return
		}

		// Read the JSON encoding of the Response itself.
		b := make([]byte, messageLength)
		if _, err := io.ReadFull(c.conn, b); err != nil {
			c.kill(RqlConnectionError{err.Error()})
			return
		}

		// Decode the response
		var response = new(Response)
		response.Token = responseToken
		err := json.Unmarshal(b, response)
		if err != nil {
			//c.Lock()
			cb, ok := c.responseFuncs[responseToken]
			//c.Unlock()
			if ok {
				cb(err, nil, nil)
			}
			continue
		}

		c.processResponse(response)
	}
}
