package gorethink

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"sync"

	"gopkg.in/dancannon/gorethink.v1/encoding"
	p "gopkg.in/dancannon/gorethink.v1/ql2"
)

var (
	errCursorClosed = errors.New("connection closed, cannot read cursor")
)

func newCursor(conn *Connection, cursorType string, token int64, term *Term, opts map[string]interface{}) *Cursor {
	if cursorType == "" {
		cursorType = "Cursor"
	}

	cursor := &Cursor{
		conn:       conn,
		token:      token,
		cursorType: cursorType,
		term:       term,
		opts:       opts,
		buffer:     make([]interface{}, 0),
		responses:  make([]json.RawMessage, 0),
	}

	return cursor
}

// Cursor is the result of a query. Its cursor starts before the first row
// of the result set. A Cursor is not thread safe and should only be accessed
// by a single goroutine at any given time. Use Next to advance through the
// rows:
//
//     cursor, err := query.Run(session)
//     ...
//     defer cursor.Close()
//
//     var response interface{}
//     for cursor.Next(&response) {
//         ...
//     }
//     err = cursor.Err() // get any error encountered during iteration
//     ...
type Cursor struct {
	releaseConn func() error

	conn       *Connection
	token      int64
	cursorType string
	term       *Term
	opts       map[string]interface{}

	mu           sync.RWMutex
	lastErr      error
	fetching     bool
	closed       bool
	finished     bool
	isAtom       bool
	pendingSkips int
	buffer       []interface{}
	responses    []json.RawMessage
	profile      interface{}
}

// Profile returns the information returned from the query profiler.
func (c *Cursor) Profile() interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.profile
}

// Type returns the cursor type (by default "Cursor")
func (c *Cursor) Type() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cursorType
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (c *Cursor) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lastErr
}

// Close closes the cursor, preventing further enumeration. If the end is
// encountered, the cursor is closed automatically. Close is idempotent.
func (c *Cursor) Close() error {
	var err error

	c.mu.Lock()
	defer c.mu.Unlock()

	// If cursor is already closed return immediately
	closed := c.closed
	if closed {
		return nil
	}

	// Get connection and check its valid, don't need to lock as this is only
	// set when the cursor is created
	conn := c.conn
	if conn == nil {
		return nil
	}
	if conn.Conn == nil {
		return nil
	}

	// Stop any unfinished queries
	if !c.finished {
		q := Query{
			Type:  p.Query_STOP,
			Token: c.token,
			Opts: map[string]interface{}{
				"noreply": true,
			},
		}

		_, _, err = conn.Query(q)
	}

	if c.releaseConn != nil {
		if err := c.releaseConn(); err != nil {
			return err
		}
	}

	c.closed = true
	c.conn = nil
	c.buffer = nil
	c.responses = nil

	return err
}

// Next retrieves the next document from the result set, blocking if necessary.
// This method will also automatically retrieve another batch of documents from
// the server when the current one is exhausted, or before that in background
// if possible.
//
// Next returns true if a document was successfully unmarshalled onto result,
// and false at the end of the result set or if an error happened.
// When Next returns false, the Err method should be called to verify if
// there was an error during iteration.
//
// Also note that you are able to reuse the same variable multiple times as
// `Next` zeroes the value before scanning in the result.
func (c *Cursor) Next(dest interface{}) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}

	hasMore, err := c.nextLocked(dest, true)
	if c.handleErrorLocked(err) != nil {
		c.mu.Unlock()
		c.Close()
		return false
	}
	c.mu.Unlock()

	if !hasMore {
		c.Close()
	}

	return hasMore
}

func (c *Cursor) nextLocked(dest interface{}, progressCursor bool) (bool, error) {
	for {
		if err := c.seekCursor(); err != nil {
			return false, err
		}

		if len(c.buffer) == 0 && len(c.responses) == 0 && c.finished {
			return false, nil
		}

		if len(c.buffer) == 0 && len(c.responses) > 0 {
			response := c.responses[0]
			c.responses = c.responses[1:]

			var value interface{}
			decoder := json.NewDecoder(bytes.NewBuffer(response))
			if c.conn.opts.UseJSONNumber {
				decoder.UseNumber()
			}
			err := decoder.Decode(&value)
			if err != nil {
				return false, err
			}

			value, err = recursivelyConvertPseudotype(value, c.opts)
			if err != nil {
				return false, err
			}

			// If response is an ATOM then try and convert to an array
			if data, ok := value.([]interface{}); ok && c.isAtom {
				for _, v := range data {
					c.buffer = append(c.buffer, v)
				}
			} else if value == nil {
				c.buffer = append(c.buffer, nil)
			} else {
				c.buffer = append(c.buffer, value)
			}
			c.applyPendingSkips(true)
		}

		if len(c.buffer) > 0 {
			var data interface{} = c.buffer[0]
			if progressCursor {
				c.buffer = c.buffer[1:]
			}

			err := encoding.Decode(dest, data)
			if err != nil {
				return false, err
			}

			return true, nil
		}
	}
}

// Peek behaves similarly to Next, retreiving the next document from the result set
// and blocking if necessary. Peek, however, does not progress the position of the cursor.
// This can be useful for expressions which can return different types to attempt to
// decode them into different interfaces.
//
// Like Next, it will also automatically retrieve another batch of documents from
// the server when the current one is exhausted, or before that in background
// if possible.
//
// Unlike Next, Peek does not progress the position of the cursor. Peek
// will return errors from decoding, but they will not be persisted in the cursor
// and therefore will not be available on cursor.Err(). This can be useful for
// expressions that can return different types to attempt to decode them into
// different interfaces.
//
// Peek returns true if a document was successfully unmarshalled onto result,
// and false at the end of the result set or if an error happened. Peek also
// returns the error (if any) that occured
func (c *Cursor) Peek(dest interface{}) (bool, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false, nil
	}

	hasMore, err := c.nextLocked(dest, false)
	if _, isDecodeErr := err.(*encoding.DecodeTypeError); isDecodeErr {
		c.mu.Unlock()
		return false, err
	}

	if c.handleErrorLocked(err) != nil {
		c.mu.Unlock()
		c.Close()
		return false, err
	}
	c.mu.Unlock()

	return hasMore, nil
}

// Skip progresses the cursor by one record. It is useful after a successful
// Peek to avoid duplicate decoding work.
func (c *Cursor) Skip() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingSkips++
}

// NextResponse retrieves the next raw response from the result set, blocking if necessary.
// Unlike Next the returned response is the raw JSON document returned from the
// database.
//
// NextResponse returns false (and a nil byte slice) at the end of the result
// set or if an error happened.
func (c *Cursor) NextResponse() ([]byte, bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false
	}

	b, hasMore, err := c.nextResponseLocked()
	if c.handleErrorLocked(err) != nil {
		c.mu.Unlock()
		c.Close()
		return nil, false
	}
	c.mu.Unlock()

	if !hasMore {
		c.Close()
	}

	return b, hasMore
}

func (c *Cursor) nextResponseLocked() ([]byte, bool, error) {
	for {
		if err := c.seekCursor(); err != nil {
			return nil, false, err
		}

		if len(c.responses) == 0 && c.finished {
			return nil, false, nil
		}

		if len(c.responses) > 0 {
			var response json.RawMessage
			response, c.responses = c.responses[0], c.responses[1:]

			return []byte(response), true, nil
		}
	}
}

// All retrieves all documents from the result set into the provided slice
// and closes the cursor.
//
// The result argument must necessarily be the address for a slice. The slice
// may be nil or previously allocated.
//
// Also note that you are able to reuse the same variable multiple times as
// `All` zeroes the value before scanning in the result. It also attempts
// to reuse the existing slice without allocating any more space by either
// resizing or returning a selection of the slice if necessary.
func (c *Cursor) All(result interface{}) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	i := 0
	for {
		if slicev.Len() == i {
			elemp := reflect.New(elemt)
			if !c.Next(elemp.Interface()) {
				break
			}
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			if !c.Next(slicev.Index(i).Addr().Interface()) {
				break
			}
		}
		i++
	}
	resultv.Elem().Set(slicev.Slice(0, i))

	if err := c.Err(); err != nil {
		c.Close()
		return err
	}

	if err := c.Close(); err != nil {
		return err
	}

	return nil
}

// One retrieves a single document from the result set into the provided
// slice and closes the cursor.
//
// Also note that you are able to reuse the same variable multiple times as
// `One` zeroes the value before scanning in the result.
func (c *Cursor) One(result interface{}) error {
	if c.IsNil() {
		c.Close()
		return ErrEmptyResult
	}

	hasResult := c.Next(result)

	if err := c.Err(); err != nil {
		c.Close()
		return err
	}

	if err := c.Close(); err != nil {
		return err
	}

	if !hasResult {
		return ErrEmptyResult
	}

	return nil
}

// Listen listens for rows from the database and sends the result onto the given
// channel. The type that the row is scanned into is determined by the element
// type of the channel.
//
// Also note that this function returns immediately.
//
//     cursor, err := r.Expr([]int{1,2,3}).Run(session)
//     if err != nil {
//         panic(err)
//     }
//
//     ch := make(chan int)
//     cursor.Listen(ch)
//     <- ch // 1
//     <- ch // 2
//     <- ch // 3
func (c *Cursor) Listen(channel interface{}) {
	go func() {
		channelv := reflect.ValueOf(channel)
		if channelv.Kind() != reflect.Chan {
			panic("input argument must be a channel")
		}
		elemt := channelv.Type().Elem()
		for {
			elemp := reflect.New(elemt)
			if !c.Next(elemp.Interface()) {
				break
			}

			channelv.Send(elemp.Elem())
		}

		c.Close()
		channelv.Close()
	}()
}

// IsNil tests if the current row is nil.
func (c *Cursor) IsNil() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.buffer) > 0 {
		bufferedItem := c.buffer[0]
		if bufferedItem == nil {
			return true
		}

		return false
	}

	if len(c.responses) > 0 {
		response := c.responses[0]
		if response == nil {
			return true
		}

		if string(response) == "null" {
			return true
		}

		return false
	}

	return true
}

// fetchMore fetches more rows from the database.
//
// If wait is true then it will wait for the database to reply otherwise it
// will return after sending the continue query.
func (c *Cursor) fetchMore() error {
	var err error

	fetching := c.fetching
	closed := c.closed

	if !fetching {
		c.fetching = true

		if closed {
			return errCursorClosed
		}

		q := Query{
			Type:  p.Query_CONTINUE,
			Token: c.token,
		}

		c.mu.Unlock()
		_, _, err = c.conn.Query(q)
		c.mu.Lock()
	}

	return err
}

// handleError sets the value of lastErr to err if lastErr is not yet set.
func (c *Cursor) handleError(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.handleErrorLocked(err)
}

func (c *Cursor) handleErrorLocked(err error) error {
	if c.lastErr == nil {
		c.lastErr = err
	}

	return c.lastErr
}

// extend adds the result of a continue query to the cursor.
func (c *Cursor) extend(response *Response) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.extendLocked(response)
}

func (c *Cursor) extendLocked(response *Response) {
	for _, response := range response.Responses {
		c.responses = append(c.responses, response)
	}

	c.finished = response.Type != p.Response_SUCCESS_PARTIAL
	c.fetching = false
	c.isAtom = response.Type == p.Response_SUCCESS_ATOM

	putResponse(response)
}

// seekCursor takes care of loading more data if needed and applying pending skips
func (c *Cursor) seekCursor() error {
	if c.lastErr != nil {
		return c.lastErr
	}

	if len(c.responses) == 0 && c.closed {
		return errCursorClosed
	}

	c.applyPendingSkips(false)

	// Load more responses if they are available
	if len(c.responses) == 0 && !c.finished {
		for {
			if len(c.responses) == 0 && !c.closed && !c.finished {
				if err := c.fetchMore(); err != nil {
					return err
				}
			}

			// If we have more pending skips or we drained all of our responses, go around again
			if morePending := c.applyPendingSkips(false); morePending || len(c.responses) == 0 {
				if !c.closed && !c.finished {
					continue
				}
			}

			return nil
		}
	}
	return nil
}

// applyPendingSkips applies all pending skips to the buffer and
// returns whether there are more pending skips to be applied
func (c *Cursor) applyPendingSkips(atomConverted bool) (stillPending bool) {
	if c.pendingSkips == 0 {
		return false
	}

	if atomConverted == false && c.isAtom {
		return true
	}

	// Drain from the buffer first
	if len(c.buffer) > c.pendingSkips {
		c.buffer = c.buffer[c.pendingSkips:]
		c.pendingSkips = 0
		return false
	} else if len(c.buffer) > 0 {
		c.pendingSkips -= len(c.buffer)
		c.buffer = c.buffer[:0]
		if c.pendingSkips == 0 {
			return false
		}
	}

	if len(c.responses) > c.pendingSkips {
		c.responses = c.responses[c.pendingSkips:]
		c.pendingSkips = 0
		return false
	}

	c.pendingSkips -= len(c.responses)
	c.responses = c.responses[:0]
	return c.pendingSkips > 0
}
