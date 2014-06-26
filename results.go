package gorethink

import (
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/dancannon/gorethink/encoding"
	p "github.com/dancannon/gorethink/ql2"
)

// Cursors are used to represent data returned from the database.
//
// The code for this struct is based off of mgo's Iter and the official
// python driver's cursor.
type Cursor struct {
	mu       sync.Mutex
	gotReply sync.Cond
	session  *Session
	query    *p.Query
	term     Term
	opts     map[string]interface{}

	timeout  time.Duration
	timedout bool

	err                 error
	outstandingRequests int
	closed              bool
	finished            bool
	responses           []*p.Response
	profile             interface{}
	buffer              []interface{}
}

// Profile returns the information returned from the query profiler.
func (c *Cursor) Profile() interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.profile
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (c *Cursor) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.err
}

// Close closes the cursor, preventing further enumeration. If the end is
// encountered, the cursor is closed automatically. Close is idempotent.
func (c *Cursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed && !c.finished {
		err := c.session.stopQuery(c)
		if err != nil && (c.err == nil || c.err == ErrEmptyResult) {
			c.err = err
		}
		c.closed = true
	}

	return c.err
}

// Timeout returns true if Next returned false due to a timeout of
// a tailable cursor. In those cases, Next may be called again to continue
// the iteration at the previous cursor position.
func (c *Cursor) Timeout() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.timedout
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
func (c *Cursor) Next(result interface{}) bool {
	c.mu.Lock()
	c.timedout = false
	timeout := time.Time{}

	if c.closed {
		c.mu.Unlock()

		return false
	}

	// Load more data if needed
	for c.err == nil && len(c.buffer) == 0 && !c.finished {
		if c.timeout >= 0 {
			if timeout.IsZero() {
				timeout = time.Now().Add(c.timeout)
			}
			if time.Now().After(timeout) {
				c.timedout = true
				c.mu.Unlock()
				return false
			}
		}

		c.mu.Unlock()
		c.getMore()
		c.mu.Lock()

		if c.err != nil {
			break
		}
	}

	if len(c.buffer) > 0 {
		var data interface{}
		data, c.buffer = c.buffer[0], c.buffer[1:]

		c.mu.Unlock()
		err := encoding.Decode(result, data)
		if err != nil {
			c.mu.Lock()
			if c.err == nil {
				c.err = err
			}
			c.mu.Unlock()

			return false
		}

		return true
	}

	c.mu.Unlock()
	return false
}

// All retrieves all documents from the result set into the provided slice
// and closes the cursor.
//
// The result argument must necessarily be the address for a slice. The slice
// may be nil or previously allocated.
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
	return c.Close()
}

// All retrieves a single document from the result set into the provided
// slice and closes the cursor.
func (c *Cursor) One(result interface{}) error {
	ok := c.Next(result)
	if !ok {
		err := c.Err()
		if err == nil {
			return ErrEmptyResult
		}
		return err
	}

	return c.Close()
}

// Tests if the current row is nil.
func (c *Cursor) IsNil() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return (len(c.responses) == 0 && len(c.buffer) == 0) || (len(c.buffer) > 0 && c.buffer[0] == nil)
}

func (c *Cursor) getMore() {
	// Check if response is closed/finished
	if len(c.responses) == 0 && c.closed {
		c.err = errors.New("connection closed, cannot read cursor")
		return
	}
	if len(c.responses) == 0 && c.finished {
		return
	}

	// Otherwise fetch more results
	if len(c.responses) == 0 && !c.finished {
		if err := c.session.continueQuery(c); err != nil {
			c.err = err
			return
		}
	}
	if len(c.responses) == 1 && !c.finished {
		if err := c.session.asyncContinueQuery(c); err != nil {
			c.err = err
			return
		}
	}

	// Load the new response into the buffer
	if len(c.responses) > 0 {
		var err error
		c.buffer, err = deconstructDatums(c.responses[0].GetResponse(), c.opts)
		if err != nil {
			c.err = err

			return
		}
		c.responses = c.responses[1:]
	}
}

func (c *Cursor) extend(response *p.Response) {
	c.mu.Lock()
	c.finished = response.GetType() != p.Response_SUCCESS_PARTIAL &&
		response.GetType() != p.Response_SUCCESS_FEED
	c.responses = append(c.responses, response)

	// Prefetch results if needed
	if len(c.responses) == 1 && !c.finished {
		if err := c.session.asyncContinueQuery(c); err != nil {
			c.err = err
			return
		}
	}

	// Load the new response into the buffer
	var err error
	c.buffer, err = deconstructDatums(c.responses[0].GetResponse(), c.opts)
	if err != nil {
		c.err = err

		return
	}
	c.responses = c.responses[1:]
	c.mu.Unlock()
}
