package gorethink

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/dancannon/gorethink/encoding"
	p "github.com/dancannon/gorethink/ql2"
)

func newCursor(conn *Connection, token int64, term *Term, opts map[string]interface{}) *Cursor {
	cursor := &Cursor{
		conn:  conn,
		token: token,
		term:  term,
		opts:  opts,
	}

	return cursor
}

// Cursors are used to represent data returned from the database.
//
// The code for this struct is based off of mgo's Iter and the official
// python driver's cursor.
type Cursor struct {
	conn  *Connection
	token int64
	query Query
	term  *Term
	opts  map[string]interface{}

	sync.Mutex
	err       error
	fetching  int32
	closed    bool
	finished  bool
	responses []*Response
	profile   interface{}
	buffer    []interface{}
}

// Profile returns the information returned from the query profiler.
func (c *Cursor) Profile() interface{} {
	c.Lock()
	defer c.Unlock()

	return c.profile
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (c *Cursor) Err() error {
	c.Lock()
	defer c.Unlock()

	return c.err
}

// Close closes the cursor, preventing further enumeration. If the end is
// encountered, the cursor is closed automatically. Close is idempotent.
func (c *Cursor) Close() error {
	c.Lock()

	// Stop any unfinished queries
	if !c.closed && !c.finished {
		err := c.conn.StopQuery(c.token)

		if err != nil && (c.err == nil || c.err == ErrEmptyResult) {
			c.err = err
		}
		c.closed = true
	}

	// Return connection to pool
	// err := c.conn.Close()
	// if err != nil {
	// 	return err
	// }

	err := c.err
	c.Unlock()

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
func (c *Cursor) Next(result interface{}) bool {
	ok, data := c.loadNext()
	if !ok {
		return false
	}

	if c.handleError(encoding.Decode(result, data)) != nil {
		return false
	}

	return true
}

func (c *Cursor) loadNext() (bool, interface{}) {
	c.Lock()
	defer c.Unlock()

	// Load more data if needed
	for c.err == nil {
		// Check if response is closed/finished
		if len(c.buffer) == 0 && len(c.responses) == 0 && c.closed {
			c.err = errors.New("connection closed, cannot read cursor")
			return false, nil
		}
		if len(c.buffer) == 0 && len(c.responses) == 0 && c.finished {
			return false, nil
		}

		// Asynchronously loading next batch if possible
		if len(c.responses) == 1 && !c.finished {
			c.fetchMore(false)
		}

		// If the buffer is empty fetch more results
		if len(c.buffer) == 0 {
			if len(c.responses) == 0 && !c.finished {
				c.Unlock()
				err := c.fetchMore(true)
				c.Lock()

				if err != nil {
					return false, nil
				}
			}

			// Load the new response into the buffer
			if len(c.responses) > 0 {
				c.buffer, c.responses = c.responses[0].Responses, c.responses[1:]
			}
		}

		// If the buffer is no longer empty then move on otherwise
		// try again
		if len(c.buffer) > 0 {
			break
		}
	}

	if c.err != nil {
		return false, nil
	}

	var data interface{}
	data, c.buffer = c.buffer[0], c.buffer[1:]

	return true, data
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
	// return c.Close()
	return nil
}

// One retrieves a single document from the result set into the provided
// slice and closes the cursor.
func (c *Cursor) One(result interface{}) error {
	if c.IsNil() {
		return ErrEmptyResult
	}

	var err error
	ok := c.Next(result)
	if !ok {
		err = c.Err()
		if err == nil {
			err = ErrEmptyResult
		}
	}

	// if e := c.Close(); e != nil {
	// 	err = e
	// }

	return err
}

// Tests if the current row is nil.
func (c *Cursor) IsNil() bool {
	c.Lock()
	defer c.Unlock()

	return (len(c.responses) == 0 && len(c.buffer) == 0) || (len(c.buffer) == 1 && c.buffer[0] == nil)
}

func (c *Cursor) handleError(err error) error {
	c.Lock()
	defer c.Unlock()

	if c.err != nil {
		c.err = err
	}

	return err
}

func (c *Cursor) fetchMore(wait bool) error {
	var err error

	if atomic.CompareAndSwapInt32(&c.fetching, 0, 1) {
		var wg sync.WaitGroup

		wg.Add(1)

		go func() {
			c.Lock()
			token := c.token
			conn := c.conn
			c.Unlock()

			err = conn.ContinueQuery(token)
			c.handleError(err)

			wg.Done()
		}()

		if wait {
			wg.Wait()
		}
	}

	return err
}

func (c *Cursor) extend(response *Response) {
	c.Lock()
	defer c.Unlock()

	c.responses = append(c.responses, response)
	c.buffer, c.responses = c.responses[0].Responses, c.responses[1:]
	c.finished = response.Type != p.Response_SUCCESS_PARTIAL && response.Type != p.Response_SUCCESS_FEED
	atomic.StoreInt32(&c.fetching, 0)

	// Asynchronously load next batch if possible
	if len(c.responses) == 1 && !c.finished {
		c.fetchMore(false)
	}
}
