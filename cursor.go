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

// Cursor is the result of a query. Its cursor starts before the first row
// of the result set. Use Next to advance through the rows:
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
	pc          *poolConn
	releaseConn func(error)

	conn  *Connection
	token int64
	query Query
	term  *Term
	opts  map[string]interface{}

	sync.Mutex
	lastErr   error
	fetching  int32
	closed    bool
	finished  bool
	responses []*Response
	profile   interface{}
	buffer    []interface{}
}

// Profile returns the information returned from the query profiler.
func (c *Cursor) Profile() interface{} {
	return c.profile
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (c *Cursor) Err() error {
	return c.lastErr
}

// Close closes the cursor, preventing further enumeration. If the end is
// encountered, the cursor is closed automatically. Close is idempotent.
func (c *Cursor) Close() error {
	var err error

	if c.closed {
		return nil
	}

	conn := c.conn
	if conn == nil {
		return nil
	}
	if conn.conn == nil {
		return nil
	}

	// Stop any unfinished queries
	if !c.closed && !c.finished {
		q := Query{
			Type:  p.Query_STOP,
			Token: c.token,
		}

		_, _, err = conn.Query(q, map[string]interface{}{})
	}

	c.closed = true
	c.conn = nil
	c.releaseConn(err)

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
func (c *Cursor) Next(dest interface{}) bool {
	var hasMore bool

	if c.closed {
		return false
	}

	hasMore, c.lastErr = c.loadNext(dest)
	if c.lastErr != nil {
		c.Close()
		return false
	}

	return hasMore
}

func (c *Cursor) loadNext(dest interface{}) (bool, error) {
	var err error

	// Load more data if needed
	for err == nil {
		// Check if response is closed/finished
		if len(c.buffer) == 0 && len(c.responses) == 0 && c.closed {
			err = errors.New("connection closed, cannot read cursor")
			return false, err
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
				err = c.fetchMore(true)
				if err != nil {
					return false, err
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

	// Decode result into dest value
	var data interface{}
	data, c.buffer = c.buffer[0], c.buffer[1:]

	err = encoding.Decode(dest, data)
	if err != nil {
		return false, err
	}

	return true, nil
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

	if c.lastErr != nil {
		c.Close()
		return c.lastErr
	}

	return c.Close()
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

	if e := c.Close(); e != nil {
		err = e
	}

	return err
}

// IsNil tests if the current row is nil.
func (c *Cursor) IsNil() bool {
	return (len(c.responses) == 0 && len(c.buffer) == 0) || (len(c.buffer) == 1 && c.buffer[0] == nil)
}

// fetchMore fetches more rows from the database.
//
// If wait is true then it will wait for the database to reply otherwise it
// will return after sending the continue query.
func (c *Cursor) fetchMore(wait bool) error {
	var err error

	if atomic.CompareAndSwapInt32(&c.fetching, 0, 1) {
		var wg sync.WaitGroup

		wg.Add(1)

		q := Query{
			Type:  p.Query_CONTINUE,
			Token: c.token,
		}

		go func() {
			_, _, err = c.conn.Query(q, map[string]interface{}{})
			c.handleError(err)

			wg.Done()
		}()

		if wait {
			wg.Wait()
		}
	}

	return err
}

// handleError sets the value of lastErr to err if lastErr is not yet set.
func (c *Cursor) handleError(err error) error {
	c.Lock()
	defer c.Unlock()

	if c.lastErr != nil {
		c.lastErr = err
	}

	return c.lastErr
}

// extend adds the result of a continue query to the cursor.
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
