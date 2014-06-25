package gorethink

import (
	"reflect"
	"sync"

	"github.com/dancannon/gorethink/encoding"
	p "github.com/dancannon/gorethink/ql2"
)

// ResultRow contains the result of a RunRow query
type ResultRow struct {
	err  error
	rows *ResultRows
}

func (r *ResultRow) Profile() interface{} {
	return r.rows.Profile()
}

func (r *ResultRow) Err() error {
	return r.err
}

// Scan copies the result from the matched row into the value pointed at by dest.
// If more than one row is returned by the query then Scan returns the first and
// ignores the rest. If no row is found then Scan returns an error.
//
// RethinkDB returns an nil value on Get queries when nothing is found, and Scan
// won't fail in this case.
func (r *ResultRow) Scan(dest interface{}) error {
	if r.err != nil {
		return r.err
	}

	return r.rows.Scan(dest)
}

// Tests if the result is nil.
// RethinkDB returns an nil value on Get queries when nothing is found.
func (r *ResultRow) IsNil() bool {
	if r.err != nil {
		return true
	}
	return r.rows.IsNil()
}

// ResultRows contains the result of a query. Its cursor starts before the first row
// of the result set. Use Next to advance through the rows.
type ResultRows struct {
	sync.Mutex

	session *Session
	query   *p.Query
	term    Term
	opts    map[string]interface{}

	profile interface{}

	initialized bool
	closed      bool
	err         error

	outstandingRequests int
	finished            bool
	responses           []*p.Response
	current             interface{}
	buffer              []interface{}
}

func (r *ResultRows) Profile() interface{} {
	return r.profile
}

// Close closes the Rows, preventing further enumeration. If the end is
// encountered, the Rows are closed automatically. Close is idempotent.
func (r *ResultRows) Close() error {
	var err error

	if !r.closed {
		err = r.session.stopQuery(r)
		r.closed = true
	}

	return err
}

// Err returns the error, if any, that was encountered during iteration.
func (r *ResultRows) Err() error {
	return r.err
}

// Next prepares the next row for reading. It returns true on success or false
// if there are no more rows. Every call to scan must be preceded by a call
// to next. If all rows in the buffer have been read and a partial sequence was
// returned then Next will load more from the database
func (r *ResultRows) Next() bool {
	for {
		if r.closed || r.err != nil {
			return false
		}

		if !r.initialized {
			r.initialized = true
		}

		// Attempt to load a row from the buffer
		if len(r.buffer) > 0 {
			r.current, r.buffer = r.buffer[0], r.buffer[1:]
			return true
		}

		// Fetch new batch from the server
		if len(r.responses) == 0 && !r.finished {
			if err := r.session.continueQuery(r); err != nil {
				r.err = err

				return false
			}
		}

		// Check if we  are finished
		if len(r.responses) == 0 && r.finished {
			return false
		}

		// If we have more batches in the response cache then load a new response
		// into the buffer
		if len(r.responses) > 0 {
			v, err := deconstructDatums(r.responses[0].GetResponse(), r.opts)
			if err != nil {
				r.err = err

				return false
			}
			r.buffer = v
			r.responses = r.responses[1:]
		}

		// Check the buffer again to make sure it is not empty
		if len(r.buffer) > 0 {
			r.current, r.buffer = r.buffer[0], r.buffer[1:]

			return true
		}
	}
}

func (r *ResultRows) extend(response *p.Response) {
	r.finished = response.GetType() != p.Response_SUCCESS_PARTIAL &&
		response.GetType() != p.Response_SUCCESS_FEED
	r.Lock()
	r.responses = append(r.responses, response)
	r.Unlock()
}

// Scan copies the result in the current row into the value pointed at by dest.
//
// If an argument is of type *interface{}, Scan copies the value provided by the
// database without conversion.
//
// If the value is a struct then Scan traverses
// the result recursively and attempts to match the keys returned by the database
// to the name used by the structs field (either the struct field name or its
// key).
func (r *ResultRows) Scan(dest interface{}) error {
	if r.err != nil {
		return r.err
	}
	if r.initialized == false {
		return RqlDriverError{"Scan called without calling Next"}
	}

	err := encoding.Decode(dest, r.current)
	if err != nil {
		return err
	}

	return nil
}

// ScanAll copies all the rows in the result buffer into the value pointed at by
// dest.
func (r *ResultRows) ScanAll(dest interface{}) error {
	// Validate the data types
	pval := reflect.ValueOf(dest)
	if pval.Kind() != reflect.Ptr {
		return RqlDriverError{"ScanAll must be passed a pointer"}
	}

	val := pval.Elem()
	if val.Kind() != reflect.Slice {
		return RqlDriverError{"ScanAll must be passed a pointer to a slice"}
	}

	elems := reflect.MakeSlice(val.Type(), 0, 0)

	// Iterate through each row in the buffer and scan into an element of the slice
	for r.Next() {
		elem := reflect.New(val.Type().Elem())

		err := r.Scan(elem.Interface())
		if err != nil {
			return err
		}

		elems = reflect.Append(elems, elem.Elem())
	}

	if r.err != nil {
		return r.err
	}

	// Copy the value from the temporary slice to the destination
	val.Set(elems)

	return nil
}

// Tests if the current row is nil.
func (r *ResultRows) IsNil() bool {
	return len(r.responses) == 0 && r.current == nil
}
