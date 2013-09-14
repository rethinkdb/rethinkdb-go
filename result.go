package rethinkgo

import (
	"fmt"
	p "github.com/dancannon/gorethink/ql2"
)

// Used when query is to be executed and no results returned
type Result struct {
}

// Rows contains the result of a query. Its cursor starts before the first row
// of the result set. Use Next to advance through the rows.
type Rows struct {
	conn         *Connection
	query        *p.Query
	term         RqlTerm
	opts         map[string]interface{}
	buffer       []*p.Datum
	current      *p.Datum
	start        int
	end          int
	token        int64
	err          error
	responseType p.Response_ResponseType
}

func (r *Rows) Close() (err error) {
	_, err = r.conn.stopQuery(r.query, r.term)
	if err != nil {
		return
	}
	err = r.conn.Close()

	return
}

func (r *Rows) Err() error {
	return r.err
}

func (r *Rows) Next() bool {
	for {
		// Attempt to get a result in the buffer
		if r.end > r.start {
			row := r.buffer[r.start]

			if !r.advance() {
				return false
			}

			r.current = row
			if row != nil {
				return true
			}
		}

		// Check if all rows have been loaded
		if r.responseType == p.Response_SUCCESS_SEQUENCE {
			r.start = 0
			r.end = 0
			return false
		}

		// Load more data from the database

		// First, shift data to beginning of buffer if there's lots of empty space
		// or space is neded.
		if r.start > 0 && (r.end == len(r.buffer) || r.start > len(r.buffer)/2) {
			copy(r.buffer, r.buffer[r.start:r.end])
			r.end -= r.start
			r.start = 0
		}

		// Resize buffer if needed
		// Is the buffer full? If so, resize.
		if r.end == len(r.buffer) {
			newSize := len(r.buffer) * 2
			newBuf := make([]*p.Datum, newSize)
			copy(newBuf, r.buffer[r.start:r.end])
			r.buffer = newBuf
			r.end -= r.start
			r.start = 0
			continue
		}

		// Continue the query
		fmt.Println("Cont")
		newResult, err := r.conn.continueQuery(r.query, r.term)
		if err != nil {
			r.err = err
			return false
		}

		r.buffer = append(r.buffer, newResult.buffer...)
		r.end += len(newResult.buffer)
	}
}

func (r *Rows) advance() bool {
	if 1 > r.end-r.start {
		return false
	}

	r.start++
	return true
}

func (r *Rows) Row() interface{} {
	data, err := deconstructDatum(r.current)
	if err != nil {
		return nil
	}

	return data
}

func (r *Rows) All() []interface{} {
	rows := []interface{}{}
	for r.Next() {
		rows = append(rows, r.Row())
	}

	return rows
}
