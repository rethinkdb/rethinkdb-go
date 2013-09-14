package rethinkgo

import (
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
	start        int
	end          int
	token        int64
	err          error
	closed       bool
	responseType p.Response_ResponseType
}

func (r *Rows) Close() error {
	var err error

	if !r.closed {
		_, err = r.conn.stopQuery(r.query, r.term)
		r.closed = true
	}

	return err
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

			if row != nil {
				return true
			}
		}

		// Check if all rows have been loaded
		if r.responseType == p.Response_SUCCESS_SEQUENCE {
			r.closed = true
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

		// Continue the query
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
	if r.end <= r.start {
		return false
	}

	r.start++
	return true
}

func (r *Rows) Row() (interface{}, error) {
	data, err := deconstructDatum(r.buffer[r.start])
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (r *Rows) All() ([]interface{}, error) {
	rows := []interface{}{}
	for r.Next() {
		row, err := r.Row()
		if err != nil {
			return []interface{}{}, err
		}

		rows = append(rows, row)
	}

	return rows, nil
}
