package rethinkgo

import (
	"github.com/dancannon/gorethink/mapping"
	p "github.com/dancannon/gorethink/ql2"
)

type Row struct {
	err  error
	rows *Rows
}

// func (r *Row) Scan(dest interface{}) error {
// var v reflect.Value
// v = reflect.ValueOf(dest)
// if v.Kind() != reflect.Ptr {
// 	return errors.New("Must pass a pointer, not a value, to Scan destination.")
// }

// direct := reflect.Indirect(v)
// base, err := BaseStructType(direct.Type())
// if err != nil {
// 	return err
// }

// fm, err := getFieldmap(base)
// if err != nil {
// 	return err
// }

// columns, err := r.Columns()
// if err != nil {
// 	return err
// }

// fields, err := getFields(fm, columns)
// if err != nil {
// 	return err
// }

// values := make([]interface{}, len(columns))
// // create a new struct type (which returns PtrTo) and indirect it
// setValues(fields, reflect.Indirect(v), values)
// // scan into the struct field pointers and append to our results

// if r.err != nil {
// 	return err
// }

// defer r.rows.Close()
// if !r.rows.Next() {
// 	panic("No rows")
// }
// return r.rows.Scan(values...)
// }

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
	if r.closed {
		return false
	}

	if r.err != nil {
		return false
	}

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

	r.advance()
	r.current = r.buffer[r.start]

	return true
}

func (r *Rows) advance() bool {
	if r.end <= r.start {
		return false
	}

	r.start++
	return true
}

func (r *Rows) Scan(dest interface{}) error {
	if r.current == nil {
		return nil
	}

	data, err := deconstructDatum(r.current)
	if err != nil {
		return err
	}

	decoder := mapping.NewDecoder()
	err = decoder.Decode(dest, data)
	if err != nil {
		return err
	}

	return nil
}

// func (r *Rows) All() ([]interface{}, error) {
// 	rows := []interface{}{}
// 	for r.Next() {
// 		row, err := r.Row()
// 		if err != nil {
// 			return []interface{}{}, err
// 		}

// 		rows = append(rows, row)
// 	}

// 	return rows, nil
// }
