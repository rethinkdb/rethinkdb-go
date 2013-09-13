package rethinkgo

import (
	p "github.com/dancannon/rethinkgo/ql2"
)

type Row struct {
	*p.Datum
}

type Result struct {
	conn         Connection
	query        *p.Query
	term         *p.Term
	opts         map[string]interface{}
	buffer       []*Row
	current      *Row
	token        int64
	err          error
	responseType p.Response_ResponseType
}

func (r Result) fetchMore() bool {
	if r.Complete() {
		return false
	}

	// Load more results
	results, err := conn.continueQuery(r.query, r.term, opts)
	if err != nil {
		return false
	} else {
		r.buffer = results
		r.current = 0
		return true
	}
}

func (r Result) Complete() bool {
	return r.responseType == p.Response_SUCCESS_SEQUENCE
}

func (r Result) HasNext() Row {
	return !r.Complete() && len(r.buffer)-1 >= r.current+1
}

func (r Result) Scan() bool {
	if len(r.buffer)-1 >= r.current+1 {
		if current, ok := r.buffer[r.current+1]; ok {
			return current
		}
	}
}

func (r Result) Row() Row {
	return r.current
}
