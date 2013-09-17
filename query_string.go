package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) Match(regexp interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Match", p.Term_MATCH, []interface{}{regexp}, map[string]interface{}{})
}
