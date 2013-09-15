package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) Match(regexp string) RqlTerm {
	return newRqlTermFromPrevVal(t, "Match", p.Term_MATCH, List{regexp}, Obj{})
}
