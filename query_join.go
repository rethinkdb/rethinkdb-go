package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) InnerJoin(other, predicate interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "InnerJoin", p.Term_INNER_JOIN, List{other, predicate}, Obj{})
}

func (t RqlTerm) OuterJoin(other, predicate interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "OuterJoin", p.Term_OUTER_JOIN, List{other, predicate}, Obj{})
}

func (t RqlTerm) EqJoin(left, right interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "EqJoin", p.Term_EQ_JOIN, List{left, right}, Obj{})
}

func (t RqlTerm) Zip() RqlTerm {
	return newRqlTermFromPrevVal(t, "Zip", p.Term_ZIP, List{}, Obj{})
}
