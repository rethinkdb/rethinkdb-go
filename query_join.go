package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) InnerJoin(other, predicate interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "InnerJoin", p.Term_INNER_JOIN, []interface{}{other, predicate}, map[string]interface{}{})
}

func (t RqlTerm) OuterJoin(other, predicate interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "OuterJoin", p.Term_OUTER_JOIN, []interface{}{other, predicate}, map[string]interface{}{})
}

func (t RqlTerm) EqJoin(left, right interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"index"}, optArgs)
	return newRqlTermFromPrevVal(t, "EqJoin", p.Term_EQ_JOIN, []interface{}{left, right}, optArgM)
}

func (t RqlTerm) Zip() RqlTerm {
	return newRqlTermFromPrevVal(t, "Zip", p.Term_ZIP, []interface{}{}, map[string]interface{}{})
}
