package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) Insert(arg interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"durability", "return_vals", "cache_size", "upsert"}, optArgs)
	return newRqlTermFromPrevVal(t, "Insert", p.Term_INSERT, []interface{}{funcWrap(arg)}, optArgM)
}

func (t RqlTerm) Update(arg interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"durability", "return_vals", "non_atomic"}, optArgs)
	return newRqlTermFromPrevVal(t, "Update", p.Term_UPDATE, []interface{}{funcWrap(arg)}, optArgM)
}

func (t RqlTerm) Replace(arg interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"durability", "return_vals", "non_atomic"}, optArgs)
	return newRqlTermFromPrevVal(t, "Replace", p.Term_REPLACE, []interface{}{funcWrap(arg)}, optArgM)
}

func (t RqlTerm) Delete(optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"durability", "return_vals"}, optArgs)
	return newRqlTermFromPrevVal(t, "Delete", p.Term_DELETE, []interface{}{}, optArgM)
}
