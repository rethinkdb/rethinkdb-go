package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) TableCreate(name interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"primary_key", "durability", "cache_size", "datacenter"}, optArgs)
	return newRqlTermFromPrevVal(t, "TableCreate", p.Term_TABLE_CREATE, []interface{}{name}, optArgM)
}

func (t RqlTerm) TableDrop(name interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "TableDrop", p.Term_TABLE_DROP, []interface{}{name}, map[string]interface{}{})
}

func (t RqlTerm) TableList() RqlTerm {
	return newRqlTermFromPrevVal(t, "TableList", p.Term_TABLE_LIST, []interface{}{}, map[string]interface{}{})
}

func (t RqlTerm) IndexCreate(name interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexCreate", p.Term_INDEX_CREATE, []interface{}{name}, map[string]interface{}{})
}

func (t RqlTerm) IndexCreateFunc(name, f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexCreate", p.Term_INDEX_CREATE, []interface{}{name, funcWrap(f)}, map[string]interface{}{})
}

func (t RqlTerm) IndexDrop(name interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexDrop", p.Term_INDEX_DROP, []interface{}{name}, map[string]interface{}{})
}

func (t RqlTerm) IndexList() RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexList", p.Term_INDEX_LIST, []interface{}{}, map[string]interface{}{})
}
