package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) TableCreate(name interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"primary_key", "durability", "cache_size", "datacenter"}, optArgs)
	return newRqlTermFromPrevVal(t, "TableCreate", p.Term_TABLE_CREATE, List{name}, optArgM)
}

func (t RqlTerm) TableDrop(name interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "TableDrop", p.Term_TABLE_DROP, List{name}, Obj{})
}

func (t RqlTerm) TableList() RqlTerm {
	return newRqlTermFromPrevVal(t, "TableList", p.Term_TABLE_LIST, List{}, Obj{})
}

func (t RqlTerm) IndexCreate(name interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexCreate", p.Term_INDEX_CREATE, List{name}, Obj{})
}

func (t RqlTerm) IndexCreateFunc(name, f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexCreate", p.Term_INDEX_CREATE, List{name, funcWrap(f)}, Obj{})
}

func (t RqlTerm) IndexDrop(name interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexDrop", p.Term_INDEX_DROP, List{name}, Obj{})
}

func (t RqlTerm) IndexList() RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexList", p.Term_INDEX_LIST, List{}, Obj{})
}
