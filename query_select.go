package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func Db(name interface{}) RqlTerm {
	return newRqlTerm("Db", p.Term_DB, List{name}, Obj{})
}

func (t RqlTerm) Table(name interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"use_outdated"}, optArgs)
	return newRqlTermFromPrevVal(t, "Table", p.Term_TABLE, List{name}, optArgM)
}

func (t RqlTerm) Get(key interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Get", p.Term_GET, List{key}, Obj{})
}

func (t RqlTerm) GetAll(keys ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "GetAll", p.Term_GET_ALL, keys, Obj{})
}

func (t RqlTerm) GetAllByIndex(index interface{}, keys ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "GetAll", p.Term_GET_ALL, keys, Obj{"index": index})
}

func (t RqlTerm) Between(lowerKey, upperKey interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"index", "left_bound", "right_bound"}, optArgs)
	return newRqlTermFromPrevVal(t, "Between", p.Term_BETWEEN, List{lowerKey, upperKey}, optArgM)
}

func (t RqlTerm) Filter(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Filter", p.Term_FILTER, List{funcWrap(f)}, Obj{})
}
