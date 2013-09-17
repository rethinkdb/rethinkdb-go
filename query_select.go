package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func Db(name interface{}) RqlTerm {
	return newRqlTerm("Db", p.Term_DB, []interface{}{name}, map[string]interface{}{})
}

func (t RqlTerm) Table(name interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"use_outdated"}, optArgs)
	return newRqlTermFromPrevVal(t, "Table", p.Term_TABLE, []interface{}{name}, optArgM)
}

func (t RqlTerm) Get(key interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Get", p.Term_GET, []interface{}{key}, map[string]interface{}{})
}

func (t RqlTerm) GetAll(keys ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "GetAll", p.Term_GET_ALL, keys, map[string]interface{}{})
}

func (t RqlTerm) GetAllByIndex(index interface{}, keys ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "GetAll", p.Term_GET_ALL, keys, map[string]interface{}{"index": index})
}

func (t RqlTerm) Between(lowerKey, upperKey interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"index", "left_bound", "right_bound"}, optArgs)
	return newRqlTermFromPrevVal(t, "Between", p.Term_BETWEEN, []interface{}{lowerKey, upperKey}, optArgM)
}

func (t RqlTerm) Filter(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Filter", p.Term_FILTER, []interface{}{funcWrap(f)}, map[string]interface{}{})
}
