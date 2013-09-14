package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func Db(name string) RqlTerm {
	return newRqlTerm("Db", p.Term_DB, List{name}, Obj{})
}

func (t RqlTerm) Table(name string) RqlTerm {
	return newRqlTermFromPrevVal(t, "Table", p.Term_TABLE, List{name}, Obj{})
}

func (t RqlTerm) Get(key interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Get", p.Term_GET, List{key}, Obj{})
}

func (t RqlTerm) GetAll(keys ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "GetAll", p.Term_GET_ALL, keys, Obj{})
}

func (t RqlTerm) Between(lowerKey, upperKey interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Table", p.Term_TABLE, List{lowerKey, upperKey}, Obj{})
}

func (t RqlTerm) Filter(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Table", p.Term_TABLE, List{arg}, Obj{})
}
