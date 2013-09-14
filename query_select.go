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
