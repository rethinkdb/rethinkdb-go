package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func DbCreate(name string) RqlTerm {
	return newRqlTerm("DbCreate", p.Term_DB_CREATE, List{name}, Obj{})
}

func DbDrop(name string) RqlTerm {
	return newRqlTerm("DbDrop", p.Term_DB_DROP, List{name}, Obj{})
}

func DbList() RqlTerm {
	return newRqlTerm("DbList", p.Term_DB_LIST, List{}, Obj{})
}
