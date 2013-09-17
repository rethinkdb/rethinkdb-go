package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func DbCreate(name interface{}) RqlTerm {
	return newRqlTerm("DbCreate", p.Term_DB_CREATE, []interface{}{name}, map[string]interface{}{})
}

func DbDrop(name interface{}) RqlTerm {
	return newRqlTerm("DbDrop", p.Term_DB_DROP, []interface{}{name}, map[string]interface{}{})
}

func DbList() RqlTerm {
	return newRqlTerm("DbList", p.Term_DB_LIST, []interface{}{}, map[string]interface{}{})
}
