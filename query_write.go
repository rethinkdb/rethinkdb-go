package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) Insert(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Insert", p.Term_INSERT, List{arg}, Obj{})
}

func (t RqlTerm) Update(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Update", p.Term_UPDATE, List{funcWrap(arg)}, Obj{})
}

func (t RqlTerm) Replace(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Replace", p.Term_REPLACE, List{funcWrap(arg)}, Obj{})
}

func (t RqlTerm) Delete() RqlTerm {
	return newRqlTermFromPrevVal(t, "Delete", p.Term_DELETE, List{}, Obj{})
}
