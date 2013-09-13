package rethinkgo

import (
	p "github.com/christopherhesse/rethinkgo/ql2"
)

func Row() RqlTerm {
	return newRqlTerm("Row", p.Term_IMPLICIT_VAR, List{}, Obj{})
}

func Literal(args ...interface{}) RqlTerm {
	enforceArgLength(0, 1, args)

	return newRqlTerm("Literal", p.Term_LITERAL, args, Obj{})
}

func (t RqlTerm) Field(field interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "(...)", p.Term_GET_FIELD, List{field}, Obj{})
}

func (t RqlTerm) HasFields(fields ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "HasFields", p.Term_HAS_FIELDS, fields, Obj{})
}

func (t RqlTerm) Pluck(fields ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Pluck", p.Term_PLUCK, fields, Obj{})
}

func (t RqlTerm) Without(fields ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Without", p.Term_WITHOUT, fields, Obj{})
}

func (t RqlTerm) Merge(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Merge", p.Term_MERGE, List{arg}, Obj{})
}

func (t RqlTerm) Append(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Append", p.Term_APPEND, List{arg}, Obj{})
}

func (t RqlTerm) Prepend(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Prepend", p.Term_PREPEND, List{arg}, Obj{})
}

func (t RqlTerm) Difference(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Difference", p.Term_DIFFERENCE, List{arg}, Obj{})
}

func (t RqlTerm) SetInsert(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetInsert", p.Term_SET_INSERT, List{arg}, Obj{})
}

func (t RqlTerm) SetUnion(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetUnion", p.Term_SET_UNION, List{arg}, Obj{})
}

func (t RqlTerm) SetIntersection(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetIntersection", p.Term_SET_INTERSECTION, List{arg}, Obj{})
}

func (t RqlTerm) SetDifference(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetDifference", p.Term_SET_DIFFERENCE, List{arg}, Obj{})
}

func (t RqlTerm) InsertAt(args ...interface{}) RqlTerm {
	enforceArgLength(2, 2, args)

	return newRqlTermFromPrevVal(t, "InsertAt", p.Term_INSERT_AT, args, Obj{})
}

func (t RqlTerm) SpliceAt(args ...interface{}) RqlTerm {
	enforceArgLength(2, 2, args)

	return newRqlTermFromPrevVal(t, "SpliceAt", p.Term_SPLICE_AT, args, Obj{})
}

func (t RqlTerm) DeleteAt(args ...interface{}) RqlTerm {
	enforceArgLength(1, 2, args)

	return newRqlTermFromPrevVal(t, "DeleteAt", p.Term_DELETE_AT, args, Obj{})
}

func (t RqlTerm) ChangeAt(args ...interface{}) RqlTerm {
	enforceArgLength(2, 2, args)

	return newRqlTermFromPrevVal(t, "ChangeAt", p.Term_CHANGE_AT, args, Obj{})
}

func (t RqlTerm) Keys() RqlTerm {
	return newRqlTermFromPrevVal(t, "Keys", p.Term_KEYS, List{}, Obj{})
}
