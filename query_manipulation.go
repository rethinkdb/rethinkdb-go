package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Returns the currently visited document.
var Row = newRqlTerm("Doc", p.Term_IMPLICIT_VAR, []interface{}{}, map[string]interface{}{})

func Literal(args ...interface{}) RqlTerm {
	enforceArgLength(0, 1, args)

	return newRqlTerm("Literal", p.Term_LITERAL, args, map[string]interface{}{})
}

func (t RqlTerm) Field(field interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "(...)", p.Term_GET_FIELD, []interface{}{field}, map[string]interface{}{})
}

func (t RqlTerm) HasFields(fields ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "HasFields", p.Term_HAS_FIELDS, fields, map[string]interface{}{})
}

func (t RqlTerm) Pluck(fields ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Pluck", p.Term_PLUCK, fields, map[string]interface{}{})
}

func (t RqlTerm) Without(fields ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Without", p.Term_WITHOUT, fields, map[string]interface{}{})
}

func (t RqlTerm) Merge(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Merge", p.Term_MERGE, []interface{}{arg}, map[string]interface{}{})
}

func (t RqlTerm) Append(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Append", p.Term_APPEND, []interface{}{arg}, map[string]interface{}{})
}

func (t RqlTerm) Prepend(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Prepend", p.Term_PREPEND, []interface{}{arg}, map[string]interface{}{})
}

func (t RqlTerm) Difference(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Difference", p.Term_DIFFERENCE, []interface{}{arg}, map[string]interface{}{})
}

func (t RqlTerm) SetInsert(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetInsert", p.Term_SET_INSERT, []interface{}{arg}, map[string]interface{}{})
}

func (t RqlTerm) SetUnion(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetUnion", p.Term_SET_UNION, []interface{}{arg}, map[string]interface{}{})
}

func (t RqlTerm) SetIntersection(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetIntersection", p.Term_SET_INTERSECTION, []interface{}{arg}, map[string]interface{}{})
}

func (t RqlTerm) SetDifference(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetDifference", p.Term_SET_DIFFERENCE, []interface{}{arg}, map[string]interface{}{})
}

func (t RqlTerm) InsertAt(index, value interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "InsertAt", p.Term_INSERT_AT, []interface{}{index, value}, map[string]interface{}{})
}

func (t RqlTerm) SpliceAt(index, value interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SpliceAt", p.Term_SPLICE_AT, []interface{}{index, value}, map[string]interface{}{})
}

func (t RqlTerm) DeleteAt(index interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "DeleteAt", p.Term_DELETE_AT, []interface{}{index}, map[string]interface{}{})
}

func (t RqlTerm) DeleteAtRange(index, endIndex interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "DeleteAt", p.Term_DELETE_AT, []interface{}{index, endIndex}, map[string]interface{}{})
}

func (t RqlTerm) ChangeAt(index, value interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "ChangeAt", p.Term_CHANGE_AT, []interface{}{index, value}, map[string]interface{}{})
}

func (t RqlTerm) Keys() RqlTerm {
	return newRqlTermFromPrevVal(t, "Keys", p.Term_KEYS, []interface{}{}, map[string]interface{}{})
}
