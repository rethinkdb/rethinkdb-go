package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Returns the currently visited document.
var Row = newRqlTerm("Doc", p.Term_IMPLICIT_VAR, []interface{}{}, map[string]interface{}{})

func Literal(args ...interface{}) RqlTerm {
	enforceArgLength(0, 1, args)

	return newRqlTerm("Literal", p.Term_LITERAL, args, map[string]interface{}{})
}

// Get a single field from an object. If called on a sequence, gets that field
// from every object in the sequence, skipping objects that lack it.
func (t RqlTerm) Field(field interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Field", p.Term_GET_FIELD, []interface{}{field}, map[string]interface{}{})
}

// Test if an object has all of the specified fields. An object has a field if
// it has the specified key and that key maps to a non-null value. For instance,
//  the object `{'a':1,'b':2,'c':null}` has the fields `a` and `b`.
func (t RqlTerm) HasFields(fields ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "HasFields", p.Term_HAS_FIELDS, fields, map[string]interface{}{})
}

// Plucks out one or more attributes from either an object or a sequence of
// objects (projection).
func (t RqlTerm) Pluck(fields ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Pluck", p.Term_PLUCK, fields, map[string]interface{}{})
}

// The opposite of pluck; takes an object or a sequence of objects, and returns
// them with the specified paths removed.
func (t RqlTerm) Without(fields ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Without", p.Term_WITHOUT, fields, map[string]interface{}{})
}

// Merge two objects together to construct a new object with properties from both.
// Gives preference to attributes from other when there is a conflict.
func (t RqlTerm) Merge(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Merge", p.Term_MERGE, []interface{}{arg}, map[string]interface{}{})
}

// Append a value to an array.
func (t RqlTerm) Append(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Append", p.Term_APPEND, []interface{}{arg}, map[string]interface{}{})
}

// Prepend a value to an array.
func (t RqlTerm) Prepend(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Prepend", p.Term_PREPEND, []interface{}{arg}, map[string]interface{}{})
}

// Remove the elements of one array from another array.
func (t RqlTerm) Difference(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Difference", p.Term_DIFFERENCE, []interface{}{arg}, map[string]interface{}{})
}

// Add a value to an array and return it as a set (an array with distinct values).
func (t RqlTerm) SetInsert(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetInsert", p.Term_SET_INSERT, []interface{}{arg}, map[string]interface{}{})
}

// Add a several values to an array and return it as a set (an array with
// distinct values).
func (t RqlTerm) SetUnion(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetUnion", p.Term_SET_UNION, []interface{}{arg}, map[string]interface{}{})
}

// Intersect two arrays returning values that occur in both of them as a set (an
// array with distinct values).
func (t RqlTerm) SetIntersection(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetIntersection", p.Term_SET_INTERSECTION, []interface{}{arg}, map[string]interface{}{})
}

// Remove the elements of one array from another and return them as a set (an
// array with distinct values).
func (t RqlTerm) SetDifference(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SetDifference", p.Term_SET_DIFFERENCE, []interface{}{arg}, map[string]interface{}{})
}

// Insert a value in to an array at a given index. Returns the modified array.
func (t RqlTerm) InsertAt(index, value interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "InsertAt", p.Term_INSERT_AT, []interface{}{index, value}, map[string]interface{}{})
}

// Insert several values in to an array at a given index. Returns the modified array.
func (t RqlTerm) SpliceAt(index, value interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "SpliceAt", p.Term_SPLICE_AT, []interface{}{index, value}, map[string]interface{}{})
}

// Remove an element from an array at a given index. Returns the modified array.
func (t RqlTerm) DeleteAt(index interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "DeleteAt", p.Term_DELETE_AT, []interface{}{index}, map[string]interface{}{})
}

// Remove the elements between the given range. Returns the modified array.
func (t RqlTerm) DeleteAtRange(index, endIndex interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "DeleteAt", p.Term_DELETE_AT, []interface{}{index, endIndex}, map[string]interface{}{})
}

// Change a value in an array at a given index. Returns the modified array.
func (t RqlTerm) ChangeAt(index, value interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "ChangeAt", p.Term_CHANGE_AT, []interface{}{index, value}, map[string]interface{}{})
}

// Return an array containing all of the object's keys.
func (t RqlTerm) Keys() RqlTerm {
	return newRqlTermFromPrevVal(t, "Keys", p.Term_KEYS, []interface{}{}, map[string]interface{}{})
}

// Creates an object from a list of key-value pairs, where the keys must be strings.
func Object(args ...interface{}) RqlTerm {
	return newRqlTerm("Object", p.Term_OBJECT, args, map[string]interface{}{})
}
