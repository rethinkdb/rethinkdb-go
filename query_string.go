package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Match against a regular expression. Returns a match object containing the
// matched string, that string's start/end position, and the capture groups.
//
//	Expr("id:0,name:mlucy,foo:bar").Match("name:(\\w+)").Field("groups").Nth(0).Field("str")
func (t RqlTerm) Match(regexp interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Match", p.Term_MATCH, []interface{}{regexp}, map[string]interface{}{})
}

// Splits a string into substrings. Splits on whitespace when called with no arguments.
// When called with a separator, splits on that separator. When called with a separator
// and a maximum number of splits, splits on that separator at most max_splits times.
// (Can be called with null as the separator if you want to split on whitespace while still
// specifying max_splits.)
//
// Mimics the behavior of Python's string.split in edge cases, except for splitting on the
// empty string, which instead produces an array of single-character strings.
func (t RqlTerm) Split(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Split", p.Term_SPLIT, args, map[string]interface{}{})
}

// Upcases a string.
func (t RqlTerm) Upcase() RqlTerm {
	return newRqlTermFromPrevVal(t, "Upcase", p.Term_UPCASE, []interface{}{}, map[string]interface{}{})
}

// Downcases a string.
func (t RqlTerm) Downcase() RqlTerm {
	return newRqlTermFromPrevVal(t, "Downcase", p.Term_DOWNCASE, []interface{}{}, map[string]interface{}{})
}
