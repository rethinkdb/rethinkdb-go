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
