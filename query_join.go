package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Returns the inner product of two sequences (e.g. a table, a filter result)
// filtered by the predicate. The query compares each row of the left sequence
// with each row of the right sequence to find all pairs of rows which satisfy
// the predicate. When the predicate is satisfied, each matched pair of rows
// of both sequences are combined into a result row.
func (t RqlTerm) InnerJoin(other, predicate interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "InnerJoin", p.Term_INNER_JOIN, []interface{}{other, predicate}, map[string]interface{}{})
}

// Computes a left outer join by retaining each row in the left table even if no
// match was found in the right table.
func (t RqlTerm) OuterJoin(other, predicate interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "OuterJoin", p.Term_OUTER_JOIN, []interface{}{other, predicate}, map[string]interface{}{})
}

type EqJoinOpts struct {
	Index interface{} `gorethink:"index,omitempty"`
}

func (o *EqJoinOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// An efficient join that looks up elements in the right table by primary key.
//
// Optional arguments: "index" (string - name of the index to use in right table instead of the primary key)
func (t RqlTerm) EqJoin(left, right interface{}, optArgs ...EqJoinOpts) RqlTerm {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTermFromPrevVal(t, "EqJoin", p.Term_EQ_JOIN, []interface{}{left, right}, opts)
}

// Used to 'zip' up the result of a join by merging the 'right' fields into 'left'
// fields of each member of the sequence.
func (t RqlTerm) Zip() RqlTerm {
	return newRqlTermFromPrevVal(t, "Zip", p.Term_ZIP, []interface{}{}, map[string]interface{}{})
}
