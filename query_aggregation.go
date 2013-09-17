package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Aggregation
// These commands are used to compute smaller values from large sequences.

func (t RqlTerm) Reduce(f, base interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Reduce", p.Term_REDUCE, []interface{}{funcWrap(f)}, map[string]interface{}{"base": base})
}

func (t RqlTerm) Count() RqlTerm {
	return newRqlTermFromPrevVal(t, "Count", p.Term_COUNT, []interface{}{}, map[string]interface{}{})
}

func (t RqlTerm) CountFiltered(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Count", p.Term_COUNT, []interface{}{funcWrap(f)}, map[string]interface{}{})
}

func (t RqlTerm) Distinct() RqlTerm {
	return newRqlTermFromPrevVal(t, "Distinct", p.Term_DISTINCT, []interface{}{}, map[string]interface{}{})
}

func (t RqlTerm) GroupedMapReduce(grouping, mapping, reduction, base interface{}) RqlTerm {
	return newRqlTermFromPrevVal(
		t,
		"GroupedMapReduce",
		p.Term_GROUPED_MAP_REDUCE,
		[]interface{}{funcWrap(grouping), funcWrap(mapping), funcWrap(reduction)},
		map[string]interface{}{"base": base},
	)
}

func (t RqlTerm) GroupBy(collector interface{}, args ...interface{}) RqlTerm {

	return newRqlTermFromPrevVal(t, "GroupBy", p.Term_GROUPBY, []interface{}{args, collector}, map[string]interface{}{})
}

func (t RqlTerm) Contains(args ...interface{}) RqlTerm {
	for k, v := range args {
		args[k] = funcWrap(v)
	}

	return newRqlTermFromPrevVal(t, "Contains", p.Term_CONTAINS, args, map[string]interface{}{})
}

// Aggregators
// These standard aggregator objects are to be used in conjunction with group_by.

// Count the total size of the group.
func Count() RqlTerm {
	return Expr(map[string]interface{}{
		"COUNT": true,
	})
}

// Compute the sum of the given field in the group.
func Sum(arg interface{}) RqlTerm {
	return Expr(map[string]interface{}{
		"SUM": arg,
	})
}

// Compute the average value of the given attribute for the group.
func Avg(arg interface{}) RqlTerm {
	return Expr(map[string]interface{}{
		"AVG": arg,
	})
}
