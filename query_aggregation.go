package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Aggregation
// These commands are used to compute smaller values from large sequences.

func (t RqlTerm) Reduce(f, base interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Reduce", p.Term_REDUCE, List{funcWrap(f), base}, Obj{})
}

func (t RqlTerm) Count(args ...interface{}) RqlTerm {
	enforceArgLength(0, 1, args)
	for k, v := range args {
		args[k] = funcWrap(v)
	}
	return newRqlTermFromPrevVal(t, "Count", p.Term_COUNT, args, Obj{})
}

func (t RqlTerm) Distinct() RqlTerm {
	return newRqlTermFromPrevVal(t, "Distinct", p.Term_DISTINCT, List{}, Obj{})
}

func (t RqlTerm) GroupedMapReduce(grouping, mapping, reduction, base interface{}) RqlTerm {
	return newRqlTermFromPrevVal(
		t,
		"GroupedMapReduce",
		p.Term_GROUPED_MAP_REDUCE,
		List{funcWrap(grouping), funcWrap(mapping), funcWrap(reduction), base},
		Obj{},
	)
}

func (t RqlTerm) GroupBy(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "GroupBy", p.Term_GROUPBY, args, Obj{})
}

func (t RqlTerm) Contains(args ...interface{}) RqlTerm {
	for k, v := range args {
		args[k] = funcWrap(v)
	}

	return newRqlTermFromPrevVal(t, "Contains", p.Term_CONTAINS, args, Obj{})
}

// Aggregators
// These standard aggregator objects are to be used in conjunction with group_by.

// Count the total size of the group.
func Count() RqlTerm {
	return newRqlTerm("Count", p.Term_COUNT, List{}, Obj{})
}

// Compute the sum of the given field in the group.
func Sum(arg interface{}) RqlTerm {
	return newRqlTerm("Count", p.Term_COUNT, List{arg}, Obj{})
}

// Compute the average value of the given attribute for the group.
func Avg(arg interface{}) RqlTerm {
	return newRqlTerm("Count", p.Term_COUNT, List{arg}, Obj{})
}
