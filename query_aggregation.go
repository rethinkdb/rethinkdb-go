package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Aggregation
// These commands are used to compute smaller values from large sequences.

// Produce a single value from a sequence through repeated application of a
// reduction function.
//
// The reduce function gets invoked repeatedly not only for the input values but
// also for results of previous reduce invocations. The type and format of the
// object that is passed in to reduce must be the same with the one returned
// from reduce.
func (t RqlTerm) Reduce(f, base interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Reduce", p.Term_REDUCE, []interface{}{funcWrap(f)}, map[string]interface{}{"base": base})
}

// Count the number of elements in the sequence.
func (t RqlTerm) Count() RqlTerm {
	return newRqlTermFromPrevVal(t, "Count", p.Term_COUNT, []interface{}{}, map[string]interface{}{})
}

// Count the number of elements in the sequence. CountFiltered uses the argument
// passed to it to filter the results before counting.
func (t RqlTerm) CountFiltered(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Count", p.Term_COUNT, []interface{}{funcWrap(f)}, map[string]interface{}{})
}

// Remove duplicate elements from the sequence.
func (t RqlTerm) Distinct() RqlTerm {
	return newRqlTermFromPrevVal(t, "Distinct", p.Term_DISTINCT, []interface{}{}, map[string]interface{}{})
}

// Partition the sequence into groups based on the grouping function. The elements
// of each group are then mapped using the mapping function and reduced using the
// reduction function.
func (t RqlTerm) GroupedMapReduce(grouping, mapping, reduction, base interface{}) RqlTerm {
	return newRqlTermFromPrevVal(
		t,
		"GroupedMapReduce",
		p.Term_GROUPED_MAP_REDUCE,
		[]interface{}{funcWrap(grouping), funcWrap(mapping), funcWrap(reduction)},
		map[string]interface{}{"base": base},
	)
}

// Groups elements by the values of the given attributes and then applies the given
// reduction. Though similar to GroupedMapReduce, GroupBy takes a standardized
// object for specifying the reduction. Can be used with a number of predefined
// common reductions
func (t RqlTerm) GroupBy(collector interface{}, args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "GroupBy", p.Term_GROUPBY, []interface{}{args, collector}, map[string]interface{}{})
}

//Returns whether or not a sequence contains all the specified values, or if
//functions are provided instead, returns whether or not a sequence contains
//values matching all the specified functions.
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
