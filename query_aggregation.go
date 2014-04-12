package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Aggregation
// These commands are used to compute smaller values from large sequences.

// Produce a single value from a sequence through repeated application of a
// reduction function
func (t RqlTerm) Reduce(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Reduce", p.Term_REDUCE, []interface{}{funcWrap(f)}, map[string]interface{}{})
}

// Remove duplicate elements from the sequence.
func (t RqlTerm) Distinct() RqlTerm {
	return newRqlTermFromPrevVal(t, "Distinct", p.Term_DISTINCT, []interface{}{}, map[string]interface{}{})
}

// Takes a stream and partitions it into multiple groups based on the
// fields or functions provided. Commands chained after group will be
//  called on each of these grouped sub-streams, producing grouped data.
func (t RqlTerm) Group(fieldOrFunctions ...interface{}) RqlTerm {
	for k, v := range fieldOrFunctions {
		fieldOrFunctions[k] = funcWrap(v)
	}

	return newRqlTermFromPrevVal(t, "Group", p.Term_GROUP, fieldOrFunctions, map[string]interface{}{})
}

// Takes a stream and partitions it into multiple groups based on the
// fields or functions provided. Commands chained after group will be
// called on each of these grouped sub-streams, producing grouped data.
func (t RqlTerm) GroupByIndex(index interface{}, fieldOrFunctions ...interface{}) RqlTerm {
	for k, v := range fieldOrFunctions {
		fieldOrFunctions[k] = funcWrap(v)
	}

	return newRqlTermFromPrevVal(t, "Group", p.Term_GROUP, fieldOrFunctions, map[string]interface{}{
		"index": index,
	})
}

func (t RqlTerm) Ungroup() RqlTerm {
	return newRqlTermFromPrevVal(t, "Ungroup", p.Term_UNGROUP, []interface{}{}, map[string]interface{}{})
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

// Count the number of elements in the sequence. With a single argument,
// count the number of elements equal to it. If the argument is a function,
// it is equivalent to calling filter before count.
func (t RqlTerm) Count(filters ...interface{}) RqlTerm {
	for k, v := range filters {
		filters[k] = funcWrap(v)
	}

	return newRqlTermFromPrevVal(t, "Count", p.Term_COUNT, filters, map[string]interface{}{})
}

// Sums all the elements of a sequence. If called with a field name, sums all
// the values of that field in the sequence, skipping elements of the sequence
// that lack that field. If called with a function, calls that function on every
// element of the sequence and sums the results, skipping elements of the
// sequence where that function returns null or a non-existence error.
func (t RqlTerm) Sum(fieldOrFunctions ...interface{}) RqlTerm {
	for k, v := range fieldOrFunctions {
		fieldOrFunctions[k] = funcWrap(v)
	}

	return newRqlTermFromPrevVal(t, "Sum", p.Term_SUM, fieldOrFunctions, map[string]interface{}{})
}

// Averages all the elements of a sequence. If called with a field name, averages
// all the values of that field in the sequence, skipping elements of the sequence
// that lack that field. If called with a function, calls that function on every
// element of the sequence and averages the results, skipping elements of the
// sequence where that function returns null or a non-existence error.
func (t RqlTerm) Avg(fieldOrFunctions ...interface{}) RqlTerm {
	for k, v := range fieldOrFunctions {
		fieldOrFunctions[k] = funcWrap(v)
	}

	return newRqlTermFromPrevVal(t, "Sum", p.Term_SUM, fieldOrFunctions, map[string]interface{}{})
}

// Finds the minimum of a sequence. If called with a field name, finds the element
// of that sequence with the smallest value in that field. If called with a function,
// calls that function on every element of the sequence and returns the element
// which produced the smallest value, ignoring any elements where the function
// returns null or produces a non-existence error.
func (t RqlTerm) Min(fieldOrFunctions ...interface{}) RqlTerm {
	for k, v := range fieldOrFunctions {
		fieldOrFunctions[k] = funcWrap(v)
	}

	return newRqlTermFromPrevVal(t, "Min", p.Term_MIN, fieldOrFunctions, map[string]interface{}{})
}

// Finds the maximum of a sequence. If called with a field name, finds the element
// of that sequence with the largest value in that field. If called with a function,
// calls that function on every element of the sequence and returns the element
// which produced the largest value, ignoring any elements where the function
// returns null or produces a non-existence error.
func (t RqlTerm) Max(fieldOrFunctions ...interface{}) RqlTerm {
	for k, v := range fieldOrFunctions {
		fieldOrFunctions[k] = funcWrap(v)
	}

	return newRqlTermFromPrevVal(t, "Max", p.Term_MAX, fieldOrFunctions, map[string]interface{}{})
}
