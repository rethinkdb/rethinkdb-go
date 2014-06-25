package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Transform each element of the sequence by applying the given mapping function.
func (t RqlTerm) Map(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Map", p.Term_MAP, funcWrapArgs(args), map[string]interface{}{})
}

// Takes a sequence of objects and a list of fields. If any objects in the
// sequence don't have all of the specified fields, they're dropped from the
// sequence. The remaining objects have the specified fields plucked out.
// (This is identical to `HasFields` followed by `Pluck` on a sequence.)
func (t RqlTerm) WithFields(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "WithFields", p.Term_WITH_FIELDS, args, map[string]interface{}{})
}

// Flattens a sequence of arrays returned by the mapping function into a single
// sequence.
func (t RqlTerm) ConcatMap(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "ConcatMap", p.Term_CONCATMAP, funcWrapArgs(args), map[string]interface{}{})
}

type OrderByOpts struct {
	Index interface{} `gorethink:"index,omitempty"`
}

func (o *OrderByOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Sort the sequence by document values of the given key(s).
// To specify the index to use for ordering us a last argument in the following form:
//
//	map[string]interface{}{"index": "index-name"}
//
// OrderBy defaults to ascending ordering. To explicitly specify the ordering,
// wrap the attribute with either Asc or Desc.
//
//	query.OrderBy("name")
//	query.OrderBy(Asc("name"))
//	query.OrderBy(Desc("name"))
func (t RqlTerm) OrderBy(args ...interface{}) RqlTerm {
	var opts = map[string]interface{}{}

	// Look for options map
	if len(args) > 0 {
		if possibleOpts, ok := args[len(args)-1].(OrderByOpts); ok {
			opts = possibleOpts.toMap()
			args = args[:len(args)-1]
		}
	}

	for k, arg := range args {
		if t, ok := arg.(RqlTerm); !(ok && (t.termType == p.Term_DESC || t.termType == p.Term_ASC)) {
			args[k] = funcWrap(arg)
		}
	}

	return newRqlTermFromPrevVal(t, "OrderBy", p.Term_ORDERBY, args, opts)
}

func Desc(args ...interface{}) RqlTerm {
	return newRqlTerm("Desc", p.Term_DESC, funcWrapArgs(args), map[string]interface{}{})
}

func Asc(args ...interface{}) RqlTerm {
	return newRqlTerm("Asc", p.Term_ASC, funcWrapArgs(args), map[string]interface{}{})
}

// Skip a number of elements from the head of the sequence.
func (t RqlTerm) Skip(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Skip", p.Term_SKIP, args, map[string]interface{}{})
}

// End the sequence after the given number of elements.
func (t RqlTerm) Limit(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Limit", p.Term_LIMIT, args, map[string]interface{}{})
}

type SliceOpts struct {
	LeftBound  interface{} `gorethink:"left_bound,omitempty"`
	RightBound interface{} `gorethink:"right_bound,omitempty"`
}

func (o *SliceOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// TODO: Add optional arguments
// Trim the sequence to within the bounds provided.
func (t RqlTerm) Slice(args ...interface{}) RqlTerm {
	var opts = map[string]interface{}{}

	// Look for options map
	if len(args) > 0 {
		if possibleOpts, ok := args[len(args)-1].(SliceOpts); ok {
			opts = possibleOpts.toMap()
			args = args[:len(args)-1]
		}
	}

	return newRqlTermFromPrevVal(t, "Slice", p.Term_SLICE, args, opts)
}

// Get the nth element of a sequence.
func (t RqlTerm) Nth(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Nth", p.Term_NTH, args, map[string]interface{}{})
}

// Get the indexes of an element in a sequence. If the argument is a predicate,
// get the indexes of all elements matching it.
func (t RqlTerm) IndexesOf(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexesOf", p.Term_INDEXES_OF, funcWrapArgs(args), map[string]interface{}{})
}

// Test if a sequence is empty.
func (t RqlTerm) IsEmpty(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IsEmpty", p.Term_IS_EMPTY, args, map[string]interface{}{})
}

// Concatenate two sequences.
func (t RqlTerm) Union(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Union", p.Term_UNION, args, map[string]interface{}{})
}

// Select a given number of elements from a sequence with uniform random
// distribution. Selection is done without replacement.
func (t RqlTerm) Sample(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Sample", p.Term_SAMPLE, args, map[string]interface{}{})
}
