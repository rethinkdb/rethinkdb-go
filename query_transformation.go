package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Transform each element of the sequence by applying the given mapping function.
func (t RqlTerm) Map(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Map", p.Term_MAP, []interface{}{funcWrap(f)}, map[string]interface{}{})
}

// Takes a sequence of objects and a list of fields. If any objects in the
// sequence don't have all of the specified fields, they're dropped from the
// sequence. The remaining objects have the specified fields plucked out.
// (This is identical to `HasFields` followed by `Pluck` on a sequence.)
func (t RqlTerm) WithFields(selectors ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "WithFields", p.Term_WITH_FIELDS, selectors, map[string]interface{}{})
}

// Flattens a sequence of arrays returned by the mapping function into a single
// sequence.
func (t RqlTerm) ConcatMap(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "ConcatMap", p.Term_CONCATMAP, []interface{}{funcWrap(f)}, map[string]interface{}{})
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
		if possibleOpts, ok := args[len(args)-1].(map[string]interface{}); ok {
			opts = possibleOpts
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

func Desc(arg interface{}) RqlTerm {
	return newRqlTerm("Desc", p.Term_DESC, []interface{}{funcWrap(arg)}, map[string]interface{}{})
}

func Asc(arg interface{}) RqlTerm {
	return newRqlTerm("Asc", p.Term_ASC, []interface{}{funcWrap(arg)}, map[string]interface{}{})
}

// Skip a number of elements from the head of the sequence.
func (t RqlTerm) Skip(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Skip", p.Term_SKIP, []interface{}{n}, map[string]interface{}{})
}

// End the sequence after the given number of elements.
func (t RqlTerm) Limit(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Limit", p.Term_LIMIT, []interface{}{n}, map[string]interface{}{})
}

// Trim the sequence to within the bounds provided.
func (t RqlTerm) Slice(lower, upper interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Slice", p.Term_SLICE, []interface{}{lower, upper}, map[string]interface{}{})
}

// Get the nth element of a sequence.
func (t RqlTerm) Nth(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Nth", p.Term_NTH, []interface{}{n}, map[string]interface{}{})
}

// Get the indexes of an element in a sequence. If the argument is a predicate,
// get the indexes of all elements matching it.
func (t RqlTerm) IndexesOf(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexesOf", p.Term_INDEXES_OF, []interface{}{funcWrap(arg)}, map[string]interface{}{})
}

// Test if a sequence is empty.
func (t RqlTerm) IsEmpty() RqlTerm {
	return newRqlTermFromPrevVal(t, "IsEmpty", p.Term_IS_EMPTY, []interface{}{}, map[string]interface{}{})
}

// Concatenate two sequences.
func (t RqlTerm) Union(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Union", p.Term_UNION, []interface{}{arg}, map[string]interface{}{})
}

// Select a given number of elements from a sequence with uniform random
// distribution. Selection is done without replacement.
func (t RqlTerm) Sample(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Sample", p.Term_SAMPLE, []interface{}{n}, map[string]interface{}{})
}
