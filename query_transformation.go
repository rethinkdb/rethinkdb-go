package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) Map(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Map", p.Term_MAP, []interface{}{funcWrap(f)}, map[string]interface{}{})
}

func (t RqlTerm) WithFields(selectors ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "WithFields", p.Term_WITH_FIELDS, selectors, map[string]interface{}{})
}

func (t RqlTerm) ConcatMap(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "ConcatMap", p.Term_CONCATMAP, []interface{}{funcWrap(f)}, map[string]interface{}{})
}

func (t RqlTerm) OrderBy(args ...interface{}) RqlTerm {
	for k, arg := range args {
		if t, ok := arg.(RqlTerm); !(ok && (t.termType == p.Term_DESC || t.termType == p.Term_ASC)) {
			args[k] = funcWrap(arg)
		}
	}

	return newRqlTermFromPrevVal(t, "OrderBy", p.Term_ORDERBY, args, map[string]interface{}{})
}

func Desc(arg interface{}) RqlTerm {
	return newRqlTerm("Desc", p.Term_DESC, []interface{}{funcWrap(arg)}, map[string]interface{}{})
}

func Asc(arg interface{}) RqlTerm {
	return newRqlTerm("Asc", p.Term_ASC, []interface{}{funcWrap(arg)}, map[string]interface{}{})
}

func (t RqlTerm) Skip(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Skip", p.Term_SKIP, []interface{}{n}, map[string]interface{}{})
}

func (t RqlTerm) Limit(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Limit", p.Term_LIMIT, []interface{}{n}, map[string]interface{}{})
}

func (t RqlTerm) Slice(lower, upper interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Slice", p.Term_SLICE, []interface{}{lower, upper}, map[string]interface{}{})
}

func (t RqlTerm) Nth(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Nth", p.Term_NTH, []interface{}{n}, map[string]interface{}{})
}

func (t RqlTerm) IndexesOf(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexesOf", p.Term_INDEXES_OF, []interface{}{funcWrap(arg)}, map[string]interface{}{})
}

func (t RqlTerm) IsEmpty() RqlTerm {
	return newRqlTermFromPrevVal(t, "IsEmpty", p.Term_IS_EMPTY, []interface{}{}, map[string]interface{}{})
}

func (t RqlTerm) Union(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Union", p.Term_UNION, []interface{}{arg}, map[string]interface{}{})
}

func (t RqlTerm) Sample(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Sample", p.Term_SAMPLE, []interface{}{n}, map[string]interface{}{})
}
