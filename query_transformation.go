package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

func (t RqlTerm) Map(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Map", p.Term_MAP, List{funcWrap(f)}, Obj{})
}

func (t RqlTerm) WithFields(selectors ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "WithFields", p.Term_WITH_FIELDS, selectors, Obj{})
}

func (t RqlTerm) ConcatMap(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "ConcatMap", p.Term_CONCATMAP, List{funcWrap(f)}, Obj{})
}

func (t RqlTerm) OrderBy(args ...interface{}) RqlTerm {
	for k, arg := range args {
		if t, ok := arg.(RqlTerm); !(ok && (t.termType == p.Term_DESC || t.termType == p.Term_ASC)) {
			args[k] = funcWrap(arg)
		}
	}

	return newRqlTermFromPrevVal(t, "OrderBy", p.Term_ORDERBY, args, Obj{})
}

func Desc(arg interface{}) RqlTerm {
	return newRqlTerm("Desc", p.Term_DESC, List{funcWrap(arg)}, Obj{})
}

func Asc(arg interface{}) RqlTerm {
	return newRqlTerm("Asc", p.Term_ASC, List{funcWrap(arg)}, Obj{})
}

func (t RqlTerm) Skip(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Skip", p.Term_SKIP, List{n}, Obj{})
}

func (t RqlTerm) Limit(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Limit", p.Term_LIMIT, List{n}, Obj{})
}

func (t RqlTerm) Slice(lower, upper interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Slice", p.Term_SLICE, List{lower, upper}, Obj{})
}

func (t RqlTerm) Nth(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Nth", p.Term_NTH, List{n}, Obj{})
}

func (t RqlTerm) IndexesOf(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexesOf", p.Term_INDEXES_OF, List{funcWrap(arg)}, Obj{})
}

func (t RqlTerm) IsEmpty() RqlTerm {
	return newRqlTermFromPrevVal(t, "IsEmpty", p.Term_IS_EMPTY, List{}, Obj{})
}

func (t RqlTerm) Union(arg interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Union", p.Term_UNION, List{arg}, Obj{})
}

func (t RqlTerm) Sample(n interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Sample", p.Term_SAMPLE, List{n}, Obj{})
}
