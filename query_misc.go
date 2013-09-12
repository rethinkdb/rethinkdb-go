package rethinkgo

import (
	p "github.com/christopherhesse/rethinkgo/ql2"
)

// Helper functions for creating internal RQL types

// makeArray takes a slice of terms and produces a single MAKE_ARRAY term
func makeArray(args termsList) RqlOp {

	return RqlVal{
		name:     "[...]",
		termType: p.Term_MAKE_ARRAY,
		args:     args,
	}
}

// makeObject takes a map of terms and produces a single MAKE_OBJECT term
func makeObject(args termsObj) RqlOp {
	// First all evaluate all fields in the map
	temp := termsObj{}
	for k, v := range args {
		temp[k] = Expr(v)
	}

	return RqlVal{
		name:     "{...}",
		termType: p.Term_MAKE_ARRAY,
		optArgs:  temp,
	}
}
