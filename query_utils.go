package rethinkgo

import (
	p "github.com/christopherhesse/rethinkgo/ql2"
)

// Helper functions for creating internal RQL types

// makeArray takes a slice of terms and produces a single MAKE_ARRAY term
func makeArray(args termsList) RqlOp {

	return RqlVal{
		name:     "[...]",
		termType: p.Term_MAKE_ARRAY.Enum(),
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
		termType: p.Term_MAKE_ARRAY.Enum(),
		optArgs:  temp,
	}
}

// Helper functions for constructing terms

// Convert a list into a slice of terms
func listToTermsList(l List) termsList {
	terms := termsList{}
	for _, v := range l {
		terms = append(terms, Expr(v))
	}

	return terms
}

// Convert a map into a map of terms
func objToTermsObj(o Obj) termsObj {
	terms := termsObj{}
	for k, v := range o {
		terms[k] = Expr(v)
	}

	return terms
}

// Helper functions for turning an expression tree into a string

func allArgsToStringSlice(args termsList, optArgs termsObj) []string {
	allArgs := []string{}

	for _, v := range args {
		allArgs = append(allArgs, v.compose())
	}
	for k, v := range optArgs {
		allArgs = append(allArgs, k+"="+v.compose())
	}

	return allArgs
}

func argsToStringSlice(args termsList) []string {
	allArgs := []string{}

	for _, v := range args {
		allArgs = append(allArgs, v.compose())
	}

	return allArgs
}

func optArgsToStringSlice(optArgs termsObj) []string {
	allArgs := []string{}

	for k, v := range optArgs {
		allArgs = append(allArgs, k+"="+v.compose())
	}

	return allArgs
}
