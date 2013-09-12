package rethinkgo

import (
	"fmt"
	p "github.com/christopherhesse/rethinkgo/ql2"
	"strings"
)

// Let user create queries as RQL Exp trees, any errors are deferred
// until the query is run, so most all functions take interface{} types.
// interface{} is effectively a void* type that we look at later to determine
// the underlying type and perform any conversions.

// Obj is a shorter name for a mapping from strings to arbitrary objects
type Obj map[string]interface{}

// List is a shorter name for an array of arbitrary objects
type List []interface{}

type termsList []RqlTerm
type termsObj map[string]RqlTerm

type RqlTerm interface {
	String() string
	build() *p.Term
}

type RqlVal struct {
	name     string
	termType p.Term_TermType
	data     interface{}
	args     []RqlTerm
	optArgs  map[string]RqlTerm
}

// build takes the query tree and turns it into a protobuf term tree.
func (t RqlVal) build() *p.Term {
	switch t.termType {
	case p.Term_DATUM:
		if t.data == nil {
			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_NULL.Enum(),
				},
			}
		} else {
			switch val := t.data.(type) {
			case bool:
				return &p.Term{
					Type: p.Term_DATUM.Enum(),
					Datum: &p.Datum{
						Type:  p.Datum_R_BOOL.Enum(),
						RBool: &val,
					},
				}
			case float64:
				return &p.Term{
					Type: p.Term_DATUM.Enum(),
					Datum: &p.Datum{
						Type: p.Datum_R_NUM.Enum(),
						RNum: &val,
					},
				}
			case string:
				return &p.Term{
					Type: p.Term_DATUM.Enum(),
					Datum: &p.Datum{
						Type: p.Datum_R_STR.Enum(),
						RStr: &val,
					},
				}
			default:
				panic(fmt.Sprintf("Cannot convert type '%T' to Datum", val))
			}
		}
	default:
		args := []*p.Term{}
		optArgs := []*p.Term_AssocPair{}
		term := &p.Term{
			Type: t.termType.Enum(),
		}

		for _, v := range t.args {
			args = append(args, v.build())
		}

		for k, v := range t.optArgs {
			optArgs = append(optArgs, &p.Term_AssocPair{
				Key: &k,
				Val: v.build(),
			})
		}

		term.Args = args
		term.Optargs = optArgs

		return term
	}
}

// compose returns a string representation of the query tree
func (t RqlVal) String() string {
	switch t.termType {
	case p.Term_MAKE_ARRAY:
		return fmt.Sprintf("[%s]", strings.Join(argsToStringSlice(t.args), ", "))
	case p.Term_MAKE_OBJ:
		return fmt.Sprintf("{%s}", strings.Join(optArgsToStringSlice(t.optArgs), ", "))
	case p.Term_DATUM:
		return fmt.Sprintf("%v", t.data)
	default:
		if t.name != "" {
			return fmt.Sprintf("r.%s(%s)", t.name, strings.Join(allArgsToStringSlice(t.args, t.optArgs), ", "))
		} else {
			return fmt.Sprintf("(%s)", t.name, strings.Join(allArgsToStringSlice(t.args, t.optArgs), ", "))
		}
	}
}

// newRqlVal is an alias for creating a new RqlValue.
func newRqlVal(name string, termType p.Term_TermType, args List, optArgs Obj) RqlVal {
	return RqlVal{
		name:     name,
		termType: termType,
		args:     listToTermsList(args),
		optArgs:  objToTermsObj(optArgs),
	}
}

// newRqlValFromPrevVal is an alias for creating a new RqlValue. Unlike newRqlVal
// this function adds the previous expression in the tree to the argument list.
// It is used when evalutating an expression like
//
// `r.Expr(1).Add(2).Mul(3)`
func newRqlValFromPrevVal(prevVal RqlVal, name string, termType p.Term_TermType, args List, optArgs Obj) RqlVal {
	args = append(List{prevVal}, args...)

	return RqlVal{
		name:     name,
		termType: termType,
		args:     listToTermsList(args),
		optArgs:  objToTermsObj(optArgs),
	}
}
