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
	compose() string
	build() *p.Term
}

type RqlOp interface {
	RqlTerm
	Add(args ...interface{}) RqlOp
	Sub(args ...interface{}) RqlOp
	Mul(args ...interface{}) RqlOp
	Div(args ...interface{}) RqlOp
	Mod(args ...interface{}) RqlOp
	And(args ...interface{}) RqlOp
	Or(args ...interface{}) RqlOp
	Eq(args ...interface{}) RqlOp
	Ne(args ...interface{}) RqlOp
	Gt(args ...interface{}) RqlOp
	Ge(args ...interface{}) RqlOp
	Lt(args ...interface{}) RqlOp
	Not() RqlOp
}

type RqlVal struct {
	name     string
	termType *p.Term_TermType
	args     []RqlTerm
	optArgs  map[string]RqlTerm
}

type RqlDatum struct {
	RqlVal
	data interface{}
}

type RqlObject struct {
	RqlVal
}

type RqlArray struct {
	RqlVal
}

func (t RqlVal) build() *p.Term {
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

func (t RqlVal) compose() string {
	if t.name != "" {
		return fmt.Sprintf("r.%s(%s)", t.name, strings.Join(allArgsToStringSlice(t.args, t.optArgs), ", "))
	} else {
		return ""
	}
}
func (t RqlDatum) build() *p.Term {
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
}

func (t RqlDatum) compose() string {
	return fmt.Sprintf("%v", t.data)
}

func (t RqlArray) compose() string {
	return fmt.Sprintf("[%s]", strings.Join(argsToStringSlice(t.args), ", "))
}

func (t RqlObject) compose() string {
	return fmt.Sprintf("{%s}", strings.Join(optArgsToStringSlice(t.optArgs), ", "))
}

func newRqlVal(name string, termType p.Term_TermType, args List, optArgs Obj) RqlVal {
	return RqlVal{
		name:     name,
		termType: termType.Enum(),
		args:     listToTermsList(args),
		optArgs:  objToTermsObj(optArgs),
	}
}

func newRqlValFromPrevVal(prevVal RqlVal, name string, termType p.Term_TermType, args List, optArgs Obj) RqlVal {
	args = append(List{prevVal}, args...)

	return RqlVal{
		name:     name,
		termType: termType.Enum(),
		args:     listToTermsList(args),
		optArgs:  objToTermsObj(optArgs),
	}
}
