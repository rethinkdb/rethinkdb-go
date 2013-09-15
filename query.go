package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	p "github.com/dancannon/gorethink/ql2"
	"strconv"
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

type RqlTerm struct {
	name     string
	termType p.Term_TermType
	data     interface{}
	args     []RqlTerm
	optArgs  map[string]RqlTerm
}

// build takes the query tree and turns it into a protobuf term tree.
func (t RqlTerm) build() *p.Term {
	switch t.termType {
	case p.Term_DATUM:
		return constructDatum(t)
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
				Key: proto.String(k),
				Val: v.build(),
			})
		}

		term.Args = args
		term.Optargs = optArgs

		return term
	}
}

// compose returns a string representation of the query tree
func (t RqlTerm) String() string {
	switch t.termType {
	case p.Term_MAKE_ARRAY:
		return fmt.Sprintf("[%s]", strings.Join(argsToStringSlice(t.args), ", "))
	case p.Term_MAKE_OBJ:
		return fmt.Sprintf("{%s}", strings.Join(optArgsToStringSlice(t.optArgs), ", "))
	case p.Term_FUNC:
		// Get string representation of each argument
		args := []string{}
		for _, v := range t.args[0].args {
			args = append(args, fmt.Sprintf("var_%d", v.data))
		}

		return fmt.Sprintf("func(%s r.RqlTerm) r.RqlTerm { return %s }",
			strings.Join(args, ", "),
			t.args[1].String(),
		)
	case p.Term_VAR:
		return fmt.Sprintf("var_%s", t.args[0])
	case p.Term_IMPLICIT_VAR:
		return "r.Doc()"
	case p.Term_DATUM:
		switch v := t.data.(type) {
		case string:
			return strconv.Quote(v)
		default:
			return fmt.Sprintf("%v", v)
		}

	default:
		if t.name != "" {
			return fmt.Sprintf("r.%s(%s)", t.name, strings.Join(allArgsToStringSlice(t.args, t.optArgs), ", "))
		} else {
			return fmt.Sprintf("(%s)", strings.Join(allArgsToStringSlice(t.args, t.optArgs), ", "))
		}
	}
}

func (t RqlTerm) Run(c *Connection) (*Rows, error) {
	return c.startQuery(t)
}

func (t RqlTerm) RunRow(c *Connection) *Row {
	rows, err := t.Run(c)
	return &Row{rows: rows, err: err}
}

// Run a write query
func (t RqlTerm) RunWrite(c *Connection) (*Rows, error) {
	rows, err := t.Run(c)
	return rows, err
}

func (t RqlTerm) Exec(c *Connection) error {
	_, err := t.Run(c)
	return err
}
