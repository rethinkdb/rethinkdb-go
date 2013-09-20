package gorethink

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	p "github.com/dancannon/gorethink/ql2"
	"strconv"
	"strings"
)

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
		return "r.Row"
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

type WriteResponse struct {
	Inserted      int
	Errors        int
	Updated       int
	Unchanged     int
	Replaced      int
	Deleted       int
	GeneratedKeys []string    `gorethink:"generated_keys"`
	FirstError    string      `gorethink:"first_error"` // populated if Errors > 0
	NewValue      interface{} `gorethink:"new_val"`
	OldValue      interface{} `gorethink:"old_val"`
}

// Run runs a query using the given connection. Run takes the optional arguments
// "use_outdated", "noreply" and "time_format".
func (t RqlTerm) Run(c *Connection, args ...interface{}) (*ResultRows, error) {
	argm := optArgsToMap([]string{"db", "use_outdated", "noreply", "time_format"}, args)
	return c.startQuery(t, argm)
}

// Run runs a query using the given connection but unlike Run returns ResultRow.
// This function should be used if your query only returns a single row.
// RunRow takes the optional arguments "db", "use_outdated", "noreply" and
// "time_format".
func (t RqlTerm) RunRow(c *Connection, args ...interface{}) *ResultRow {
	rows, err := t.Run(c, args...)
	return &ResultRow{rows: rows, err: err}
}

// RunWrite runs a query using the given connection but unlike Run automatically
// scans the result into a variable of type WriteResponse. This function should be used
// if you are running a write query (such as Insert,  Update, TableCreate, etc...)
// RunWrite takes the optional arguments "db", "use_outdated","noreply" and "time_format".
func (t RqlTerm) RunWrite(c *Connection, args ...interface{}) (WriteResponse, error) {
	var response WriteResponse
	row := t.RunRow(c, args...)
	err := row.Scan(&response)
	return response, err
}

// Exec runs the query but does not return the result (It also automatically sets
// the noreply option). RunRow takes the optional arguments "db", "use_outdated"
// and "time_format".
func (t RqlTerm) Exec(c *Connection, args ...interface{}) error {
	// Ensure that noreply is set to true
	args = append(args, "noreply")
	args = append(args, true)

	_, err := t.Run(c, args...)
	return err
}
