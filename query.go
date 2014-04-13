package gorethink

import (
	"fmt"
	"strconv"
	"strings"

	"code.google.com/p/goprotobuf/proto"
	p "github.com/dancannon/gorethink/ql2"
)

type OptArgs interface {
	toMap() map[string]interface{}
}
type termsList []RqlTerm
type termsObj map[string]RqlTerm
type RqlTerm struct {
	name     string
	rootTerm bool
	termType p.Term_TermType
	data     interface{}
	args     []RqlTerm
	optArgs  map[string]RqlTerm
}

// build takes the query tree and turns it into a protobuf term tree.
func (t RqlTerm) build() *p.Term {
	switch t.termType {
	case p.Term_DATUM:
		datum, err := constructDatum(t)
		if err != nil {
			panic(err)
		}
		return datum
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

func (t RqlTerm) compose(args []string, optArgs map[string]string) string {
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
		if t.rootTerm {
			return fmt.Sprintf("r.%s(%s)", t.name, strings.Join(allArgsToStringSlice(t.args, t.optArgs), ", "))
		} else {
			return fmt.Sprintf("%s.%s(%s)", t.args[0].String(), t.name, strings.Join(allArgsToStringSlice(t.args[1:], t.optArgs), ", "))
		}
	}
}

// String returns a string representation of the query tree
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
		if t.rootTerm {
			return fmt.Sprintf("r.%s(%s)", t.name, strings.Join(allArgsToStringSlice(t.args, t.optArgs), ", "))
		} else {
			return fmt.Sprintf("%s.%s(%s)", t.args[0].String(), t.name, strings.Join(allArgsToStringSlice(t.args[1:], t.optArgs), ", "))
		}
	}
}

type WriteResponse struct {
	Errors        int
	Created       int
	Inserted      int
	Updated       int
	Unchanged     int
	Replaced      int
	Deleted       int
	GeneratedKeys []string    `gorethink:"generated_keys"`
	FirstError    string      `gorethink:"first_error"` // populated if Errors > 0
	NewValue      interface{} `gorethink:"new_val"`
	OldValue      interface{} `gorethink:"old_val"`
}

type RunOpts struct {
	Db          interface{} `gorethink:"db,omitempty"`
	Profile     interface{} `gorethink:"profile,omitempty"`
	UseOutdated interface{} `gorethink:"use_outdated,omitempty"`
	NoReply     interface{} `gorethink:"noreply,omitempty"`
	TimeFormat  interface{} `gorethink:"time_format,omitempty"`
}

func (o *RunOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Run runs a query using the given connection.
//
//	rows, err := query.Run(sess)
//	if err != nil {
//		// error
//	}
//	for rows.Next() {
//		doc := MyDocumentType{}
//		err := r.Scan(&doc)
//		    // Do something with row
//	}
func (t RqlTerm) Run(s *Session, optArgs ...RunOpts) (*ResultRows, error) {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return s.startQuery(t, opts)
}

// Run runs a query using the given connection but unlike Run returns ResultRow.
// This function should be used if your query only returns a single row.
//
//	row, err := query.RunRow(sess, r.RunOpts{
//		UseOutdated: true,
//	})
//	if err != nil {
//		// error
//	}
//	if row.IsNil() {
//		// nothing was found
//	}
//	err = row.Scan(&doc)
func (t RqlTerm) RunRow(s *Session, optArgs ...RunOpts) (*ResultRow, error) {
	rows, err := t.Run(s, optArgs...)
	if err == nil {
		rows.Next()
	}
	return &ResultRow{rows: rows, err: err}, err
}

// RunWrite runs a query using the given connection but unlike Run automatically
// scans the result into a variable of type WriteResponse. This function should be used
// if you are running a write query (such as Insert,  Update, TableCreate, etc...)
//
// Optional arguments :
// "db", "use_outdated" (defaults to false), "noreply" (defaults to false) and "time_format".
//
//	res, err := r.Db("database").Table("table").Insert(doc).RunWrite(sess, r.RunOpts{
//		NoReply: true,
//	})
func (t RqlTerm) RunWrite(s *Session, optArgs ...RunOpts) (WriteResponse, error) {
	var response WriteResponse
	row, err := t.RunRow(s, optArgs...)
	if err == nil {
		err = row.Scan(&response)
	}
	return response, err
}

// Exec runs the query but does not return the result (It also automatically sets
// the noreply option).
func (t RqlTerm) Exec(s *Session, optArgs ...RunOpts) error {
	// Ensure that noreply is set to true
	if len(optArgs) >= 1 {
		optArgs[0].NoReply = true
	} else {
		optArgs = append(optArgs, RunOpts{
			NoReply: true,
		})
	}

	_, err := t.Run(s, optArgs...)
	return err
}
