package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	p "github.com/dancannon/gorethink/ql2"
	"strconv"
	"strings"
	"time"
)

// Helper functions for constructing terms

// newRqlTerm is an alias for creating a new RqlTermue.
func newRqlTerm(name string, termType p.Term_TermType, args List, optArgs Obj) RqlTerm {
	return RqlTerm{
		name:     name,
		termType: termType,
		args:     listToTermsList(args),
		optArgs:  objToTermsObj(optArgs),
	}
}

// newRqlTermFromPrevVal is an alias for creating a new RqlTermue. Unlike newRqlTerm
// this function adds the previous expression in the tree to the argument list.
// It is used when evalutating an expression like
//
// `r.Expr(1).Add(2).Mul(3)`
func newRqlTermFromPrevVal(prevVal RqlTerm, name string, termType p.Term_TermType, args List, optArgs Obj) RqlTerm {
	args = append(List{prevVal}, args...)

	return RqlTerm{
		name:     name,
		termType: termType,
		args:     listToTermsList(args),
		optArgs:  objToTermsObj(optArgs),
	}
}

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

func enforceArgLength(min, max int, args []interface{}) {
	if max == -1 {
		max = len(args)
	}

	if len(args) < min || len(args) > max {
		panic("Function has incorrect number of arguments")
	}
}

func mergeArgs(args ...interface{}) []interface{} {
	newArgs := []interface{}{}

	for _, arg := range args {
		switch v := arg.(type) {
		case []interface{}:
			newArgs = append(newArgs, v...)
		default:
			newArgs = append(newArgs, v)
		}
	}

	return newArgs
}

func reqlTimeToNativeTime(timestamp int64, timezone string) (time.Time, error) {
	t := time.Unix(timestamp, 0)

	// Caclulate the timezone
	if timezone != "" {
		hours, err := strconv.Atoi(timezone[1:3])
		if err != nil {
			return time.Time{}, err
		}
		minutes, err := strconv.Atoi(timezone[4:6])
		if err != nil {
			return time.Time{}, err
		}
		tzOffset := ((hours * 60) + minutes) * 60
		if timezone[:1] == "-" {
			tzOffset = 0 - tzOffset
		}

		t = t.In(time.FixedZone(timezone, tzOffset))
	}

	return t, nil
}

// Helper functions for debugging

func allArgsToStringSlice(args termsList, optArgs termsObj) []string {
	allArgs := []string{}

	for _, v := range args {
		allArgs = append(allArgs, v.String())
	}
	for k, v := range optArgs {
		allArgs = append(allArgs, k+"="+v.String())
	}

	return allArgs
}

func argsToStringSlice(args termsList) []string {
	allArgs := []string{}

	for _, v := range args {
		allArgs = append(allArgs, v.String())
	}

	return allArgs
}

func optArgsToStringSlice(optArgs termsObj) []string {
	allArgs := []string{}

	for k, v := range optArgs {
		allArgs = append(allArgs, k+"="+v.String())
	}

	return allArgs
}

func prefixLines(s string, prefix string) (result string) {
	for _, line := range strings.Split(s, "\n") {
		result += prefix + line + "\n"
	}
	return
}

func protobufToString(p proto.Message, indentLevel int) string {
	return prefixLines(proto.MarshalTextString(p), strings.Repeat("    ", indentLevel))
}

func optArgsToMap(keys []string, args []interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(args)/2)
	for i := 0; i < len(args); i++ {
		// Check that the key is of type string
		if key, ok := args[i].(string); ok {
			i++

			// Check if key is allowed
			allowed := false
			for _, k := range keys {
				if k == key {
					allowed = true
				}
			}
			if !allowed {
				break
			}

			result[key] = args[i]
		} else {
			panic("gorethink: OptArg key is not of type string")
		}

	}
	return result
}
