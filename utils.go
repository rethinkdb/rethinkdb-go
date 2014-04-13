package gorethink

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/dancannon/gorethink/encoding"
	p "github.com/dancannon/gorethink/ql2"
)

// Helper functions for constructing terms

// newRqlTerm is an alias for creating a new RqlTermue.
func newRqlTerm(name string, termType p.Term_TermType, args []interface{}, optArgs map[string]interface{}) RqlTerm {
	return RqlTerm{
		name:     name,
		rootTerm: true,
		termType: termType,
		args:     listToTermsList(args),
		optArgs:  objToTermsObj(optArgs),
	}
}

// newRqlTermFromPrevVal is an alias for creating a new RqlTerm. Unlike newRqlTerm
// this function adds the previous expression in the tree to the argument list.
// It is used when evalutating an expression like
//
// `r.Expr(1).Add(2).Mul(3)`
func newRqlTermFromPrevVal(prevVal RqlTerm, name string, termType p.Term_TermType, args []interface{}, optArgs map[string]interface{}) RqlTerm {
	args = append([]interface{}{prevVal}, args...)

	return RqlTerm{
		name:     name,
		rootTerm: false,
		termType: termType,
		args:     listToTermsList(args),
		optArgs:  objToTermsObj(optArgs),
	}
}

// Convert a list into a slice of terms
func listToTermsList(l []interface{}) termsList {
	terms := termsList{}
	for _, v := range l {
		terms = append(terms, Expr(v))
	}

	return terms
}

// Convert a map into a map of terms
func objToTermsObj(o map[string]interface{}) termsObj {
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

func reqlTimeToNativeTime(timestamp float64, timezone string) (time.Time, error) {
	sec, ms := math.Modf(timestamp)

	t := time.Unix(int64(sec), int64(ms*1000*1000*1000))

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

func reqlGroupedDataToObj(obj map[string]interface{}) (interface{}, error) {
	if data, ok := obj["data"]; ok {
		ret := []interface{}{}
		for _, v := range data.([]interface{}) {
			v := v.([]interface{})
			ret = append(ret, map[string]interface{}{
				"group":     v[0],
				"reduction": v[1],
			})
		}
		return ret, nil
	} else {
		return nil, fmt.Errorf("pseudo-type GROUPED_DATA object %v does not have the expected field \"data\"", obj)
	}
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

func optArgsToMap(optArgs OptArgs) map[string]interface{} {
	data, err := encoding.Encode(optArgs)

	if err == nil && data != nil {
		if m, ok := data.(map[string]interface{}); ok {
			return m
		}
	}

	return map[string]interface{}{}
}
