package gorethink

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/dancannon/gorethink/encoding"
	p "github.com/dancannon/gorethink/ql2"
)

// Helper functions for constructing terms

// constructRootTerm is an alias for creating a new term.
func constructRootTerm(name string, termType p.Term_TermType, args []interface{}, optArgs map[string]interface{}) Term {
	return Term{
		name:     name,
		rootTerm: true,
		termType: termType,
		args:     convertTermList(args),
		optArgs:  convertTermObj(optArgs),
	}
}

// constructMethodTerm is an alias for creating a new term. Unlike constructRootTerm
// this function adds the previous expression in the tree to the argument list to
// create a method term.
func constructMethodTerm(prevVal Term, name string, termType p.Term_TermType, args []interface{}, optArgs map[string]interface{}) Term {
	args = append([]interface{}{prevVal}, args...)

	return Term{
		name:     name,
		rootTerm: false,
		termType: termType,
		args:     convertTermList(args),
		optArgs:  convertTermObj(optArgs),
	}
}

// Helper functions for creating internal RQL types

// makeArray takes a slice of terms and produces a single MAKE_ARRAY term
func makeArray(args termsList) Term {
	return Term{
		name:     "[...]",
		termType: p.Term_MAKE_ARRAY,
		args:     args,
	}
}

// makeObject takes a map of terms and produces a single MAKE_OBJECT term
func makeObject(args termsObj) Term {
	// First all evaluate all fields in the map
	temp := termsObj{}
	for k, v := range args {
		temp[k] = Expr(v)
	}

	return Term{
		name:     "{...}",
		termType: p.Term_MAKE_OBJ,
		optArgs:  temp,
	}
}

var nextVarId int64 = 0

func makeFunc(f interface{}) Term {
	value := reflect.ValueOf(f)
	valueType := value.Type()

	var argNums []interface{}
	var args []reflect.Value
	for i := 0; i < valueType.NumIn(); i++ {
		// Get a slice of the VARs to use as the function arguments
		args = append(args, reflect.ValueOf(constructRootTerm("var", p.Term_VAR, []interface{}{nextVarId}, map[string]interface{}{})))
		argNums = append(argNums, nextVarId)
		atomic.AddInt64(&nextVarId, 1)

		// make sure all input arguments are of type Term
		if valueType.In(i).String() != "gorethink.Term" {
			panic("Function argument is not of type Term")
		}
	}

	if valueType.NumOut() != 1 {
		panic("Function does not have a single return value")
	}

	body := value.Call(args)[0].Interface()
	argsArr := makeArray(convertTermList(argNums))

	return constructRootTerm("func", p.Term_FUNC, []interface{}{argsArr, body}, map[string]interface{}{})
}

func funcWrap(value interface{}) Term {
	val := Expr(value)

	if implVarScan(val) {
		return makeFunc(func(x Term) Term {
			return val
		})
	} else {
		return val
	}
}

func funcWrapArgs(args []interface{}) []interface{} {
	for i, arg := range args {
		args[i] = funcWrap(arg)
	}

	return args
}

// implVarScan recursivly checks a value to see if it contains an
// IMPLICIT_VAR term. If it does it returns true
func implVarScan(value Term) bool {
	if value.termType == p.Term_IMPLICIT_VAR {
		return true
	} else {
		for _, v := range value.args {
			if implVarScan(v) {
				return true
			}
		}

		for _, v := range value.optArgs {
			if implVarScan(v) {
				return true
			}
		}

		return false
	}
}

// Convert an opt args struct to a map.
func optArgsToMap(optArgs OptArgs) map[string]interface{} {
	data, err := encoding.Encode(optArgs)

	if err == nil && data != nil {
		if m, ok := data.(map[string]interface{}); ok {
			return m
		}
	}

	return map[string]interface{}{}
}

// Convert a list into a slice of terms
func convertTermList(l []interface{}) termsList {
	terms := termsList{}
	for _, v := range l {
		terms = append(terms, Expr(v))
	}

	return terms
}

// Convert a map into a map of terms
func convertTermObj(o map[string]interface{}) termsObj {
	terms := termsObj{}
	for k, v := range o {
		terms[k] = Expr(v)
	}

	return terms
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

// Pseudo-type helper functions

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
