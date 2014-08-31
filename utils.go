package gorethink

import (
	"reflect"
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

	if implVarScan(val) && val.termType != p.Term_ARGS {
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
	data, err := encode(optArgs)

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

func encode(v interface{}) (interface{}, error) {
	encoding.RegisterEncodeHook(func(v reflect.Value) (success bool, ret reflect.Value, err error) {
		if v.Type() == reflect.TypeOf(time.Time{}) {
			return true, v, nil
		} else if v.Type() == reflect.TypeOf(Term{}) {
			return true, v, nil
		} else {
			return false, v, nil
		}
	})

	return encoding.Encode(v)
}
