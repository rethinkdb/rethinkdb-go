package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
	"reflect"
)

// Helper functions for creating internal RQL types

// makeArray takes a slice of terms and produces a single MAKE_ARRAY term
func makeArray(args termsList) RqlTerm {
	return RqlTerm{
		name:     "[...]",
		termType: p.Term_MAKE_ARRAY,
		args:     args,
	}
}

// makeObject takes a map of terms and produces a single MAKE_OBJECT term
func makeObject(args termsObj) RqlTerm {
	// First all evaluate all fields in the map
	temp := termsObj{}
	for k, v := range args {
		temp[k] = Expr(v)
	}

	return RqlTerm{
		name:     "{...}",
		termType: p.Term_MAKE_OBJ,
		optArgs:  temp,
	}
}

var nextVarId int64 = 0

func makeFunc(f interface{}) RqlTerm {
	value := reflect.ValueOf(f)
	valueType := value.Type()

	var argNums []interface{}
	var args []reflect.Value
	for i := 0; i < valueType.NumIn(); i++ {
		// Get a slice of the VARs to use as the function arguments
		args = append(args, reflect.ValueOf(newRqlTerm("var", p.Term_VAR, []interface{}{nextVarId}, map[string]interface{}{})))
		argNums = append(argNums, nextVarId)
		nextVarId++

		// make sure all input arguments are of type RqlTerm
		if valueType.In(i).String() != "gorethink.RqlTerm" {
			panic("Function argument is not of type RqlTerm")
		}
	}

	if valueType.NumOut() != 1 {
		panic("Function does not have a single return value")
	}

	body := value.Call(args)[0].Interface()
	argsArr := makeArray(listToTermsList(argNums))

	return newRqlTerm("func", p.Term_FUNC, []interface{}{argsArr, body}, map[string]interface{}{})
}

func funcWrap(value interface{}) RqlTerm {
	val := Expr(value)

	if implVarScan(val) {
		return makeFunc(func(x RqlTerm) RqlTerm {
			return val
		})
	} else {
		return val
	}
}

// implVarScan recursivly checks a value to see if it contains an
// IMPLICIT_VAR term. If it does it returns true
func implVarScan(value RqlTerm) bool {
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
