package gorethink

import (
	"reflect"
	"time"

	"github.com/dancannon/gorethink/encoding"
	p "github.com/dancannon/gorethink/ql2"
)

// Expr converts any value to an expression.  Internally it uses the `json`
// module to convert any literals, so any type annotations or methods understood
// by that module can be used. If the value cannot be converted, an error is
// returned at query .Run(session) time.
//
// If you want to call expression methods on an object that is not yet an
// expression, this is the function you want.
func Expr(value interface{}) RqlTerm {
	return expr(value, 20)
}

func expr(value interface{}, depth int) RqlTerm {
	if depth <= 0 {
		panic("Maximum nesting depth limit exceeded")
	}

	if value == nil {
		return RqlTerm{
			termType: p.Term_DATUM,
			data:     nil,
		}
	}

	switch val := value.(type) {
	case RqlTerm:
		return val
	case time.Time:
		return EpochTime(val.Unix())
	case []interface{}:
		vals := []RqlTerm{}
		for _, v := range val {
			vals = append(vals, expr(v, depth))
		}

		return makeArray(vals)
	case map[string]interface{}:
		vals := map[string]RqlTerm{}
		for k, v := range val {
			vals[k] = expr(v, depth)
		}

		return makeObject(vals)
	default:
		// Use reflection to check for other types
		typ := reflect.TypeOf(val)
		rval := reflect.ValueOf(val)

		if typ.Kind() == reflect.Ptr || typ.Kind() == reflect.Interface {
			v := reflect.ValueOf(val)

			if v.IsNil() {
				return RqlTerm{
					termType: p.Term_DATUM,
					data:     nil,
				}
			}

			val = v.Elem().Interface()
			typ = reflect.TypeOf(val)
		}

		if typ.Kind() == reflect.Func {
			return makeFunc(val)
		}
		if typ.Kind() == reflect.Struct {
			data, err := encoding.Encode(val)

			if err != nil || data == nil {
				return RqlTerm{
					termType: p.Term_DATUM,
					data:     nil,
				}
			}

			return expr(data, depth-1)
		}
		if typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
			vals := []RqlTerm{}
			for i := 0; i < rval.Len(); i++ {
				vals = append(vals, expr(rval.Index(i).Interface(), depth))
			}

			return makeArray(vals)
		}
		if typ.Kind() == reflect.Map {
			vals := map[string]RqlTerm{}
			for _, k := range rval.MapKeys() {
				vals[k.String()] = expr(rval.MapIndex(k).Interface(), depth)
			}

			return makeObject(vals)
		}

		// If no other match was found then return a datum value
		return RqlTerm{
			termType: p.Term_DATUM,
			data:     val,
		}
	}
}

// Create a JavaScript expression.
func Js(js interface{}) RqlTerm {
	return newRqlTerm("Js", p.Term_JAVASCRIPT, []interface{}{js}, map[string]interface{}{})
}

// Parse a JSON string on the server.
func Json(json interface{}) RqlTerm {
	return newRqlTerm("Json", p.Term_JSON, []interface{}{json}, map[string]interface{}{})
}

// Throw a runtime error. If called with no arguments inside the second argument
// to `default`, re-throw the current error.
func Error(message interface{}) RqlTerm {
	return newRqlTerm("Error", p.Term_ERROR, []interface{}{message}, map[string]interface{}{})
}

// Evaluate the expr in the context of one or more value bindings. The type of
// the result is the type of the value returned from expr.
func (t RqlTerm) Do(f interface{}) RqlTerm {
	newArgs := []interface{}{}
	newArgs = append(newArgs, funcWrap(f))
	newArgs = append(newArgs, t)

	return newRqlTerm("Do", p.Term_FUNCALL, newArgs, map[string]interface{}{})
}

// Evaluate the expr in the context of one or more value bindings. The type of
// the result is the type of the value returned from expr.
func Do(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	newArgs := []interface{}{}
	newArgs = append(newArgs, funcWrap(args[len(args)-1]))
	newArgs = append(newArgs, args[:len(args)-1]...)

	return newRqlTerm("Do", p.Term_FUNCALL, newArgs, map[string]interface{}{})
}

// Evaluate one of two control paths based on the value of an expression.
// branch is effectively an if renamed due to language constraints.
//
// The type of the result is determined by the type of the branch that gets executed.
func Branch(test, trueBranch, falseBranch interface{}) RqlTerm {
	return newRqlTerm("Branch", p.Term_BRANCH, []interface{}{test, trueBranch, falseBranch}, map[string]interface{}{})
}

// Loop over a sequence, evaluating the given write query for each element.
func (t RqlTerm) ForEach(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Foreach", p.Term_FOREACH, []interface{}{funcWrap(f)}, map[string]interface{}{})
}

// Handle non-existence errors. Tries to evaluate and return its first argument.
// If an error related to the absence of a value is thrown in the process, or if
// its first argument returns null, returns its second argument. (Alternatively,
// the second argument may be a function which will be called with either the
// text of the non-existence error or null.)
func (t RqlTerm) Default(value interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Default", p.Term_DEFAULT, []interface{}{value}, map[string]interface{}{})
}

// Converts a value of one type into another.
//
// You can convert: a selection, sequence, or object into an ARRAY, an array of
// pairs into an OBJECT, and any DATUM into a STRING.
func (t RqlTerm) CoerceTo(typeName interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "CoerceTo", p.Term_COERCE_TO, []interface{}{typeName}, map[string]interface{}{})
}

// Gets the type of a value.
func (t RqlTerm) TypeOf() RqlTerm {
	return newRqlTermFromPrevVal(t, "TypeOf", p.Term_TYPEOF, []interface{}{}, map[string]interface{}{})
}

// Get information about a RQL value.
func (t RqlTerm) Info() RqlTerm {
	return newRqlTermFromPrevVal(t, "Info", p.Term_INFO, []interface{}{}, map[string]interface{}{})
}
