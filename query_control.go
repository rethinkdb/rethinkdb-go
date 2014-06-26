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
func Expr(value interface{}) Term {
	return expr(value, 20)
}

func expr(value interface{}, depth int) Term {
	if depth <= 0 {
		panic("Maximum nesting depth limit exceeded")
	}

	if value == nil {
		return Term{
			termType: p.Term_DATUM,
			data:     nil,
		}
	}

	switch val := value.(type) {
	case Term:
		return val
	case time.Time:
		return EpochTime(val.Unix())
	case []interface{}:
		vals := []Term{}
		for _, v := range val {
			vals = append(vals, expr(v, depth))
		}

		return makeArray(vals)
	case map[string]interface{}:
		vals := map[string]Term{}
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
				return Term{
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
				return Term{
					termType: p.Term_DATUM,
					data:     nil,
				}
			}

			return expr(data, depth-1)
		}
		if typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
			vals := []Term{}
			for i := 0; i < rval.Len(); i++ {
				vals = append(vals, expr(rval.Index(i).Interface(), depth))
			}

			return makeArray(vals)
		}
		if typ.Kind() == reflect.Map {
			vals := map[string]Term{}
			for _, k := range rval.MapKeys() {
				vals[k.String()] = expr(rval.MapIndex(k).Interface(), depth)
			}

			return makeObject(vals)
		}

		// If no other match was found then return a datum value
		return Term{
			termType: p.Term_DATUM,
			data:     val,
		}
	}
}

// Create a JavaScript expression.
func Js(jssrc interface{}) Term {
	return constructRootTerm("Js", p.Term_JAVASCRIPT, []interface{}{jssrc}, map[string]interface{}{})
}

type HttpOpts struct {
	// General Options
	Timeout      interface{} `gorethink:"timeout,omitempty"`
	Reattempts   interface{} `gorethink:"reattempts,omitempty"`
	Redirects    interface{} `gorethink:"redirect,omitempty"`
	Verify       interface{} `gorethink:"verify,omitempty"`
	ResultFormat interface{} `gorethink:"resul_format,omitempty"`

	// Request Options
	Method interface{} `gorethink:"method,omitempty"`
	Auth   interface{} `gorethink:"auth,omitempty"`
	Params interface{} `gorethink:"params,omitempty"`
	Header interface{} `gorethink:"header,omitempty"`
	Data   interface{} `gorethink:"data,omitempty"`

	// Pagination
	Page      interface{} `gorethink:"page,omitempty"`
	PageLimit interface{} `gorethink:"page_limit,omitempty"`
}

func (o *HttpOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Parse a JSON string on the server.
func Http(url interface{}, optArgs ...HttpOpts) Term {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return constructRootTerm("Http", p.Term_HTTP, []interface{}{url}, opts)
}

// Parse a JSON string on the server.
func Json(args ...interface{}) Term {
	return constructRootTerm("Json", p.Term_JSON, args, map[string]interface{}{})
}

// Throw a runtime error. If called with no arguments inside the second argument
// to `default`, re-throw the current error.
func Error(args ...interface{}) Term {
	return constructRootTerm("Error", p.Term_ERROR, args, map[string]interface{}{})
}

// Args is a special term usd to splice an array of arguments into another term.
// This is useful when you want to call a varadic term such as GetAll with a set
// of arguments provided at runtime.
func Args(args ...interface{}) Term {
	return constructRootTerm("Args", p.Term_ARGS, args, map[string]interface{}{})
}

// Evaluate the expr in the context of one or more value bindings. The type of
// the result is the type of the value returned from expr.
func (t Term) Do(args ...interface{}) Term {
	newArgs := []interface{}{}
	newArgs = append(newArgs, funcWrap(args[len(args)-1]))
	newArgs = append(newArgs, t)
	newArgs = append(newArgs, args[:len(args)-1]...)

	return constructRootTerm("Do", p.Term_FUNCALL, newArgs, map[string]interface{}{})
}

// Evaluate the expr in the context of one or more value bindings. The type of
// the result is the type of the value returned from expr.
func Do(args ...interface{}) Term {
	newArgs := []interface{}{}
	newArgs = append(newArgs, funcWrap(args[len(args)-1]))
	newArgs = append(newArgs, args[:len(args)-1]...)

	return constructRootTerm("Do", p.Term_FUNCALL, newArgs, map[string]interface{}{})
}

// Evaluate one of two control paths based on the value of an expression.
// branch is effectively an if renamed due to language constraints.
//
// The type of the result is determined by the type of the branch that gets executed.
func Branch(args ...interface{}) Term {
	return constructRootTerm("Branch", p.Term_BRANCH, args, map[string]interface{}{})
}

// Loop over a sequence, evaluating the given write query for each element.
func (t Term) ForEach(args ...interface{}) Term {
	return constructMethodTerm(t, "Foreach", p.Term_FOREACH, funcWrapArgs(args), map[string]interface{}{})
}

// Handle non-existence errors. Tries to evaluate and return its first argument.
// If an error related to the absence of a value is thrown in the process, or if
// its first argument returns null, returns its second argument. (Alternatively,
// the second argument may be a function which will be called with either the
// text of the non-existence error or null.)
func (t Term) Default(args ...interface{}) Term {
	return constructMethodTerm(t, "Default", p.Term_DEFAULT, args, map[string]interface{}{})
}

// Converts a value of one type into another.
//
// You can convert: a selection, sequence, or object into an ARRAY, an array of
// pairs into an OBJECT, and any DATUM into a STRING.
func (t Term) CoerceTo(args ...interface{}) Term {
	return constructMethodTerm(t, "CoerceTo", p.Term_COERCE_TO, args, map[string]interface{}{})
}

// Gets the type of a value.
func (t Term) TypeOf(args ...interface{}) Term {
	return constructMethodTerm(t, "TypeOf", p.Term_TYPEOF, args, map[string]interface{}{})
}

// Get information about a RQL value.
func (t Term) Info(args ...interface{}) Term {
	return constructMethodTerm(t, "Info", p.Term_INFO, args, map[string]interface{}{})
}
