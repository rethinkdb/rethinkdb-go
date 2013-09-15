package rethinkgo

import (
	"github.com/dancannon/gorethink/encoding"
	p "github.com/dancannon/gorethink/ql2"
	"reflect"
	"time"
)

// Expr converts any value to an expression.  Internally it uses the `json`
// module to convert any literals, so any type annotations or methods understood
// by that module can be used. If the value cannot be converted, an error is
// returned at query .Run(session) time.
//
// If you want to call expression methods on an object that is not yet an
// expression, this is the function you want.
//
// Example usage:
//
//  var response interface{}
//  rows := r.Expr(r.Obj{"go": "awesome", "rethinkdb": "awesomer"}).Run(session).One(&response)
//
// Example response:
//
//  {"go": "awesome", "rethinkdb": "awesomer"}
func Expr(value interface{}) RqlTerm {
	return expr(value, 20)
}

func expr(value interface{}, depth int) RqlTerm {
	if depth <= 0 {
		panic("Maximum nesting depth limit exceeded")
	}

	switch val := value.(type) {
	case RqlTerm:
		return val
	case time.Time:
		return EpochTime(val.Unix())
	case List:
		vals := []RqlTerm{}
		for _, v := range val {
			vals = append(vals, expr(v, depth))
		}

		return makeArray(vals)
	case Obj:
		vals := map[string]RqlTerm{}
		for k, v := range val {
			vals[k] = expr(v, depth)
		}

		return makeObject(vals)
	default:
		// Use reflection to check for other types
		typ := reflect.TypeOf(val)

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

		// If no other match was found then return a datum value
		return RqlTerm{
			termType: p.Term_DATUM,
			data:     val,
		}
	}
}

func Js(js string) RqlTerm {
	return newRqlTerm("Js", p.Term_JAVASCRIPT, List{js}, Obj{})
}

func Json(json string) RqlTerm {
	return newRqlTerm("Json", p.Term_JSON, List{json}, Obj{})
}

func Error(message string) RqlTerm {
	return newRqlTerm("Error", p.Term_ERROR, List{message}, Obj{})
}

// Do evalutes the last argument (a function) using all previous arguments as the arguments to the function.
//
// For instance, Do(a, b, c, f) will be run as f(a, b, c).
//
// Example usage:
//
//  var response interface{}
//  err := r.Do(1, 2, 3, func(a, b, c r.Exp) interface{} {
//      return r.List{a, b, c}
//  }).Run(session).One(&response)
//
// Example response:
//
// [1,2,3]
func (t RqlTerm) Do(f interface{}) RqlTerm {
	newArgs := List{}
	newArgs = append(newArgs, funcWrap(f))
	newArgs = append(newArgs, t)

	return newRqlTerm("Do", p.Term_FUNCALL, newArgs, Obj{})
}

func Do(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	newArgs := List{}
	newArgs = append(newArgs, funcWrap(args[len(args)-1]))
	newArgs = append(newArgs, args[:len(args)-1]...)

	return newRqlTerm("Do", p.Term_FUNCALL, newArgs, Obj{})
}

func Branch(test, trueBranch, falseBranch interface{}) RqlTerm {
	return newRqlTerm("Branch", p.Term_BRANCH, List{test, trueBranch, falseBranch}, Obj{})
}

func (t RqlTerm) ForEach(f interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Foreach", p.Term_FOREACH, List{funcWrap(f)}, Obj{})
}

func (t RqlTerm) Default(value interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Default", p.Term_DEFAULT, List{value}, Obj{})
}

func (t RqlTerm) CoerceTo(typeName string) RqlTerm {
	return newRqlTermFromPrevVal(t, "CoerceTo", p.Term_COERCE_TO, List{typeName}, Obj{})
}

func (t RqlTerm) TypeOf() RqlTerm {
	return newRqlTermFromPrevVal(t, "TypeOf", p.Term_TYPEOF, List{}, Obj{})
}

func (t RqlTerm) Info() RqlTerm {
	return newRqlTermFromPrevVal(t, "Info", p.Term_INFO, List{}, Obj{})
}
