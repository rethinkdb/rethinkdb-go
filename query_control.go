package rethinkgo

import (
	p "github.com/christopherhesse/rethinkgo/ql2"
	"reflect"
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
	// case time.Time:
	// 	return EpochTime(val.Unix())
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
		if reflect.TypeOf(val).Kind() == reflect.Func {
			return makeFunc(val)
		}

		// If no other match was found then return a datum value
		return RqlTerm{
			termType: p.Term_DATUM,
			data:     val,
		}
	}
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
func (t RqlTerm) Do(args ...interface{}) RqlTerm {
	enforceArgLength(1, 1, args)
	args[len(args)-1] = funcWrap(args[len(args)-1])

	return newRqlTermFromPrevVal(t, "do", p.Term_FUNCALL, args, Obj{})
}

func Do(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)
	args[len(args)-1] = funcWrap(args[len(args)-1])

	return newRqlTerm("do", p.Term_FUNCALL, args, Obj{})
}
