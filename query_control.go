package rethinkgo

import (
	p "github.com/christopherhesse/rethinkgo/ql2"
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
func Expr(value interface{}) RqlVal {
	return expr(value, 20)
}

func expr(value interface{}, depth int) RqlVal {
	if depth <= 0 {
		panic("Maximum nesting depth limit exceeded")
	}

	switch val := value.(type) {
	case RqlVal:
		return val
	// case func(...interface{}) RqlQueryBase:
	// 	return makeFunc(val, map[string]interface{}{})
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
		return RqlVal{
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
func (t RqlVal) Do(args ...interface{}) RqlVal {
	enforceArgLength(1, 1, args)

	return newRqlValFromPrevVal(t, "do", p.Term_FUNCALL, args, Obj{})
}

func Do(args ...interface{}) RqlVal {
	enforceArgLength(2, 0, args)

	return newRqlVal("do", p.Term_FUNCALL, args, Obj{})
}
