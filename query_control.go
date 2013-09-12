package rethinkgo

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
func Expr(value interface{}) RqlOp {
	return expr(value, 20)
}

func expr(value interface{}, depth int) RqlOp {
	if depth <= 0 {
		panic("Maximum nesting depth limit exceeded")
	}

	switch val := value.(type) {
	case RqlOp:
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
		return RqlDatum{
			data: val,
		}
	}
}
