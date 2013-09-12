package rethinkgo

import (
	p "github.com/christopherhesse/rethinkgo/ql2"
)

// Add sums two numbers or concatenates two arrays.
//
// Example usage:
//
//  r.Expr(1,2,3).Add(r.Expr(4,5,6)) => [1,2,3,4,5,6]
//  r.Expr(2).Add(2) => 4
func (t RqlVal) Add(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "add", p.Term_ADD, args, Obj{})
}

func Add(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("add", p.Term_ADD, args, Obj{})
}

// // Sub subtracts two numbers.
// //
// // Example usage:
// //
// //  r.Expr(2).Sub(2) => 0
// func (t RqlVal) Sub(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_SUB.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, listargs)},
// 	}
// }

// func Sub(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_SUB.Enum(),
// 		[]RqlQueryBase{}{listToQueryList(args)},
// 	}
// }

// // Mul multiplies two numbers.
// //
// // Example usage:
// //
// //  r.Expr(2).Mul(3) => 6
// func (t RqlVal) Mul(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_MUL.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Mul(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_MUL.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Div divides two numbers.
// //
// // Example usage:
// //
// //  r.Expr(3).Div(2) => 1.5
// func (t RqlVal) Div(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_DIV.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Div(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_DIV.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Mod divides two numbers and returns the remainder.
// //
// // Example usage:
// //
// //  r.Expr(23).Mod(10) => 3
// func (t RqlVal) Mod(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_MOD.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Mod(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_MOD.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // And performs a logical and on two values.
// //
// // Example usage:
// //
// //  r.Expr(true).And(true) => true
// func (t RqlVal) And(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_ALL.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func And(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_ALL.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Or performs a logical or on two values.
// //
// // Example usage:
// //
// //  r.Expr(true).Or(false) => true
// func (t RqlVal) Or(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_OR.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Or(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_OR.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Eq returns true if two values are equal.
// //
// // Example usage:
// //
// //  r.Expr(1).Eq(1) => true
// func (t RqlVal) Eq(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_EQ.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Eq(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_EQ.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Ne returns true if two values are not equal.
// //
// // Example usage:
// //
// //  r.Expr(1).Ne(-1) => true
// func (t RqlVal) Ne(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_NE.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Ne(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_NE.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Gt returns true if the first value is greater than the second.
// //
// // Example usage:
// //
// //  r.Expr(2).Gt(1) => true
// func (t RqlVal) Gt(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_GT.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Gt(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_GT.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Gt returns true if the first value is greater than or equal to the second.
// //
// // Example usage:
// //
// //  r.Expr(2).Gt(2) => true
// func (t RqlVal) Ge(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.TERM_GE.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Ge(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.TERM_GE.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Lt returns true if the first value is less than the second.
// //
// // Example usage:
// //
// //  r.Expr(1).Lt(2) => true
// func (t RqlVal) Lt(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.Term_LT.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Lt(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.Term_LT.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Le returns true if the first value is less than or equal to the second.
// //
// // Example usage:
// //
// //  r.Expr(2).Lt(2) => true
// func (t RqlVal) Le(args interface{}) RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.Term_LE.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Le(args interface{}) RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.Term_LE.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }

// // Not performs a logical not on a value.
// //
// // Example usage:
// //
// //  r.Expr(true).Not() => false
// func (t RqlVal) Not() RqlTerm {
// 	enforceArgLength(1, 0, args)

// 	return RqlQuery{
// 		p.Term_NOT.Enum(),
// 		[]RqlQueryBase{}{mergeArgs(t, args)},
// 	}
// }

// func Not() RqlTerm {
// 	enforceArgLength(2, 0, args)

// 	return RqlQuery{
// 		p.Term_NOT.Enum(),
// 		[]RqlQueryBase{}{args},
// 	}
// }
