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

// Sub subtracts two numbers.
//
// Example usage:
//
//  r.Expr(2).Sub(2) => 0
func (t RqlVal) Sub(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "sub", p.Term_SUB, args, Obj{})
}

func Sub(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("sub", p.Term_SUB, args, Obj{})
}

// Mul multiplies two numbers.
//
// Example usage:
//
//  r.Expr(2).Mul(3) => 6
func (t RqlVal) Mul(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "mul", p.Term_MUL, args, Obj{})
}

func Mul(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("mul", p.Term_MUL, args, Obj{})
}

// Div divides two numbers.
//
// Example usage:
//
//  r.Expr(3).Div(2) => 1.5
func (t RqlVal) Div(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "div", p.Term_DIV, args, Obj{})
}

func Div(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("div", p.Term_DIV, args, Obj{})
}

// Mod divides two numbers and returns the remainder.
//
// Example usage:
//
//  r.Expr(23).Mod(10) => 3
func (t RqlVal) Mod(args ...interface{}) RqlOp {
	enforceArgLength(1, 1, args)

	return newRqlValFromPrevVal(t, "mod", p.Term_MOD, args, Obj{})
}

func Mod(args ...interface{}) RqlOp {
	enforceArgLength(2, 2, args)

	return newRqlVal("mod", p.Term_MOD, args, Obj{})
}

// And performs a logical and on two values.
//
// Example usage:
//
//  r.Expr(true).And(true) => true
func (t RqlVal) And(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "and", p.Term_ALL, args, Obj{})
}

func And(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("and", p.Term_ALL, args, Obj{})
}

// Or performs a logical or on two values.
//
// Example usage:
//
//  r.Expr(true).Or(false) => true
func (t RqlVal) Or(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "or", p.Term_ANY, args, Obj{})
}

func Or(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("or", p.Term_ANY, args, Obj{})
}

// Eq returns true if two values are equal.
//
// Example usage:
//
//  r.Expr(1).Eq(1) => true
func (t RqlVal) Eq(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "eq", p.Term_EQ, args, Obj{})
}

func Eq(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("eq", p.Term_EQ, args, Obj{})
}

// Ne returns true if two values are not equal.
//
// Example usage:
//
//  r.Expr(1).Ne(-1) => true
func (t RqlVal) Ne(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "ne", p.Term_NE, args, Obj{})
}

func Ne(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("ne", p.Term_NE, args, Obj{})
}

// Gt returns true if the first value is greater than the second.
//
// Example usage:
//
//  r.Expr(2).Gt(1) => true
func (t RqlVal) Gt(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "gt", p.Term_GT, args, Obj{})
}

func Gt(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("gt", p.Term_GT, args, Obj{})
}

// Gt returns true if the first value is greater than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Gt(2) => true
func (t RqlVal) Ge(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "ge", p.Term_GE, args, Obj{})
}

func Ge(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("ge", p.Term_GE, args, Obj{})
}

// Lt returns true if the first value is less than the second.
//
// Example usage:
//
//  r.Expr(1).Lt(2) => true
func (t RqlVal) Lt(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "lt", p.Term_LT, args, Obj{})
}

func Lt(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("lt", p.Term_LT, args, Obj{})
}

// Le returns true if the first value is less than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Lt(2) => true
func (t RqlVal) Le(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlValFromPrevVal(t, "le", p.Term_LE, args, Obj{})
}

func Le(args ...interface{}) RqlOp {
	enforceArgLength(2, 0, args)

	return newRqlVal("le", p.Term_LE, args, Obj{})
}

// Not performs a logical not on a value.
//
// Example usage:
//
//  r.Expr(true).Not() => false
func (t RqlVal) Not() RqlOp {
	return newRqlValFromPrevVal(t, "not", p.Term_NOT, List{}, Obj{})
}

func Not(args ...interface{}) RqlOp {
	enforceArgLength(1, 0, args)

	return newRqlVal("not", p.Term_NOT, args, Obj{})
}
