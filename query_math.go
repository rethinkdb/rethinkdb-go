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
func (t RqlTerm) Add(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "add", p.Term_ADD, args, Obj{})
}

func Add(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("add", p.Term_ADD, args, Obj{})
}

// Sub subtracts two numbers.
//
// Example usage:
//
//  r.Expr(2).Sub(2) => 0
func (t RqlTerm) Sub(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "sub", p.Term_SUB, args, Obj{})
}

func Sub(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("sub", p.Term_SUB, args, Obj{})
}

// Mul multiplies two numbers.
//
// Example usage:
//
//  r.Expr(2).Mul(3) => 6
func (t RqlTerm) Mul(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "mul", p.Term_MUL, args, Obj{})
}

func Mul(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("mul", p.Term_MUL, args, Obj{})
}

// Div divides two numbers.
//
// Example usage:
//
//  r.Expr(3).Div(2) => 1.5
func (t RqlTerm) Div(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "div", p.Term_DIV, args, Obj{})
}

func Div(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("div", p.Term_DIV, args, Obj{})
}

// Mod divides two numbers and returns the remainder.
//
// Example usage:
//
//  r.Expr(23).Mod(10) => 3
func (t RqlTerm) Mod(args ...interface{}) RqlTerm {
	enforceArgLength(1, 1, args)

	return newRqlTermFromPrevVal(t, "mod", p.Term_MOD, args, Obj{})
}

func Mod(args ...interface{}) RqlTerm {
	enforceArgLength(2, 2, args)

	return newRqlTerm("mod", p.Term_MOD, args, Obj{})
}

// And performs a logical and on two values.
//
// Example usage:
//
//  r.Expr(true).And(true) => true
func (t RqlTerm) And(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "and", p.Term_ALL, args, Obj{})
}

func And(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("and", p.Term_ALL, args, Obj{})
}

// Or performs a logical or on two values.
//
// Example usage:
//
//  r.Expr(true).Or(false) => true
func (t RqlTerm) Or(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "or", p.Term_ANY, args, Obj{})
}

func Or(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("or", p.Term_ANY, args, Obj{})
}

// Eq returns true if two values are equal.
//
// Example usage:
//
//  r.Expr(1).Eq(1) => true
func (t RqlTerm) Eq(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "eq", p.Term_EQ, args, Obj{})
}

func Eq(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("eq", p.Term_EQ, args, Obj{})
}

// Ne returns true if two values are not equal.
//
// Example usage:
//
//  r.Expr(1).Ne(-1) => true
func (t RqlTerm) Ne(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "ne", p.Term_NE, args, Obj{})
}

func Ne(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("ne", p.Term_NE, args, Obj{})
}

// Gt returns true if the first value is greater than the second.
//
// Example usage:
//
//  r.Expr(2).Gt(1) => true
func (t RqlTerm) Gt(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "gt", p.Term_GT, args, Obj{})
}

func Gt(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("gt", p.Term_GT, args, Obj{})
}

// Gt returns true if the first value is greater than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Gt(2) => true
func (t RqlTerm) Ge(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "ge", p.Term_GE, args, Obj{})
}

func Ge(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("ge", p.Term_GE, args, Obj{})
}

// Lt returns true if the first value is less than the second.
//
// Example usage:
//
//  r.Expr(1).Lt(2) => true
func (t RqlTerm) Lt(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "lt", p.Term_LT, args, Obj{})
}

func Lt(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("lt", p.Term_LT, args, Obj{})
}

// Le returns true if the first value is less than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Lt(2) => true
func (t RqlTerm) Le(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTermFromPrevVal(t, "le", p.Term_LE, args, Obj{})
}

func Le(args ...interface{}) RqlTerm {
	enforceArgLength(2, 0, args)

	return newRqlTerm("le", p.Term_LE, args, Obj{})
}

// Not performs a logical not on a value.
//
// Example usage:
//
//  r.Expr(true).Not() => false
func (t RqlTerm) Not() RqlTerm {
	return newRqlTermFromPrevVal(t, "not", p.Term_NOT, List{}, Obj{})
}

func Not(args ...interface{}) RqlTerm {
	enforceArgLength(1, 0, args)

	return newRqlTerm("not", p.Term_NOT, args, Obj{})
}
