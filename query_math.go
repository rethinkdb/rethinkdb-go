package rethinkgo

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Add sums two numbers or concatenates two arrays.
//
// Example usage:
//
//  r.Expr(1,2,3).Add(r.Expr(4,5,6)) => [1,2,3,4,5,6]
//  r.Expr(2).Add(2) => 4
func (t RqlTerm) Add(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Add", p.Term_ADD, args, map[string]interface{}{})
}

func Add(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Add", p.Term_ADD, args, map[string]interface{}{})
}

// Sub subtracts two numbers.
//
// Example usage:
//
//  r.Expr(2).Sub(2) => 0
func (t RqlTerm) Sub(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Sub", p.Term_SUB, args, map[string]interface{}{})
}

func Sub(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Sub", p.Term_SUB, args, map[string]interface{}{})
}

// Mul multiplies two numbers.
//
// Example usage:
//
//  r.Expr(2).Mul(3) => 6
func (t RqlTerm) Mul(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Mul", p.Term_MUL, args, map[string]interface{}{})
}

func Mul(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Mul", p.Term_MUL, args, map[string]interface{}{})
}

// Div divides two numbers.
//
// Example usage:
//
//  r.Expr(3).Div(2) => 1.5
func (t RqlTerm) Div(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Div", p.Term_DIV, args, map[string]interface{}{})
}

func Div(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Div", p.Term_DIV, args, map[string]interface{}{})
}

// Mod divides two numbers and returns the remainder.
//
// Example usage:
//
//  r.Expr(23).Mod(10) => 3
func (t RqlTerm) Mod(args ...interface{}) RqlTerm {
	enforceArgLength(1, 1, args)

	return newRqlTermFromPrevVal(t, "Mod", p.Term_MOD, args, map[string]interface{}{})
}

func Mod(args ...interface{}) RqlTerm {
	enforceArgLength(2, 2, args)

	return newRqlTerm("Mod", p.Term_MOD, args, map[string]interface{}{})
}

// And performs a logical and on two values.
//
// Example usage:
//
//  r.Expr(true).And(true) => true
func (t RqlTerm) And(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "And", p.Term_ALL, args, map[string]interface{}{})
}

func And(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("And", p.Term_ALL, args, map[string]interface{}{})
}

// Or performs a logical or on two values.
//
// Example usage:
//
//  r.Expr(true).Or(false) => true
func (t RqlTerm) Or(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Or", p.Term_ANY, args, map[string]interface{}{})
}

func Or(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Or", p.Term_ANY, args, map[string]interface{}{})
}

// Eq returns true if two values are equal.
//
// Example usage:
//
//  r.Expr(1).Eq(1) => true
func (t RqlTerm) Eq(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Eq", p.Term_EQ, args, map[string]interface{}{})
}

func Eq(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Eq", p.Term_EQ, args, map[string]interface{}{})
}

// Ne returns true if two values are not equal.
//
// Example usage:
//
//  r.Expr(1).Ne(-1) => true
func (t RqlTerm) Ne(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Ne", p.Term_NE, args, map[string]interface{}{})
}

func Ne(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Ne", p.Term_NE, args, map[string]interface{}{})
}

// Gt returns true if the first value is greater than the second.
//
// Example usage:
//
//  r.Expr(2).Gt(1) => true
func (t RqlTerm) Gt(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Gt", p.Term_GT, args, map[string]interface{}{})
}

func Gt(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Gt", p.Term_GT, args, map[string]interface{}{})
}

// Gt returns true if the first value is greater than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Gt(2) => true
func (t RqlTerm) Ge(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Ge", p.Term_GE, args, map[string]interface{}{})
}

func Ge(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Ge", p.Term_GE, args, map[string]interface{}{})
}

// Lt returns true if the first value is less than the second.
//
// Example usage:
//
//  r.Expr(1).Lt(2) => true
func (t RqlTerm) Lt(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Lt", p.Term_LT, args, map[string]interface{}{})
}

func Lt(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Lt", p.Term_LT, args, map[string]interface{}{})
}

// Le returns true if the first value is less than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Lt(2) => true
func (t RqlTerm) Le(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTermFromPrevVal(t, "Le", p.Term_LE, args, map[string]interface{}{})
}

func Le(args ...interface{}) RqlTerm {
	enforceArgLength(2, -1, args)

	return newRqlTerm("Le", p.Term_LE, args, map[string]interface{}{})
}

// Not performs a logical not on a value.
//
// Example usage:
//
//  r.Expr(true).Not() => false
func (t RqlTerm) Not() RqlTerm {
	return newRqlTermFromPrevVal(t, "Not", p.Term_NOT, []interface{}{}, map[string]interface{}{})
}

func Not(args ...interface{}) RqlTerm {
	enforceArgLength(1, -1, args)

	return newRqlTerm("Not", p.Term_NOT, args, map[string]interface{}{})
}
