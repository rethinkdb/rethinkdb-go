package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Add sums two numbers or concatenates two arrays.
func (t RqlTerm) Add(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Add", p.Term_ADD, args, map[string]interface{}{})
}

// Add sums two numbers or concatenates two arrays.
func Add(args ...interface{}) RqlTerm {
	return newRqlTerm("Add", p.Term_ADD, args, map[string]interface{}{})
}

// Sub subtracts two numbers.
func (t RqlTerm) Sub(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Sub", p.Term_SUB, args, map[string]interface{}{})
}

// Sub subtracts two numbers.
func Sub(args ...interface{}) RqlTerm {
	return newRqlTerm("Sub", p.Term_SUB, args, map[string]interface{}{})
}

// Mul multiplies two numbers.
func (t RqlTerm) Mul(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Mul", p.Term_MUL, args, map[string]interface{}{})
}

func Mul(args ...interface{}) RqlTerm {
	return newRqlTerm("Mul", p.Term_MUL, args, map[string]interface{}{})
}

// Div divides two numbers.
func (t RqlTerm) Div(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Div", p.Term_DIV, args, map[string]interface{}{})
}

// Div divides two numbers.
func Div(args ...interface{}) RqlTerm {
	return newRqlTerm("Div", p.Term_DIV, args, map[string]interface{}{})
}

// Mod divides two numbers and returns the remainder.
func (t RqlTerm) Mod(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Mod", p.Term_MOD, args, map[string]interface{}{})
}

// Mod divides two numbers and returns the remainder.
func Mod(args ...interface{}) RqlTerm {
	return newRqlTerm("Mod", p.Term_MOD, args, map[string]interface{}{})
}

// And performs a logical and on two values.
func (t RqlTerm) And(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "And", p.Term_ALL, args, map[string]interface{}{})
}

// And performs a logical and on two values.
func And(args ...interface{}) RqlTerm {
	return newRqlTerm("And", p.Term_ALL, args, map[string]interface{}{})
}

// Or performs a logical or on two values.
func (t RqlTerm) Or(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Or", p.Term_ANY, args, map[string]interface{}{})
}

// Or performs a logical or on two values.
func Or(args ...interface{}) RqlTerm {
	return newRqlTerm("Or", p.Term_ANY, args, map[string]interface{}{})
}

// Eq returns true if two values are equal.
func (t RqlTerm) Eq(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Eq", p.Term_EQ, args, map[string]interface{}{})
}

// Eq returns true if two values are equal.
func Eq(args ...interface{}) RqlTerm {
	return newRqlTerm("Eq", p.Term_EQ, args, map[string]interface{}{})
}

// Ne returns true if two values are not equal.
func (t RqlTerm) Ne(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Ne", p.Term_NE, args, map[string]interface{}{})
}

// Ne returns true if two values are not equal.
func Ne(args ...interface{}) RqlTerm {
	return newRqlTerm("Ne", p.Term_NE, args, map[string]interface{}{})
}

// Gt returns true if the first value is greater than the second.
func (t RqlTerm) Gt(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Gt", p.Term_GT, args, map[string]interface{}{})
}

// Gt returns true if the first value is greater than the second.
func Gt(args ...interface{}) RqlTerm {
	return newRqlTerm("Gt", p.Term_GT, args, map[string]interface{}{})
}

// Ge returns true if the first value is greater than or equal to the second.
func (t RqlTerm) Ge(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Ge", p.Term_GE, args, map[string]interface{}{})
}

// Ge returns true if the first value is greater than or equal to the second.
func Ge(args ...interface{}) RqlTerm {
	return newRqlTerm("Ge", p.Term_GE, args, map[string]interface{}{})
}

// Lt returns true if the first value is less than the second.
func (t RqlTerm) Lt(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Lt", p.Term_LT, args, map[string]interface{}{})
}

// Lt returns true if the first value is less than the second.
func Lt(args ...interface{}) RqlTerm {
	return newRqlTerm("Lt", p.Term_LT, args, map[string]interface{}{})
}

// Le returns true if the first value is less than or equal to the second.
func (t RqlTerm) Le(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Le", p.Term_LE, args, map[string]interface{}{})
}

// Le returns true if the first value is less than or equal to the second.
func Le(args ...interface{}) RqlTerm {
	return newRqlTerm("Le", p.Term_LE, args, map[string]interface{}{})
}

// Not performs a logical not on a value.
func (t RqlTerm) Not(args ...interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "Not", p.Term_NOT, args, map[string]interface{}{})
}

// Not performs a logical not on a value.
func Not(args ...interface{}) RqlTerm {
	return newRqlTerm("Not", p.Term_NOT, args, map[string]interface{}{})
}

type RandomOpts struct {
	Float interface{} `gorethink:"float,omitempty"`
}

func (o *RandomOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Generate a random number between the given bounds. If no arguments are
// given, the result will be a floating-point number in the range [0,1).
//
// When passing a single argument, r.random(x), the result will be in the
// range [0,x), and when passing two arguments, r.random(x,y), the range is
// [x,y). If x and y are equal, an error will occur, unless generating a
// floating-point number, for which x will be returned.
//
// Note: The last argument given will always be the 'open' side of the range,
// but when generating a floating-point number, the 'open' side may be less
// than the 'closed' side.
func (t RqlTerm) Random(args ...interface{}) RqlTerm {
	var opts = map[string]interface{}{}

	// Look for options map
	if len(args) > 0 {
		if possibleOpts, ok := args[len(args)-1].(RandomOpts); ok {
			opts = possibleOpts.toMap()
			args = args[:len(args)-1]
		}
	}

	return newRqlTermFromPrevVal(t, "Random", p.Term_RANDOM, args, opts)
}
