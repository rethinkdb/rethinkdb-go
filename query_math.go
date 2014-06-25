package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Add sums two numbers or concatenates two arrays.
func (t Term) Add(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Add", p.Term_ADD, args, map[string]interface{}{})
}

// Add sums two numbers or concatenates two arrays.
func Add(args ...interface{}) Term {
	return newRqlTerm("Add", p.Term_ADD, args, map[string]interface{}{})
}

// Sub subtracts two numbers.
func (t Term) Sub(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Sub", p.Term_SUB, args, map[string]interface{}{})
}

// Sub subtracts two numbers.
func Sub(args ...interface{}) Term {
	return newRqlTerm("Sub", p.Term_SUB, args, map[string]interface{}{})
}

// Mul multiplies two numbers.
func (t Term) Mul(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Mul", p.Term_MUL, args, map[string]interface{}{})
}

func Mul(args ...interface{}) Term {
	return newRqlTerm("Mul", p.Term_MUL, args, map[string]interface{}{})
}

// Div divides two numbers.
func (t Term) Div(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Div", p.Term_DIV, args, map[string]interface{}{})
}

// Div divides two numbers.
func Div(args ...interface{}) Term {
	return newRqlTerm("Div", p.Term_DIV, args, map[string]interface{}{})
}

// Mod divides two numbers and returns the remainder.
func (t Term) Mod(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Mod", p.Term_MOD, args, map[string]interface{}{})
}

// Mod divides two numbers and returns the remainder.
func Mod(args ...interface{}) Term {
	return newRqlTerm("Mod", p.Term_MOD, args, map[string]interface{}{})
}

// And performs a logical and on two values.
func (t Term) And(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "And", p.Term_ALL, args, map[string]interface{}{})
}

// And performs a logical and on two values.
func And(args ...interface{}) Term {
	return newRqlTerm("And", p.Term_ALL, args, map[string]interface{}{})
}

// Or performs a logical or on two values.
func (t Term) Or(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Or", p.Term_ANY, args, map[string]interface{}{})
}

// Or performs a logical or on two values.
func Or(args ...interface{}) Term {
	return newRqlTerm("Or", p.Term_ANY, args, map[string]interface{}{})
}

// Eq returns true if two values are equal.
func (t Term) Eq(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Eq", p.Term_EQ, args, map[string]interface{}{})
}

// Eq returns true if two values are equal.
func Eq(args ...interface{}) Term {
	return newRqlTerm("Eq", p.Term_EQ, args, map[string]interface{}{})
}

// Ne returns true if two values are not equal.
func (t Term) Ne(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Ne", p.Term_NE, args, map[string]interface{}{})
}

// Ne returns true if two values are not equal.
func Ne(args ...interface{}) Term {
	return newRqlTerm("Ne", p.Term_NE, args, map[string]interface{}{})
}

// Gt returns true if the first value is greater than the second.
func (t Term) Gt(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Gt", p.Term_GT, args, map[string]interface{}{})
}

// Gt returns true if the first value is greater than the second.
func Gt(args ...interface{}) Term {
	return newRqlTerm("Gt", p.Term_GT, args, map[string]interface{}{})
}

// Ge returns true if the first value is greater than or equal to the second.
func (t Term) Ge(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Ge", p.Term_GE, args, map[string]interface{}{})
}

// Ge returns true if the first value is greater than or equal to the second.
func Ge(args ...interface{}) Term {
	return newRqlTerm("Ge", p.Term_GE, args, map[string]interface{}{})
}

// Lt returns true if the first value is less than the second.
func (t Term) Lt(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Lt", p.Term_LT, args, map[string]interface{}{})
}

// Lt returns true if the first value is less than the second.
func Lt(args ...interface{}) Term {
	return newRqlTerm("Lt", p.Term_LT, args, map[string]interface{}{})
}

// Le returns true if the first value is less than or equal to the second.
func (t Term) Le(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Le", p.Term_LE, args, map[string]interface{}{})
}

// Le returns true if the first value is less than or equal to the second.
func Le(args ...interface{}) Term {
	return newRqlTerm("Le", p.Term_LE, args, map[string]interface{}{})
}

// Not performs a logical not on a value.
func (t Term) Not(args ...interface{}) Term {
	return newRqlTermFromPrevVal(t, "Not", p.Term_NOT, args, map[string]interface{}{})
}

// Not performs a logical not on a value.
func Not(args ...interface{}) Term {
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
func (t Term) Random(args ...interface{}) Term {
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
