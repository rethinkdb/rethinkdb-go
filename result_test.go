package rethinkgo

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestAtomResult(c *test.C) {
	query := Expr(List{1, 2, 3, 4, 5, 6, 7, 8, 9, 0})
	result, err := query.Run(conn)
	c.Assert(err, test.IsNil)

	num := 0
	for result.Next() {
		c.Assert(len(result.Row().([]interface{})), test.Equals, 10)
		num++
	}

	c.Assert(num, test.Equals, 1)
}
