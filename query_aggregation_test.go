package rethinkgo

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestAggregationExprCount(c *test.C) {
	var response int
	query := Expr(List{1, 2, 3, 4, 5, 6, 7, 8, 9}).Count()
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 9)
}
