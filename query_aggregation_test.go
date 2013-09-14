package rethinkgo

import (
	"fmt"
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestAggregationExprCount(c *test.C) {
	query := Expr(List{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}).Count()
	fmt.Println(query.String())
	query.Run(conn)
}

func (s *RethinkSuite) TestAggregationExprSum(c *test.C) {
	query := Expr(1)
	fmt.Println(query.String())
	query.Run(conn)
}

func (s *RethinkSuite) TestAggregationExprAvg(c *test.C) {
	query := Expr(1)
	fmt.Println(query.String())
	query.Run(conn)
}
