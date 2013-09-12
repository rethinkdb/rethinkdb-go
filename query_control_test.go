package rethinkgo

import (
	"fmt"
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestControlExecList(c *test.C) {
	query := Expr(List{
		1, 2, 3, 4, 5, 6, List{
			7.1, 7.2, 7.3,
		},
	})
	fmt.Println(query.String())
}

// func (s *RethinkSuite) TestControlExecObj(c *test.C) {
// 	query := Expr(Obj{
// 		"A": 1,
// 		"B": 2,
// 		"C": Obj{
// 			"1": 3,
// 			"2": 4,
// 		},
// 	})
// 	fmt.Println(query.String())
// }

func (s *RethinkSuite) TestControlDo(c *test.C) {
	query := Do(Obj{"a": 1}, Obj{"a": 2}, Obj{"a": 3}, func(row RqlTerm) RqlTerm {
		// return row.Field("a")
		return row
	})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestControlDoWithExpr(c *test.C) {
	query := Expr(List{
		Obj{"a": 1},
		Obj{"a": 2},
		Obj{"a": 3},
	}).Do(func(row RqlTerm) RqlTerm {
		// return row.Field("a")
		return row
	})
	fmt.Println(query.String())
}
