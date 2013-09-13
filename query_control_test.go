package rethinkgo

import (
	"fmt"
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestControlExecSimple(c *test.C) {
	query := Expr(1)
	fmt.Println(query.String())
	query.Run(conn)
}

func (s *RethinkSuite) TestControlExecList(c *test.C) {
	query := Expr(List{
		1, 2, 3, 4, 5, 6, List{
			7.1, 7.2, 7.3,
		},
	})
	fmt.Println(query.String())
	query.Run(conn)
}

func (s *RethinkSuite) TestControlExecObj(c *test.C) {
	query := Expr(Obj{
		"A": 1,
		"B": 2,
		"C": Obj{
			"1": 3,
			"2": 4,
		},
	})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestControlExecTypes(c *test.C) {
	query := Expr(List{int64(1), uint64(1), float64(1.0), int32(1), uint32(1), float32(1), "1", true, false})
	fmt.Printf("%#v\n", query.String())
	query.Run(conn)
}

func (s *RethinkSuite) TestControlJs(c *test.C) {
	query := Js("return 1;")
	fmt.Println(query)
}

func (s *RethinkSuite) TestControlJson(c *test.C) {
	query := Json("[1,2,3]")
	fmt.Println(query)
}

func (s *RethinkSuite) TestControlError(c *test.C) {
	query := Error("An error occurred")
	fmt.Println(query)
}

func (s *RethinkSuite) TestControlDo(c *test.C) {
	query := Do(List{Obj{"a": 1}, Obj{"a": 2}, Obj{"a": 3}}, func(row RqlTerm) RqlTerm {
		return row.Field("a")
	})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestControlDoWithExpr(c *test.C) {
	query := Expr(List{
		Obj{"a": 1},
		Obj{"a": 2},
		Obj{"a": 3},
	}).Do(func(row RqlTerm) RqlTerm {
		return row.Field("a")
	})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestControlBranchSimple(c *test.C) {
	query := Branch(
		true,
		1,
		2,
	)
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestControlBranchWithMapExpr(c *test.C) {
	c.Skip("Need to implement other functions first")
	// query := Expr(List{1, 2, 3}).Map(Branch(
	// 	Row.Eq(2),
	// 	Row.Sub(1),
	// 	Row.Add(1),
	// ))
	// fmt.Println(query.String())
}

func (s *RethinkSuite) TestControlForEach(c *test.C) {
	c.Skip("Need to implement other functions first")
}

func (s *RethinkSuite) TestControlDefault(c *test.C) {
	c.Skip("Need to implement other functions first")
	// query := Expr(List{
	// 	Obj{"a": true},
	// 	Obj{"a": true},
	// }).Map(Row.Field("a").Default(false))
	// fmt.Println(query.String())
}
