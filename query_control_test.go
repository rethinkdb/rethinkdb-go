package rethinkgo

import (
	"errors"
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestControlExecSimple(c *test.C) {
	var response int
	query := Expr(1)
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
}

func (s *RethinkSuite) TestControlExecList(c *test.C) {
	var response []interface{}
	query := Expr(List{
		1, 2, 3, 4, 5, 6, List{
			7.1, 7.2, 7.3,
		},
	})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{
		1, 2, 3, 4, 5, 6, List{
			7.1, 7.2, 7.3,
		},
	})
}

func (s *RethinkSuite) TestControlExecObj(c *test.C) {
	var response map[string]interface{}
	query := Expr(Obj{
		"A": 1,
		"B": 2,
		"C": Obj{
			"1": 3,
			"2": 4,
		},
	})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, Obj{
		"A": 1,
		"B": 2,
		"C": Obj{
			"1": 3,
			"2": 4,
		},
	})
}

func (s *RethinkSuite) TestControlExecTypes(c *test.C) {
	var response []interface{}
	query := Expr(List{int64(1), uint64(1), float64(1.0), int32(1), uint32(1), float32(1), "1", true, false})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{int64(1), uint64(1), float64(1.0), int32(1), uint32(1), float32(1), "1", true, false})
}

func (s *RethinkSuite) TestControlJs(c *test.C) {
	var response int
	query := Js("1;")
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
}

func (s *RethinkSuite) TestControlJson(c *test.C) {
	var response []int
	query := Json("[1,2,3]")
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 2, 3})
}

func (s *RethinkSuite) TestControlError(c *test.C) {
	c.Skip("Need to implement other functions first")
	var response []interface{}
	query := Error("An error occurred")
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.Equals, errors.New("An error occurred"))
}

func (s *RethinkSuite) TestControlDoNothing(c *test.C) {
	var response []interface{}
	query := Do(List{Obj{"a": 1}, Obj{"a": 2}, Obj{"a": 3}})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{Obj{"a": 1}, Obj{"a": 2}, Obj{"a": 3}})
}

func (s *RethinkSuite) TestControlDo(c *test.C) {
	var response []interface{}
	query := Do(List{
		Obj{"a": 1},
		Obj{"a": 2},
		Obj{"a": 3},
	}, func(row RqlTerm) RqlTerm {
		return row.Field("a")
	})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 2, 3})
}

func (s *RethinkSuite) TestControlDoWithExpr(c *test.C) {
	var response []interface{}
	query := Expr(List{
		Obj{"a": 1},
		Obj{"a": 2},
		Obj{"a": 3},
	}).Do(func(row RqlTerm) RqlTerm {
		return row.Field("a")
	})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 2, 3})
}

func (s *RethinkSuite) TestControlBranchSimple(c *test.C) {
	var response int
	query := Branch(
		true,
		1,
		2,
	)
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
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
