package rethinkgo

import (
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
	query := Expr(narr)
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		1, 2, 3, 4, 5, 6, []interface{}{
			7.1, 7.2, 7.3,
		},
	})
}

func (s *RethinkSuite) TestControlExecObj(c *test.C) {
	var response map[string]interface{}
	query := Expr(nobj)
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"A": 1,
		"B": 2,
		"C": map[string]interface{}{
			"1": 3,
			"2": 4,
		},
	})
}

func (s *RethinkSuite) TestControlExecTypes(c *test.C) {
	var response []interface{}
	query := Expr([]interface{}{int64(1), uint64(1), float64(1.0), int32(1), uint32(1), float32(1), "1", true, false})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{int64(1), uint64(1), float64(1.0), int32(1), uint32(1), float32(1), "1", true, false})
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
	c.Assert(response, JsonEquals, []interface{}{1, 2, 3})
}

func (s *RethinkSuite) TestControlError(c *test.C) {
	var response []interface{}
	query := Error("An error occurred")
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.NotNil)
	c.Assert(err, test.FitsTypeOf, RqlRuntimeError{})
	c.Assert(err.(RqlRuntimeError).Error(), test.Equals, "gorethink: An error occurred in: \nr.Error(\"An error occurred\")")
}

func (s *RethinkSuite) TestControlDoNothing(c *test.C) {
	var response []interface{}
	query := Do([]interface{}{map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2}, map[string]interface{}{"a": 3}})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2}, map[string]interface{}{"a": 3}})
}

func (s *RethinkSuite) TestControlDo(c *test.C) {
	var response []interface{}
	query := Do([]interface{}{
		map[string]interface{}{"a": 1},
		map[string]interface{}{"a": 2},
		map[string]interface{}{"a": 3},
	}, func(row RqlTerm) RqlTerm {
		return row.Field("a")
	})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{1, 2, 3})
}

func (s *RethinkSuite) TestControlDoWithExpr(c *test.C) {
	var response []interface{}
	query := Expr([]interface{}{
		map[string]interface{}{"a": 1},
		map[string]interface{}{"a": 2},
		map[string]interface{}{"a": 3},
	}).Do(func(row RqlTerm) RqlTerm {
		return row.Field("a")
	})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{1, 2, 3})
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
	var response interface{}
	query := Expr([]interface{}{1, 2, 3}).Map(Branch(
		Row.Eq(2),
		Row.Sub(1),
		Row.Add(1),
	))
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{2, 1, 4})
}

func (s *RethinkSuite) TestControlDefault(c *test.C) {
	var response interface{}
	query := Expr(defaultObjList).Map(func(row RqlTerm) RqlTerm {
		return row.Field("a").Default(1)
	})
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{1, 1})
}

func (s *RethinkSuite) TestControlCoerceTo(c *test.C) {
	var response string
	query := Expr(1).CoerceTo("STRING")
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "1")
}

func (s *RethinkSuite) TestControlTypeOf(c *test.C) {
	var response string
	query := Expr(1).TypeOf()
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "NUMBER")
}
