package gorethink

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestControlExecNil(c *test.C) {
	var response interface{}
	query := Expr(nil)
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, nil)
}

func (s *RethinkSuite) TestControlExecSimple(c *test.C) {
	var response int
	query := Expr(1)
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
}

func (s *RethinkSuite) TestControlExecList(c *test.C) {
	var response []interface{}
	query := Expr(narr)
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

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
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

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

func (s *RethinkSuite) TestControlStruct(c *test.C) {
	var response map[string]interface{}
	query := Expr(str)
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"id": "A",
		"B":  1,
		"D":  map[string]interface{}{"D2": "2", "D1": 1},
		"E":  []interface{}{"E1", "E2", "E3", 4},
		"F": map[string]interface{}{
			"XA": 2,
			"XB": "B",
			"XC": []interface{}{"XC1", "XC2"},
			"XD": map[string]interface{}{
				"YA": 3,
				"YB": map[string]interface{}{
					"1": "1",
					"2": "2",
					"3": 3,
				},
				"YC": map[string]interface{}{
					"YC1": "YC1",
				},
				"YD": map[string]interface{}{
					"YD1": "YD1",
				},
			},
			"XE": "XE",
			"XF": []interface{}{"XE1", "XE2"},
		},
	})
}

func (s *RethinkSuite) TestControlMapTypeAlias(c *test.C) {
	var response TMap
	query := Expr(TMap{"A": 1, "B": 2})
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, TMap{"A": 1, "B": 2})
}

func (s *RethinkSuite) TestControlStringTypeAlias(c *test.C) {
	var response TStr
	query := Expr(TStr("Hello"))
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, TStr("Hello"))
}

func (s *RethinkSuite) TestControlExecTypes(c *test.C) {
	var response []interface{}
	query := Expr([]interface{}{int64(1), uint64(1), float64(1.0), int32(1), uint32(1), float32(1), "1", true, false})
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{int64(1), uint64(1), float64(1.0), int32(1), uint32(1), float32(1), "1", true, false})
}

func (s *RethinkSuite) TestControlJs(c *test.C) {
	var response int
	query := Js("1;")
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
}

func (s *RethinkSuite) TestControlJson(c *test.C) {
	var response []int
	query := Json("[1,2,3]")
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{1, 2, 3})
}

func (s *RethinkSuite) TestControlError(c *test.C) {
	var response interface{}
	query := Error("An error occurred")
	r, err := query.RunRow(sess)
	c.Assert(err, test.NotNil)

	err = r.Scan(&response)

	c.Assert(err, test.NotNil)
	c.Assert(err, test.FitsTypeOf, RqlRuntimeError{})
	c.Assert(err.Error(), test.Equals, "gorethink: An error occurred in: \nr.Error(\"An error occurred\")")
}

func (s *RethinkSuite) TestControlDoNothing(c *test.C) {
	var response []interface{}
	query := Do([]interface{}{map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2}, map[string]interface{}{"a": 3}})
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

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
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

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
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

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
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
}

func (s *RethinkSuite) TestControlBranchWithMapExpr(c *test.C) {
	var response []interface{}
	query := Expr([]interface{}{1, 2, 3}).Map(Branch(
		Row.Eq(2),
		Row.Sub(1),
		Row.Add(1),
	))
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{2, 1, 4})
}

func (s *RethinkSuite) TestControlDefault(c *test.C) {
	var response []interface{}
	query := Expr(defaultObjList).Map(func(row RqlTerm) RqlTerm {
		return row.Field("a").Default(1)
	})
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{1, 1})
}

func (s *RethinkSuite) TestControlCoerceTo(c *test.C) {
	var response string
	query := Expr(1).CoerceTo("STRING")
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "1")
}

func (s *RethinkSuite) TestControlTypeOf(c *test.C) {
	var response string
	query := Expr(1).TypeOf()
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "NUMBER")
}
