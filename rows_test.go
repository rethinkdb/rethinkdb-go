package rethinkgo

import (
	test "launchpad.net/gocheck"
)

type object struct {
	Id    int64  `rethinkdb:"id"`
	Name  string `rethinkdb:"name"`
	Attrs []attr
}

type attr struct {
	Name  string
	Value interface{}
}

func (s *RethinkSuite) TestResultScanLiteral(c *test.C) {
	row := Expr(5).RunRow(conn)

	var response interface{}
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, 5)
}

func (s *RethinkSuite) TestResultScanSlice(c *test.C) {
	row := Expr(List{1, 2, 3, 4, 5}).RunRow(conn)

	var response interface{}
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 2, 3, 4, 5})
}

func (s *RethinkSuite) TestResultScanMap(c *test.C) {
	row := Expr(Obj{
		"id":   2,
		"name": "Object 1",
	}).RunRow(conn)

	var response map[string]interface{}
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, Obj{
		"id":   2,
		"name": "Object 1",
	})
}

func (s *RethinkSuite) TestResultScanMapIntoInterface(c *test.C) {
	row := Expr(Obj{
		"id":   2,
		"name": "Object 1",
	}).RunRow(conn)

	var response interface{}
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, Obj{
		"id":   2,
		"name": "Object 1",
	})
}

func (s *RethinkSuite) TestResultScanMapNested(c *test.C) {
	row := Expr(Obj{
		"id":   2,
		"name": "Object 1",
		"attr": List{Obj{
			"name":  "attr 1",
			"value": "value 1",
		}},
	}).RunRow(conn)

	var response interface{}
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, Obj{
		"id":   2,
		"name": "Object 1",
		"attr": List{Obj{
			"name":  "attr 1",
			"value": "value 1",
		}},
	})
}

func (s *RethinkSuite) TestResultScanStruct(c *test.C) {
	row := Expr(Obj{
		"id":   2,
		"name": "Object 1",
		"Attrs": List{Obj{
			"Name":  "attr 1",
			"Value": "value 1",
		}},
	}).RunRow(conn)

	var response object
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.DeepEquals, object{
		Id:   2,
		Name: "Object 1",
		Attrs: []attr{attr{
			Name:  "attr 1",
			Value: "value 1",
		}},
	})
}

func (s *RethinkSuite) TestResultAtomString(c *test.C) {
	row := Expr("a").RunRow(conn)

	var response string
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "a")
}

func (s *RethinkSuite) TestResultAtomArray(c *test.C) {
	row := Expr(List{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}).RunRow(conn)

	var response []int
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.DeepEquals, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 0})
}
