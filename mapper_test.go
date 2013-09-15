package rethinkgo

import (
	"fmt"
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

func (s *RethinkSuite) TestMapperScanLiteral(c *test.C) {
	rows, err := Expr(5).Run(conn)
	c.Assert(err, test.IsNil)

	for rows.Next() {
		var response interface{}
		rows.Scan(&response)

		fmt.Printf("%#v\n", response)
	}
}

func (s *RethinkSuite) TestMapperScanSlice(c *test.C) {
	rows, err := Expr(List{1, 2, 3, 4, 5}).Run(conn)
	c.Assert(err, test.IsNil)

	for rows.Next() {
		var response interface{}
		rows.Scan(&response)

		fmt.Printf("%#v\n", response)
	}
}

func (s *RethinkSuite) TestMapperScanIntSlice(c *test.C) {
	rows, err := Expr(List{1, 2, 3, 4, 5}).Run(conn)
	c.Assert(err, test.IsNil)

	for rows.Next() {
		var response []int
		rows.Scan(&response)

		fmt.Printf("%#v\n", response)
	}
}

func (s *RethinkSuite) TestMapperScanMap(c *test.C) {
	rows, err := Expr(Obj{
		"id":   2,
		"name": "Object 1",
		// "attr": List{Obj{
		// 	"name":  "attr 1",
		// 	"value": "value 1",
		// }},
	}).Run(conn)
	c.Assert(err, test.IsNil)

	for rows.Next() {
		var response map[string]interface{}
		err = rows.Scan(&response)
		c.Assert(err, test.IsNil)

		fmt.Printf("%#v\n", response)
	}
}

func (s *RethinkSuite) TestMapperScanStruct(c *test.C) {
	rows, err := Expr(Obj{
		"id":   2,
		"name": "Object 1",
		"Attrs": List{Obj{
			"Name":  "attr 1",
			"Value": "value 1",
		}},
	}).Run(conn)
	c.Assert(err, test.IsNil)

	for rows.Next() {
		var response object
		err = rows.Scan(&response)
		c.Assert(err, test.IsNil)

		fmt.Printf("%#v\n", response)
	}
}
