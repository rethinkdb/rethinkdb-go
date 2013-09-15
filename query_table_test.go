package rethinkgo

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestTableCreate(c *test.C) {
	var response interface{}

	Db("test").TableDrop("test").Exec(conn)

	// Test database creation
	query := Db("test").TableCreate("test")

	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, Obj{"created": 1})
}

func (s *RethinkSuite) TestTableList(c *test.C) {
	var response interface{}

	Db("test").TableCreate("test").Exec(conn)

	// Try and find it in the list
	success := false
	err := Db("test").TableList().RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.FitsTypeOf, []interface{}{})

	for _, db := range response.([]interface{}) {
		if db == "test" {
			success = true
		}
	}

	c.Assert(success, test.Equals, true)
}

func (s *RethinkSuite) TestTableDelete(c *test.C) {
	var response interface{}

	Db("test").TableCreate("test").Exec(conn)

	// Test database creation
	query := Db("test").TableDrop("test")

	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, Obj{"dropped": 1})
}

func (s *RethinkSuite) TestTableIndexCreate(c *test.C) {
	var response interface{}

	Db("test").TableCreate("test").Exec(conn)
	Db("test").Table("test").IndexDrop("test").Exec(conn)

	// Test database creation
	query := Db("test").Table("test").IndexCreate("test")

	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, Obj{"created": 1})
}

func (s *RethinkSuite) TestTableIndexList(c *test.C) {
	var response interface{}

	Db("test").TableCreate("test").Exec(conn)
	Db("test").Table("test").IndexCreate("test").Exec(conn)

	// Try and find it in the list
	success := false
	err := Db("test").Table("test").IndexList().RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.FitsTypeOf, []interface{}{})

	for _, db := range response.([]interface{}) {
		if db == "test" {
			success = true
		}
	}

	c.Assert(success, test.Equals, true)
}

func (s *RethinkSuite) TestTableIndexDelete(c *test.C) {
	var response interface{}

	Db("test").TableCreate("test").Exec(conn)
	Db("test").Table("test").IndexCreate("test").Exec(conn)

	// Test database creation
	query := Db("test").Table("test").IndexDrop("test")

	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, Obj{"dropped": 1})
}
