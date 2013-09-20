package gorethink

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestDbCreate(c *test.C) {
	var response interface{}

	// Delete the test2 database if it already exists
	DbDrop("test").Exec(conn)

	// Test database creation
	query := DbCreate("test")

	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"created": 1})
}

func (s *RethinkSuite) TestDbList(c *test.C) {
	var response interface{}

	// create database
	DbCreate("test").Exec(conn)

	// Try and find it in the list
	success := false
	err := DbList().RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.FitsTypeOf, []interface{}{})

	for _, db := range response.([]interface{}) {
		if db == "test" {
			success = true
		}
	}

	c.Assert(success, test.Equals, true)
}

func (s *RethinkSuite) TestDbDelete(c *test.C) {
	var response interface{}

	// Delete the test2 database if it already exists
	DbCreate("test").Exec(conn)

	// Test database creation
	query := DbDrop("test")

	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"dropped": 1})

	// Ensure that there is still a test DB after the test has finished
	DbCreate("test").Exec(conn)
}
