package gorethink

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestQueryRun(c *test.C) {
	var response string

	err := Expr("Test").RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "Test")
}

func (s *RethinkSuite) TestQueryRunRawTime(c *test.C) {
	var response map[string]interface{}

	err := Now().RunRow(conn, "time_format", "raw").Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response["$reql_type$"], test.NotNil)
	c.Assert(response["$reql_type$"], test.Equals, "TIME")
}
