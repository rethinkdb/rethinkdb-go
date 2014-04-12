package gorethink

import test "launchpad.net/gocheck"

func (s *RethinkSuite) TestQueryRun(c *test.C) {
	var response string

	row, err := Expr("Test").RunRow(sess)
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "Test")
}

func (s *RethinkSuite) TestQueryProfile(c *test.C) {
	var response string

	row, err := Expr("Test").RunRow(sess, RunOpts{
		Profile: true,
	})
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(row.Profile(), test.NotNil)
	c.Assert(response, test.Equals, "Test")
}

func (s *RethinkSuite) TestQueryRunRawTime(c *test.C) {
	var response map[string]interface{}

	row, err := Now().RunRow(sess, RunOpts{
		TimeFormat: "raw",
	})
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response["$reql_type$"], test.NotNil)
	c.Assert(response["$reql_type$"], test.Equals, "TIME")
}
