package gorethink

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestStringMatchSuccess(c *test.C) {
	query := Expr("id:0,name:mlucy,foo:bar").Match("name:(\\w+)").Field("groups").Nth(0).Field("str")

	var response string
	err := query.RunRow(sess).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "mlucy")
}

func (s *RethinkSuite) TestStringMatchFail(c *test.C) {
	query := Expr("id:0,foo:bar").Match("name:(\\w+)").Field("groups").Nth(0).Field("str")

	var response int
	err := query.RunRow(sess).Scan(&response)

	c.Assert(err, test.NotNil)
}
