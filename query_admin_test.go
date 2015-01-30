package gorethink

import (
	test "gopkg.in/check.v1"
)

func (s *RethinkSuite) TestAdminDbConfig(c *test.C) {
	Db("test").TableDrop("test").Exec(sess)
	Db("test").TableCreate("test").Exec(sess)

	// Test index rename
	query := Db("test").Table("test").Config()

	res, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	var response map[string]interface{}
	err = res.One(&response)
	c.Assert(err, test.IsNil)

	c.Assert(response["name"], test.Equals, "test")
}

func (s *RethinkSuite) TestAdminTableConfig(c *test.C) {
	Db("test").TableDrop("test").Exec(sess)
	Db("test").TableCreate("test").Exec(sess)

	// Test index rename
	query := Db("test").Config()

	res, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	var response map[string]interface{}
	err = res.One(&response)
	c.Assert(err, test.IsNil)

	c.Assert(response["name"], test.Equals, "test")
}

func (s *RethinkSuite) TestAdminTableStatus(c *test.C) {
	Db("test").TableDrop("test").Exec(sess)
	Db("test").TableCreate("test").Exec(sess)

	// Test index rename
	query := Db("test").Table("test").Status()

	res, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	var response map[string]interface{}
	err = res.One(&response)
	c.Assert(err, test.IsNil)

	c.Assert(response["name"], test.Equals, "test")
	c.Assert(response["status"], test.NotNil)
}

func (s *RethinkSuite) TestAdminWait(c *test.C) {
	Db("test").TableDrop("test").Exec(sess)
	Db("test").TableCreate("test").Exec(sess)

	// Test index rename
	query := Wait()

	res, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	var response map[string]interface{}
	err = res.One(&response)
	c.Assert(err, test.IsNil)

	c.Assert(response["ready"].(float64) > 0, test.Equals, true)
}

func (s *RethinkSuite) TestAdminStatus(c *test.C) {
	Db("test").TableDrop("test").Exec(sess)
	Db("test").TableCreate("test").Exec(sess)

	// Test index rename
	query := Db("test").Table("test").Wait()

	res, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	var response map[string]interface{}
	err = res.One(&response)
	c.Assert(err, test.IsNil)

	c.Assert(response["ready"], test.Equals, float64(1))
}
