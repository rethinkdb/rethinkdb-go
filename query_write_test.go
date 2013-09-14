package rethinkgo

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestWriteInsert(c *test.C) {
	query := Db("test").Table("test").Insert(Obj{"num": 1})
	_, err := query.Run(conn)
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TestWriteUpdate(c *test.C) {
	query := Db("test").Table("test").Insert(Obj{"num": 1})
	_, err := query.Run(conn)
	c.Assert(err, test.IsNil)

	// Update the first row in the table
	query = Db("test").Table("test").Nth(1).Update(Obj{"num": 2})
	_, err = query.Run(conn)
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TestWriteReplacec(c *test.C) {
	query := Db("test").Table("test").Insert(Obj{"num": 1})
	_, err := query.Run(conn)
	c.Assert(err, test.IsNil)

	// Replace the first row in the table
	query = Db("test").Table("test").Nth(1).Update(Obj{"num": 2})
	_, err = query.Run(conn)
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TestWriteDelete(c *test.C) {
	query := Db("test").Table("test").Insert(Obj{"num": 1})
	_, err := query.Run(conn)
	c.Assert(err, test.IsNil)

	// Delete the first row in the table
	query = Db("test").Table("test").Nth(1).Delete()
	_, err = query.Run(conn)
	c.Assert(err, test.IsNil)
}
