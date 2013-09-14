package rethinkgo

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestWriteInsert(c *test.C) {
	// Insert 1500 test rows
	query := Db("test").Table("test").Insert(Obj{"num": 1})
	_, err := query.Run(conn)
	c.Assert(err, test.IsNil)
}
