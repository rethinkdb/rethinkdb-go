package rethinkgo

import (
	"fmt"
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestWriteInsert(c *test.C) {
	query := Db("test").Table("test").Insert(Obj{"num": 1})
	_, err := query.Run(conn)
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TestWriteInsertStruct(c *test.C) {
	o := object{
		Id:   5,
		Name: "Object 3",
		Attrs: []attr{
			attr{
				Name:  "Attr 2",
				Value: "Value",
			},
		},
	}

	query := Db("test").Table("test").Insert(o)
	// query.Exec(conn)
	fmt.Println(query)
}

func (s *RethinkSuite) TestWriteUpdate(c *test.C) {
	query := Db("test").Table("test").Insert(Obj{"num": 1})
	_, err := query.Run(conn)
	c.Assert(err, test.IsNil)

	// Update the first row in the table
	query = Db("test").Table("test").Sample(1).Update(Obj{"num": 2})
	_, err = query.Run(conn)
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TestWriteReplace(c *test.C) {
	query := Db("test").Table("test").Insert(Obj{"num": 1})
	_, err := query.Run(conn)
	c.Assert(err, test.IsNil)

	// Replace the first row in the table
	query = Db("test").Table("test").Sample(1).Update(Obj{"num": 2})
	_, err = query.Run(conn)
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TestWriteDelete(c *test.C) {
	query := Db("test").Table("test").Insert(Obj{"num": 1})
	_, err := query.Run(conn)
	c.Assert(err, test.IsNil)

	// Delete the first row in the table
	query = Db("test").Table("test").Sample(1).Delete()
	_, err = query.Run(conn)
	c.Assert(err, test.IsNil)
}
