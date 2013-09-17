package rethinkgo

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestJoinInnerJoin(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(conn)
	Db("test").TableCreate("Join1").Exec(conn)
	Db("test").TableCreate("Join2").Exec(conn)

	// Insert rows
	Db("test").Table("Join1").Insert(joinTable1).Exec(conn)
	Db("test").Table("Join2").Insert(joinTable2).Exec(conn)

	// Test query
	var response interface{}
	query := Db("test").Table("Join1").InnerJoin(Db("test").Table("Join2"), func(a, b RqlTerm) RqlTerm {
		return a.Field("id").Eq(b.Field("id"))
	})
	rows, err := query.Run(conn)
	response, err = rows.All()

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{
			"right": map[string]interface{}{"title": "goof", "id": 0},
			"left":  map[string]interface{}{"name": "bob", "id": 0},
		},
		map[string]interface{}{
			"right": map[string]interface{}{"title": "lmoe", "id": 2},
			"left":  map[string]interface{}{"name": "joe", "id": 2},
		},
	})
}

func (s *RethinkSuite) TestJoinInnerJoinZip(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(conn)
	Db("test").TableCreate("Join1").Exec(conn)
	Db("test").TableCreate("Join2").Exec(conn)

	// Insert rows
	Db("test").Table("Join1").Insert(joinTable1).Exec(conn)
	Db("test").Table("Join2").Insert(joinTable2).Exec(conn)

	// Test query
	var response interface{}
	query := Db("test").Table("Join1").InnerJoin(Db("test").Table("Join2"), func(a, b RqlTerm) RqlTerm {
		return a.Field("id").Eq(b.Field("id"))
	}).Zip()
	rows, err := query.Run(conn)
	response, err = rows.All()

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"title": "goof", "name": "bob", "id": 0},
		map[string]interface{}{"title": "lmoe", "name": "joe", "id": 2},
	})
}

func (s *RethinkSuite) TestJoinOuterJoinZip(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(conn)
	Db("test").TableCreate("Join1").Exec(conn)
	Db("test").TableCreate("Join2").Exec(conn)

	// Insert rows
	Db("test").Table("Join1").Insert(joinTable1).Exec(conn)
	Db("test").Table("Join2").Insert(joinTable2).Exec(conn)

	// Test query
	var response interface{}
	query := Db("test").Table("Join1").OuterJoin(Db("test").Table("Join2"), func(a, b RqlTerm) RqlTerm {
		return a.Field("id").Eq(b.Field("id"))
	}).Zip()
	rows, err := query.Run(conn)
	response, err = rows.All()

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"title": "goof", "name": "bob", "id": 0},
		map[string]interface{}{"name": "tom", "id": 1},
		map[string]interface{}{"title": "lmoe", "name": "joe", "id": 2},
	})
}

func (s *RethinkSuite) TestJoinEqJoinZip(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(conn)
	Db("test").TableCreate("Join1").Exec(conn)
	Db("test").TableCreate("Join2").Exec(conn)

	// Insert rows
	Db("test").Table("Join1").Insert(joinTable1).Exec(conn)
	Db("test").Table("Join2").Insert(joinTable2).Exec(conn)

	// Test query
	var response interface{}
	query := Db("test").Table("Join1").EqJoin("id", Db("test").Table("Join2")).Zip()
	rows, err := query.Run(conn)
	c.Assert(err, test.IsNil)

	response, err = rows.All()
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"title": "goof", "name": "bob", "id": 0},
		map[string]interface{}{"title": "lmoe", "name": "joe", "id": 2},
	})
}

func (s *RethinkSuite) TestJoinEqJoinDiffIdsZip(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(conn)
	Db("test").TableCreate("Join1").Exec(conn)
	Db("test").TableCreate("Join3", "primary_key", "it").Exec(conn)
	Db("test").Table("Join3").IndexCreate("it").Exec(conn)

	// Insert rows
	Db("test").Table("Join1").Delete().Exec(conn)
	Db("test").Table("Join3").Delete().Exec(conn)
	Db("test").Table("Join1").Insert(joinTable1).Exec(conn)
	Db("test").Table("Join3").Insert(joinTable3).Exec(conn)

	// Test query
	var response interface{}
	query := Db("test").Table("Join1").EqJoin("id", Db("test").Table("Join3"), "index", "it").Zip()
	rows, err := query.Run(conn)
	c.Assert(err, test.IsNil)

	response, err = rows.All()
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"title": "goof", "name": "bob", "id": 0, "it": 0},
		map[string]interface{}{"title": "lmoe", "name": "joe", "id": 2, "it": 2},
	})
}
