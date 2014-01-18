package gorethink

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestSelectGet(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table1").Exec(sess)

	// Insert rows
	Db("test").Table("Table1").Insert(objList).Exec(sess)

	// Test query
	var response interface{}
	query := Db("test").Table("Table1").Get(6)
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"id": 6, "g1": 1, "g2": 1, "num": 15})
}

func (s *RethinkSuite) TestSelectGetAll(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table1").Exec(sess)
	Db("test").Table("Table1").IndexCreate("num").Exec(sess)

	// Insert rows
	Db("test").Table("Table1").Insert(objList).Exec(sess)

	// Test query
	var response []interface{}
	query := Db("test").Table("Table1").GetAll(6).OrderBy("id")
	rows, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = rows.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"num": 15, "id": 6, "g2": 1, "g1": 1},
	})
}

func (s *RethinkSuite) TestSelectGetAllMultiple(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table1").Exec(sess)
	Db("test").Table("Table1").IndexCreate("num").Exec(sess)

	// Insert rows
	Db("test").Table("Table1").Insert(objList).Exec(sess)

	// Test query
	var response []interface{}
	query := Db("test").Table("Table1").GetAll(1, 2, 3).OrderBy("id")
	rows, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = rows.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"num": 0, "id": 1, "g2": 1, "g1": 1},
		map[string]interface{}{"num": 5, "id": 2, "g2": 2, "g1": 2},
		map[string]interface{}{"num": 10, "id": 3, "g2": 2, "g1": 3},
	})
}

func (s *RethinkSuite) TestSelectGetAllByIndex(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table1").Exec(sess)
	Db("test").Table("Table1").IndexCreate("num").Exec(sess)

	// Insert rows
	Db("test").Table("Table1").Insert(objList).Exec(sess)

	// Test query
	var response interface{}
	query := Db("test").Table("Table1").GetAllByIndex("num", 15).OrderBy("id")
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"id": 6, "g1": 1, "g2": 1, "num": 15})
}

func (s *RethinkSuite) TestSelectGetAllMultipleByIndex(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table2").Exec(sess)
	Db("test").Table("Table2").IndexCreate("num").Exec(sess)

	// Insert rows
	Db("test").Table("Table2").Insert(objList).Exec(sess)

	// Test query
	var response interface{}
	query := Db("test").Table("Table2").GetAllByIndex("num", 15).OrderBy("id")
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"id": 6, "g1": 1, "g2": 1, "num": 15})
}

func (s *RethinkSuite) TestSelectGetAllCompoundIndex(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableDrop("TableCompound").Exec(sess)
	Db("test").TableCreate("TableCompound").Exec(sess)
	write, err := Db("test").Table("TableCompound").IndexCreateFunc("full_name", func(row RqlTerm) interface{} {
		return []interface{}{row.Field("first_name"), row.Field("last_name")}
	}).RunWrite(sess)
	c.Assert(err, test.IsNil)
	c.Assert(write.Created, test.Equals, 1)

	// Insert rows
	Db("test").Table("TableCompound").Insert(nameList).Exec(sess)

	// Test query
	var response interface{}
	query := Db("test").Table("TableCompound").GetAllByIndex("full_name", []interface{}{"John", "Smith"})
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"id": 1, "first_name": "John", "last_name": "Smith", "gender": "M"})
}

func (s *RethinkSuite) TestSelectBetween(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table1").Exec(sess)

	// Insert rows
	Db("test").Table("Table1").Insert(objList).Exec(sess)

	// Test query
	var response []interface{}
	query := Db("test").Table("Table1").Between(1, 3).OrderBy("id")
	rows, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = rows.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"num": 0, "id": 1, "g2": 1, "g1": 1},
		map[string]interface{}{"num": 5, "id": 2, "g2": 2, "g1": 2},
	})
}

func (s *RethinkSuite) TestSelectBetweenWithIndex(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table2").Exec(sess)
	Db("test").Table("Table2").IndexCreate("num").Exec(sess)

	// Insert rows
	Db("test").Table("Table2").Insert(objList).Exec(sess)

	// Test query
	var response []interface{}
	query := Db("test").Table("Table2").Between(10, 50, BetweenOpts{
		Index: "num",
	}).OrderBy("id")
	rows, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = rows.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"num": 10, "id": 3, "g2": 2, "g1": 3},
		map[string]interface{}{"num": 15, "id": 6, "g2": 1, "g1": 1},
		map[string]interface{}{"num": 25, "id": 9, "g2": 3, "g1": 2},
	})
}

func (s *RethinkSuite) TestSelectBetweenWithOptions(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table2").Exec(sess)
	Db("test").Table("Table2").IndexCreate("num").Exec(sess)

	// Insert rows
	Db("test").Table("Table2").Insert(objList).Exec(sess)

	// Test query
	var response []interface{}
	query := Db("test").Table("Table2").Between(10, 50, BetweenOpts{
		Index:      "num",
		RightBound: "closed",
	}).OrderBy("id")
	rows, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = rows.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"num": 10, "id": 3, "g2": 2, "g1": 3},
		map[string]interface{}{"num": 15, "id": 6, "g2": 1, "g1": 1},
		map[string]interface{}{"num": 50, "id": 8, "g2": 2, "g1": 4},
		map[string]interface{}{"num": 25, "id": 9, "g2": 3, "g1": 2},
	})
}

func (s *RethinkSuite) TestSelectFilterImplicit(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table1").Exec(sess)

	// Insert rows
	Db("test").Table("Table1").Insert(objList).Exec(sess)

	// Test query
	var response []interface{}
	query := Db("test").Table("Table1").Filter(Row.Field("num").Ge(50)).OrderBy("id")
	rows, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = rows.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"num": 100, "id": 5, "g2": 3, "g1": 2},
		map[string]interface{}{"num": 50, "id": 8, "g2": 2, "g1": 4},
	})
}

func (s *RethinkSuite) TestSelectFilterFunc(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("Table1").Exec(sess)

	// Insert rows
	Db("test").Table("Table1").Insert(objList).Exec(sess)

	// Test query
	var response []interface{}
	query := Db("test").Table("Table1").Filter(func(row RqlTerm) RqlTerm {
		return row.Field("num").Ge(50)
	}).OrderBy("id")
	rows, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = rows.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"num": 100, "id": 5, "g2": 3, "g1": 2},
		map[string]interface{}{"num": 50, "id": 8, "g2": 2, "g1": 4},
	})
}
