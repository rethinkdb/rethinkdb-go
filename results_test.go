package gorethink

import (
	test "launchpad.net/gocheck"
)

type object struct {
	Id    int64  `gorethink:"id,omitempty"`
	Name  string `gorethink:"name"`
	Attrs []attr
}

type attr struct {
	Name  string
	Value interface{}
}

func (s *RethinkSuite) TestRowsScanLiteral(c *test.C) {
	row, err := Expr(5).RunRow(sess)
	c.Assert(err, test.IsNil)

	var response interface{}
	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, 5)
}

func (s *RethinkSuite) TestRowsScanSlice(c *test.C) {
	row, err := Expr([]interface{}{1, 2, 3, 4, 5}).Run(sess)
	c.Assert(err, test.IsNil)

	var response []interface{}
	err = row.ScanAll(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{1, 2, 3, 4, 5})
}

func (s *RethinkSuite) TestRowsPartiallyNilSlice(c *test.C) {
	row, err := Expr(map[string]interface{}{
		"item": []interface{}{
			map[string]interface{}{"num": 1},
			nil,
		},
	}).RunRow(sess)
	c.Assert(err, test.IsNil)

	var response map[string]interface{}
	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"item": []interface{}{
			map[string]interface{}{"num": 1},
			nil,
		},
	})
}

func (s *RethinkSuite) TestRowsScanMap(c *test.C) {
	row, err := Expr(map[string]interface{}{
		"id":   2,
		"name": "Object 1",
	}).RunRow(sess)
	c.Assert(err, test.IsNil)

	var response map[string]interface{}
	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"id":   2,
		"name": "Object 1",
	})
}

func (s *RethinkSuite) TestRowsScanMapIntoInterface(c *test.C) {
	row, err := Expr(map[string]interface{}{
		"id":   2,
		"name": "Object 1",
	}).RunRow(sess)
	c.Assert(err, test.IsNil)

	var response interface{}
	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"id":   2,
		"name": "Object 1",
	})
}

func (s *RethinkSuite) TestRowsScanMapNested(c *test.C) {
	row, err := Expr(map[string]interface{}{
		"id":   2,
		"name": "Object 1",
		"attr": []interface{}{map[string]interface{}{
			"name":  "attr 1",
			"value": "value 1",
		}},
	}).RunRow(sess)
	c.Assert(err, test.IsNil)

	var response interface{}
	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"id":   2,
		"name": "Object 1",
		"attr": []interface{}{map[string]interface{}{
			"name":  "attr 1",
			"value": "value 1",
		}},
	})
}

func (s *RethinkSuite) TestRowsScanStruct(c *test.C) {
	row, err := Expr(map[string]interface{}{
		"id":   2,
		"name": "Object 1",
		"Attrs": []interface{}{map[string]interface{}{
			"Name":  "attr 1",
			"Value": "value 1",
		}},
	}).RunRow(sess)
	c.Assert(err, test.IsNil)

	var response object
	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.DeepEquals, object{
		Id:   2,
		Name: "Object 1",
		Attrs: []attr{attr{
			Name:  "attr 1",
			Value: "value 1",
		}},
	})
}

func (s *RethinkSuite) TestRowsAtomString(c *test.C) {
	row, err := Expr("a").RunRow(sess)
	c.Assert(err, test.IsNil)

	var response string
	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "a")
}

func (s *RethinkSuite) TestRowsAtomArray(c *test.C) {
	row, err := Expr([]interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}).Run(sess)
	c.Assert(err, test.IsNil)

	var response []int
	err = row.ScanAll(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.DeepEquals, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 0})
}

func (s *RethinkSuite) TestEmptyResults(c *test.C) {
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("test").Exec(sess)
	row, err := Db("test").Table("test").Get("missing value").RunRow(sess)
	c.Assert(err, test.IsNil)
	c.Assert(row.IsNil(), test.Equals, true)

	row, err = Db("test").Table("test").Get("missing value").RunRow(sess)
	c.Assert(err, test.IsNil)
	var response interface{}
	row.Scan(response)
	c.Assert(row.IsNil(), test.Equals, true)

	rows, err := Db("test").Table("test").Get("missing value").Run(sess)
	c.Assert(err, test.IsNil)
	rows.Next()
	c.Assert(rows.IsNil(), test.Equals, true)

	rows, err = Db("test").Table("test").GetAll("missing value", "another missing value").Run(sess)
	c.Assert(err, test.IsNil)
	c.Assert(rows.Next(), test.Equals, false)

	var obj object
	obj.Name = "missing value"
	row, err = Db("test").Table("test").Filter(obj).RunRow(sess)
	c.Assert(err, test.IsNil)
	c.Assert(row.IsNil(), test.Equals, true)
}

func (s *RethinkSuite) TestRowsScanAll(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableDrop("Table3").Exec(sess)
	Db("test").TableCreate("Table3").Exec(sess)
	Db("test").Table("Table3").IndexCreate("num").Exec(sess)

	// Insert rows
	Db("test").Table("Table3").Insert([]interface{}{
		map[string]interface{}{
			"id":   2,
			"name": "Object 1",
			"Attrs": []interface{}{map[string]interface{}{
				"Name":  "attr 1",
				"Value": "value 1",
			}},
		},
		map[string]interface{}{
			"id":   3,
			"name": "Object 2",
			"Attrs": []interface{}{map[string]interface{}{
				"Name":  "attr 1",
				"Value": "value 1",
			}},
		},
	}).Exec(sess)

	// Test query
	query := Db("test").Table("Table3").OrderBy("id")
	rows, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	var response []object
	err = rows.ScanAll(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.HasLen, 2)
	c.Assert(response, test.DeepEquals, []object{
		object{
			Id:   2,
			Name: "Object 1",
			Attrs: []attr{attr{
				Name:  "attr 1",
				Value: "value 1",
			}},
		},
		object{
			Id:   3,
			Name: "Object 2",
			Attrs: []attr{attr{
				Name:  "attr 1",
				Value: "value 1",
			}},
		},
	})
}

func (s *RethinkSuite) TestRowsCount(c *test.C) {
	rows, err := Expr([]interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}).Run(sess)
	c.Assert(err, test.IsNil)
	count, _ := rows.Count()
	c.Assert(count, test.Equals, 10)
}
