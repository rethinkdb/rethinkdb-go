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
	row := Expr(5).RunRow(sess)

	var response interface{}
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, 5)
}

func (s *RethinkSuite) TestRowsScanSlice(c *test.C) {
	row := Expr([]interface{}{1, 2, 3, 4, 5}).RunRow(sess)

	var response interface{}
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{1, 2, 3, 4, 5})
}

func (s *RethinkSuite) TestRowsScanMap(c *test.C) {
	row := Expr(map[string]interface{}{
		"id":   2,
		"name": "Object 1",
	}).RunRow(sess)

	var response map[string]interface{}
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"id":   2,
		"name": "Object 1",
	})
}

func (s *RethinkSuite) TestRowsScanMapIntoInterface(c *test.C) {
	row := Expr(map[string]interface{}{
		"id":   2,
		"name": "Object 1",
	}).RunRow(sess)

	var response interface{}
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"id":   2,
		"name": "Object 1",
	})
}

func (s *RethinkSuite) TestRowsScanMapNested(c *test.C) {
	row := Expr(map[string]interface{}{
		"id":   2,
		"name": "Object 1",
		"attr": []interface{}{map[string]interface{}{
			"name":  "attr 1",
			"value": "value 1",
		}},
	}).RunRow(sess)

	var response interface{}
	err := row.Scan(&response)
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
	row := Expr(map[string]interface{}{
		"id":   2,
		"name": "Object 1",
		"Attrs": []interface{}{map[string]interface{}{
			"Name":  "attr 1",
			"Value": "value 1",
		}},
	}).RunRow(sess)

	var response object
	err := row.Scan(&response)
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
	row := Expr("a").RunRow(sess)

	var response string
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "a")
}

func (s *RethinkSuite) TestRowsAtomArray(c *test.C) {
	row := Expr([]interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}).RunRow(sess)

	var response []int
	err := row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.DeepEquals, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 0})
}

func (s *RethinkSuite) TestEmptyResults(c *test.C) {
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("test").Exec(sess)
	row := Db("test").Table("test").Get("missing value").RunRow(sess)
	c.Assert(row.IsNil(), test.Equals, true)

	row = Db("test").Table("test").Get("missing value").RunRow(sess)
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
}
