package rethinkgo

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestManipulationDocField(c *test.C) {
	query := Expr(map[string]interface{}{"a": 1}).Do(Row.Field("a"))

	var response int
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
}

func (s *RethinkSuite) TestManipulationPluck(c *test.C) {
	query := Expr(map[string]interface{}{"a": 1, "b": 2, "c": 3}).Pluck("a", "c")

	var response map[string]interface{}
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"a": 1, "c": 3})
}

func (s *RethinkSuite) TestManipulationWithout(c *test.C) {
	query := Expr(map[string]interface{}{"a": 1, "b": 2, "c": 3}).Pluck("a", "c")

	var response map[string]interface{}
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"a": 1, "c": 3})
}

func (s *RethinkSuite) TestManipulationMerge(c *test.C) {
	query := Expr(map[string]interface{}{"a": 1, "c": 3}).Merge(map[string]interface{}{"b": 2})

	var response map[string]interface{}
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"a": 1, "b": 2, "c": 3})
}

func (s *RethinkSuite) TestManipulationMergeLiteral(c *test.C) {
	query := Expr(map[string]interface{}{
		"a": map[string]interface{}{
			"aa": map[string]interface{}{
				"aaa": 1,
				"aab": 2,
			},
			"ab": map[string]interface{}{
				"aba": 3,
				"abb": 4,
			},
		},
	}).Merge(map[string]interface{}{"a": map[string]interface{}{"ab": Literal()}})

	var response map[string]interface{}
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"a": map[string]interface{}{"aa": map[string]interface{}{"aab": 2, "aaa": 1}}})
}

func (s *RethinkSuite) TestManipulationAppend(c *test.C) {
	query := Expr(List{1, 2, 3}).Append(4).Append(5)

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 2, 3, 4, 5})
}

func (s *RethinkSuite) TestManipulationPrepend(c *test.C) {
	query := Expr(List{3, 4, 5}).Prepend(2).Prepend(1)

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 2, 3, 4, 5})
}

func (s *RethinkSuite) TestManipulationDifference(c *test.C) {
	query := Expr(List{3, 4, 5}).Difference(List{3, 4})

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{5})
}

func (s *RethinkSuite) TestManipulationSetInsert(c *test.C) {
	query := Expr(List{1, 2, 3}).SetInsert(3).SetInsert(4)

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 2, 3, 4})
}

func (s *RethinkSuite) TestManipulationSetUnion(c *test.C) {
	query := Expr(List{1, 2, 3}).SetUnion(List{3, 4})

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 2, 3, 4})
}

func (s *RethinkSuite) TestManipulationSetIntersection(c *test.C) {
	query := Expr(List{1, 2, 3}).SetIntersection(List{2, 3, 3, 4})

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{2, 3})
}

func (s *RethinkSuite) TestManipulationSetDifference(c *test.C) {
	query := Expr(List{1, 2, 3}).SetDifference(List{2, 3, 4, 4})

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1})
}

func (s *RethinkSuite) TestManipulationHasFieldsTrue(c *test.C) {
	query := Expr(map[string]interface{}{"a": 1}).HasFields("a")

	var response bool
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, true)
}

func (s *RethinkSuite) TestManipulationHasFieldsNested(c *test.C) {
	query := Expr(map[string]interface{}{"a": map[string]interface{}{"b": 1}}).HasFields(map[string]interface{}{"a": map[string]interface{}{"b": true}})

	var response bool
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, true)
}

func (s *RethinkSuite) TestManipulationHasFieldsNestedShort(c *test.C) {
	query := Expr(map[string]interface{}{"a": map[string]interface{}{"b": 1}}).HasFields(map[string]interface{}{"a": "b"})

	var response bool
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, true)
}

func (s *RethinkSuite) TestManipulationHasFieldsFalse(c *test.C) {
	query := Expr(map[string]interface{}{"a": 1}).HasFields("b")

	var response bool
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, false)
}

func (s *RethinkSuite) TestManipulationInsertAt(c *test.C) {
	query := Expr(List{1, 2, 3}).InsertAt(1, 1.5)

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 1.5, 2, 3})
}

func (s *RethinkSuite) TestManipulationSpliceAt(c *test.C) {
	query := Expr(List{1, 2, 3}).SpliceAt(1, List{1.25, 1.5, 1.75})

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 1.25, 1.5, 1.75, 2, 3})
}

func (s *RethinkSuite) TestManipulationDeleteAt(c *test.C) {
	query := Expr(List{1, 2, 3}).DeleteAt(1)

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 3})
}

func (s *RethinkSuite) TestManipulationDeleteAtRange(c *test.C) {
	query := Expr(List{1, 2, 3, 4}).DeleteAtRange(1, 3)

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 4})
}

func (s *RethinkSuite) TestManipulationChangeAt(c *test.C) {
	query := Expr(List{1, 5, 3, 4}).ChangeAt(1, 2)

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{1, 2, 3, 4})
}

func (s *RethinkSuite) TestManipulationKeys(c *test.C) {
	query := Expr(map[string]interface{}{"a": 1, "b": 2, "c": 3}).Keys()

	var response List
	err := query.RunRow(conn).Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, List{"a", "b", "c"})
}
