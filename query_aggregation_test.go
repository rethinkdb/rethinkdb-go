package gorethink

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestAggregationReduce(c *test.C) {
	var response int
	query := Expr(arr).Reduce(func(acc, val RqlTerm) RqlTerm {
		return acc.Add(val)
	}, 0)
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 45)
}

func (s *RethinkSuite) TestAggregationExprCount(c *test.C) {
	var response int
	query := Expr(arr).Count()
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 9)
}

func (s *RethinkSuite) TestAggregationDistinct(c *test.C) {
	var response []int
	query := Expr(darr).Distinct()
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.HasLen, 5)
}

func (s *RethinkSuite) TestAggregationGroupedMapReduce(c *test.C) {
	var response []interface{}
	query := Expr(objList).GroupedMapReduce(
		func(row RqlTerm) RqlTerm {
			return row.Field("id").Mod(2).Eq(0)
		},
		func(row RqlTerm) RqlTerm {
			return row.Field("num")
		},
		func(acc, num RqlTerm) RqlTerm {
			return acc.Add(num)
		},
		0,
	)
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"reduction": 135, "group": false},
		map[string]interface{}{"reduction": 70, "group": true},
	})
}

func (s *RethinkSuite) TestAggregationGroupedMapReduceTable(c *test.C) {
	// Ensure table + database exist
	DbCreate("test").Exec(sess)
	Db("test").TableCreate("TestAggregationGroupedMapReduceTable").Exec(sess)

	// Insert rows
	err := Db("test").Table("TestAggregationGroupedMapReduceTable").Insert(objList).Exec(sess)
	c.Assert(err, test.IsNil)

	var response []interface{}
	query := Db("test").Table("TestAggregationGroupedMapReduceTable").GroupedMapReduce(
		func(row RqlTerm) RqlTerm {
			return row.Field("id").Mod(2).Eq(0)
		},
		func(row RqlTerm) RqlTerm {
			return row.Field("num")
		},
		func(acc, num RqlTerm) RqlTerm {
			return acc.Add(num)
		},
		0,
	)
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"reduction": 135, "group": false},
		map[string]interface{}{"reduction": 70, "group": true},
	})
}

func (s *RethinkSuite) TestAggregationGroupByCount(c *test.C) {
	var response []interface{}
	query := Expr(objList).GroupBy(Count(), "g1")
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"reduction": 3, "group": map[string]interface{}{"g1": 1}},
		map[string]interface{}{"reduction": 4, "group": map[string]interface{}{"g1": 2}},
		map[string]interface{}{"reduction": 1, "group": map[string]interface{}{"g1": 3}},
		map[string]interface{}{"reduction": 1, "group": map[string]interface{}{"g1": 4}},
	})
}

func (s *RethinkSuite) TestAggregationGroupBySum(c *test.C) {
	var response []interface{}
	query := Expr(objList).GroupBy(Sum("num"), "g1")
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"reduction": 15, "group": map[string]interface{}{"g1": 1}},
		map[string]interface{}{"reduction": 130, "group": map[string]interface{}{"g1": 2}},
		map[string]interface{}{"reduction": 10, "group": map[string]interface{}{"g1": 3}},
		map[string]interface{}{"reduction": 50, "group": map[string]interface{}{"g1": 4}},
	})
}

func (s *RethinkSuite) TestAggregationGroupByAvg(c *test.C) {
	var response []interface{}
	query := Expr(objList).GroupBy(Avg("num"), "g1")
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"reduction": 5, "group": map[string]interface{}{"g1": 1}},
		map[string]interface{}{"reduction": 32.5, "group": map[string]interface{}{"g1": 2}},
		map[string]interface{}{"reduction": 10, "group": map[string]interface{}{"g1": 3}},
		map[string]interface{}{"reduction": 50, "group": map[string]interface{}{"g1": 4}},
	})
}

func (s *RethinkSuite) TestAggregationGroupBySumMultipleSelectors(c *test.C) {
	var response []interface{}
	query := Expr(objList).GroupBy(Sum("num"), "g1", "g2")
	r, err := query.Run(sess)
	c.Assert(err, test.IsNil)

	err = r.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		map[string]interface{}{"reduction": 15, "group": map[string]interface{}{"g2": 1, "g1": 1}},
		map[string]interface{}{"reduction": 0, "group": map[string]interface{}{"g2": 2, "g1": 1}},
		map[string]interface{}{"reduction": 5, "group": map[string]interface{}{"g2": 2, "g1": 2}},
		map[string]interface{}{"reduction": 125, "group": map[string]interface{}{"g2": 3, "g1": 2}},
		map[string]interface{}{"reduction": 10, "group": map[string]interface{}{"g2": 2, "g1": 3}},
		map[string]interface{}{"reduction": 50, "group": map[string]interface{}{"g2": 2, "g1": 4}},
	})
}

func (s *RethinkSuite) TestAggregationContains(c *test.C) {
	var response interface{}
	query := Expr(arr).Contains(2)
	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, true)
}
