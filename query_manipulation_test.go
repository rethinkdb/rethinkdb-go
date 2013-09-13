package rethinkgo

import (
	"fmt"
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestManipulationRowField(c *test.C) {
	// query := Expr(Obj{"a": 1}).Map(r.Row().Field("a"))
	// fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationPluck(c *test.C) {
	query := Expr(Obj{"a": 1, "b": 2, "c": 3}).Pluck("a", "c")
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationWithout(c *test.C) {
	query := Expr(Obj{"a": 1, "b": 2, "c": 3}).Pluck("a", "c")
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationMerge(c *test.C) {
	query := Expr(Obj{"a": 1, "c": 3}).Merge(Obj{"b": 2})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationMergeLiteral(c *test.C) {
	query := Expr(Obj{
		"a": Obj{
			"aa": Obj{
				"aaa": 1,
				"aab": 2,
			},
			"ab": Obj{
				"aba": 3,
				"abb": 4,
			},
		},
	}).Merge(Obj{"a": Obj{"ab": Literal()}})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationAppend(c *test.C) {
	query := Expr(List{1, 2, 3}).Append(4).Append(5)
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationPrepend(c *test.C) {
	query := Expr(List{3, 4, 5}).Append(1).Append(2)
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationDifference(c *test.C) {
	query := Expr(List{3, 4, 5}).Difference(List{3, 4})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationSetInsert(c *test.C) {
	query := Expr(List{1, 2, 3}).SetInsert(3).SetInsert(4)
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationSetUnion(c *test.C) {
	query := Expr(List{1, 2, 3}).SetUnion(List{3, 4})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationSetIntersection(c *test.C) {
	query := Expr(List{1, 2, 3}).SetIntersection(List{2, 3, 3, 4})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationSetDifference(c *test.C) {
	query := Expr(List{1, 2, 3}).SetDifference(List{2, 3, 4, 4})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationHasFieldsTrue(c *test.C) {
	query := Expr(Obj{"a": 1}).HasFields("a")
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationHasFieldsFalse(c *test.C) {
	query := Expr(Obj{"a": 1}).HasFields("b")
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationInsertAt(c *test.C) {
	query := Expr(List{1, 2, 3}).InsertAt(1, 1.5)
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationSpliceAt(c *test.C) {
	query := Expr(List{1, 2, 3}).SpliceAt(1, List{1.25, 1.5, 1.75})
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationDeleteAt(c *test.C) {
	query := Expr(List{1, 2, 3}).DeleteAt(1)
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationDeleteAtRange(c *test.C) {
	query := Expr(List{1, 2, 3, 4}).SpliceAt(1, 2)
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationChangeAt(c *test.C) {
	query := Expr(List{1, 5, 3, 4}).SpliceAt(1, 2)
	fmt.Println(query.String())
}

func (s *RethinkSuite) TestManipulationKeys(c *test.C) {
	query := Expr(Obj{"a": 1, "b": 2, "c": 3}).Keys()
	fmt.Println(query.String())
}
