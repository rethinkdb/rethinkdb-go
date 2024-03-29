// Code generated by gen_tests.py and process_polyglot.py.
// Do not edit this file directly.
// The template for this file is located at:
// ../template.go.tpl
package reql_tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"gopkg.in/rethinkdb/rethinkdb-go.v6/internal/compare"
)

// Tests for the fold term
func TestTransformFoldSuite(t *testing.T) {
	suite.Run(t, new(TransformFoldSuite))
}

type TransformFoldSuite struct {
	suite.Suite

	session *r.Session
}

func (suite *TransformFoldSuite) SetupTest() {
	suite.T().Log("Setting up TransformFoldSuite")
	// Use imports to prevent errors
	_ = time.Time{}
	_ = compare.AnythingIsFine

	session, err := r.Connect(r.ConnectOpts{
		Address: url,
	})
	suite.Require().NoError(err, "Error returned when connecting to server")
	suite.session = session

	r.DBDrop("test").Exec(suite.session)
	err = r.DBCreate("test").Exec(suite.session)
	suite.Require().NoError(err)
	err = r.DB("test").Wait().Exec(suite.session)
	suite.Require().NoError(err)

	r.DB("test").TableDrop("tbl").Exec(suite.session)
	err = r.DB("test").TableCreate("tbl").Exec(suite.session)
	suite.Require().NoError(err)
	err = r.DB("test").Table("tbl").Wait().Exec(suite.session)
	suite.Require().NoError(err)
}

func (suite *TransformFoldSuite) TearDownSuite() {
	suite.T().Log("Tearing down TransformFoldSuite")

	if suite.session != nil {
		r.DB("rethinkdb").Table("_debug_scratch").Delete().Exec(suite.session)
		r.DB("test").TableDrop("tbl").Exec(suite.session)
		r.DBDrop("test").Exec(suite.session)

		suite.session.Close()
	}
}

func (suite *TransformFoldSuite) TestCases() {
	suite.T().Log("Running TransformFoldSuite: Tests for the fold term")

	tbl := r.DB("test").Table("tbl")
	_ = tbl // Prevent any noused variable errors

	{
		// transform/fold.yaml line #6
		/* {'deleted':0,'replaced':0,'unchanged':0,'errors':0,'skipped':0,'inserted':100} */
		var expected_ map[interface{}]interface{} = map[interface{}]interface{}{"deleted": 0, "replaced": 0, "unchanged": 0, "errors": 0, "skipped": 0, "inserted": 100}
		/* tbl.insert(r.range(100).map(lambda i: {'id':i, 'a':i%4}).coerce_to("array")) */

		suite.T().Log("About to run line #6: tbl.Insert(r.Range(100).Map(func(i r.Term) interface{} { return map[interface{}]interface{}{'id': i, 'a': r.Mod(i, 4), }}).CoerceTo('array'))")

		runAndAssert(suite.Suite, expected_, tbl.Insert(r.Range(100).Map(func(i r.Term) interface{} { return map[interface{}]interface{}{"id": i, "a": r.Mod(i, 4)} }).CoerceTo("array")), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #6")
	}

	{
		// transform/fold.yaml line #19
		/* 10 */
		var expected_ int = 10
		/* r.range(0, 10).fold(0, lambda acc, row: acc.add(1)) */

		suite.T().Log("About to run line #19: r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1)})")

		runAndAssert(suite.Suite, expected_, r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1) }), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #19")
	}

	{
		// transform/fold.yaml line #23
		/* 20 */
		var expected_ int = 20
		/* r.range(0, 10).fold(0, lambda acc, row: acc.add(1), final_emit=lambda acc: acc.mul(2)) */

		suite.T().Log("About to run line #23: r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1)}).OptArgs(r.FoldOpts{FinalEmit: func(acc r.Term) interface{} { return acc.Mul(2)}, })")

		runAndAssert(suite.Suite, expected_, r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1) }).OptArgs(r.FoldOpts{FinalEmit: func(acc r.Term) interface{} { return acc.Mul(2) }}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #23")
	}

	{
		// transform/fold.yaml line #27
		/* [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] */
		var expected_ []interface{} = []interface{}{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		/* r.range(0, 10).fold(0, lambda acc, row: acc.add(1), emit=lambda old,row,acc: [row]).coerce_to("array") */

		suite.T().Log("About to run line #27: r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1)}).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{row}}, }).CoerceTo('array')")

		runAndAssert(suite.Suite, expected_, r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1) }).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{row} }}).CoerceTo("array"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #27")
	}

	{
		// transform/fold.yaml line #31
		/* [2, 5, 8, 10] */
		var expected_ []interface{} = []interface{}{2, 5, 8, 10}
		/* r.range(0, 10).fold(0, lambda acc, row: acc.add(1), emit=lambda old,row,acc: r.branch(acc.mod(3).eq(0),[row],[]),final_emit=lambda acc: [acc]).coerce_to("array") */

		suite.T().Log("About to run line #31: r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1)}).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return r.Branch(acc.Mod(3).Eq(0), []interface{}{row}, []interface{}{})}, FinalEmit: func(acc r.Term) interface{} { return []interface{}{acc}}, }).CoerceTo('array')")

		runAndAssert(suite.Suite, expected_, r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1) }).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} {
			return r.Branch(acc.Mod(3).Eq(0), []interface{}{row}, []interface{}{})
		}, FinalEmit: func(acc r.Term) interface{} { return []interface{}{acc} }}).CoerceTo("array"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #31")
	}

	{
		// transform/fold.yaml line #35
		/* [1, 2, 3, 5, 8, 13, 21, 34, 55, 89] */
		var expected_ []interface{} = []interface{}{1, 2, 3, 5, 8, 13, 21, 34, 55, 89}
		/* r.range(0, 10).fold([1, 1], lambda acc, row: [acc[1], acc[0].add(acc[1])], emit=lambda old,row,acc: [acc[0]]).coerce_to("array") */

		suite.T().Log("About to run line #35: r.Range(0, 10).Fold([]interface{}{1, 1}, func(acc r.Term, row r.Term) interface{} { return []interface{}{acc.AtIndex(1), acc.AtIndex(0).Add(acc.AtIndex(1))}}).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{acc.AtIndex(0)}}, }).CoerceTo('array')")

		runAndAssert(suite.Suite, expected_, r.Range(0, 10).Fold([]interface{}{1, 1}, func(acc r.Term, row r.Term) interface{} {
			return []interface{}{acc.AtIndex(1), acc.AtIndex(0).Add(acc.AtIndex(1))}
		}).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{acc.AtIndex(0)} }}).CoerceTo("array"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #35")
	}

	{
		// transform/fold.yaml line #37
		/* "STREAM" */
		var expected_ string = "STREAM"
		/* r.range(0, 10).fold(0, lambda acc, row: acc, emit=lambda old,row,acc: acc).type_of() */

		suite.T().Log("About to run line #37: r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc}).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return acc}, }).TypeOf()")

		runAndAssert(suite.Suite, expected_, r.Range(0, 10).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc }).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return acc }}).TypeOf(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #37")
	}

	{
		// transform/fold.yaml line #39
		/* [{'a': 0, 'id': 20}, {'a': 3, 'id': 15}, {'a': 2, 'id': 46}, {'a': 2, 'id': 78}, {'a': 2, 'id': 90}] */
		var expected_ []interface{} = []interface{}{map[interface{}]interface{}{"a": 0, "id": 20}, map[interface{}]interface{}{"a": 3, "id": 15}, map[interface{}]interface{}{"a": 2, "id": 46}, map[interface{}]interface{}{"a": 2, "id": 78}, map[interface{}]interface{}{"a": 2, "id": 90}}
		/* tbl.filter("id").fold(0, lambda acc, row: acc.add(1), emit=lambda old,row,acc: r.branch(old.mod(20).eq(0),[row],[])).coerce_to("array") */

		suite.T().Log("About to run line #39: tbl.Filter('id').Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1)}).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return r.Branch(old.Mod(20).Eq(0), []interface{}{row}, []interface{}{})}, }).CoerceTo('array')")

		runAndAssert(suite.Suite, expected_, tbl.Filter("id").Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1) }).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} {
			return r.Branch(old.Mod(20).Eq(0), []interface{}{row}, []interface{}{})
		}}).CoerceTo("array"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #39")
	}

	{
		// transform/fold.yaml line #42
		/* [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] */
		var expected_ []interface{} = []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		/* r.range().fold(0, lambda acc, row: acc.add(1), emit=lambda old,row,acc: [acc]).limit(10) */

		suite.T().Log("About to run line #42: r.Range().Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1)}).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{acc}}, }).Limit(10)")

		runAndAssert(suite.Suite, expected_, r.Range().Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1) }).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{acc} }}).Limit(10), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #42")
	}

	{
		// transform/fold.yaml line #45
		/* err("ReqlQueryLogicError", "Cannot use an infinite stream with an aggregation function (`reduce`, `count`, etc.) or coerce it to an array.") */
		var expected_ Err = err("ReqlQueryLogicError", "Cannot use an infinite stream with an aggregation function (`reduce`, `count`, etc.) or coerce it to an array.")
		/* r.range().fold(0, lambda acc, row: acc.add(1), emit=lambda old,row,acc: [acc]).map(lambda doc: 1).reduce(lambda l, r: l+r) */

		suite.T().Log("About to run line #45: r.Range().Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1)}).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{acc}}, }).Map(func(doc r.Term) interface{} { return 1}).Reduce(func(l r.Term, r r.Term) interface{} { return r.Add(l, r)})")

		runAndAssert(suite.Suite, expected_, r.Range().Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1) }).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{acc} }}).Map(func(doc r.Term) interface{} { return 1 }).Reduce(func(l r.Term, r r.Term) interface{} { return r.Add(l, r) }), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #45")
	}

	{
		// transform/fold.yaml line #48
		/* [x for x in range(1, 1001)] */
		var expected_ []interface{} = (func() []interface{} {
			res := []interface{}{}
			for iterator_ := 1; iterator_ < 1001; iterator_++ {
				x := iterator_
				res = append(res, x)
			}
			return res
		}())
		/* r.range(0, 1000).fold(0, lambda acc, row: acc.add(1), emit=lambda old,row,acc: [acc]).coerce_to("array") */

		suite.T().Log("About to run line #48: r.Range(0, 1000).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1)}).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{acc}}, }).CoerceTo('array')")

		runAndAssert(suite.Suite, expected_, r.Range(0, 1000).Fold(0, func(acc r.Term, row r.Term) interface{} { return acc.Add(1) }).OptArgs(r.FoldOpts{Emit: func(old r.Term, row r.Term, acc r.Term) interface{} { return []interface{}{acc} }}).CoerceTo("array"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #48")
	}
}
