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

// Test the arity of every function
func TestAritySuite(t *testing.T) {
	suite.Run(t, new(AritySuite))
}

type AritySuite struct {
	suite.Suite

	session *r.Session
}

func (suite *AritySuite) SetupTest() {
	suite.T().Log("Setting up AritySuite")
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

func (suite *AritySuite) TearDownSuite() {
	suite.T().Log("Tearing down AritySuite")

	if suite.session != nil {
		r.DB("rethinkdb").Table("_debug_scratch").Delete().Exec(suite.session)
		r.DB("test").TableDrop("tbl").Exec(suite.session)
		r.DBDrop("test").Exec(suite.session)

		suite.session.Close()
	}
}

func (suite *AritySuite) TestCases() {
	suite.T().Log("Running AritySuite: Test the arity of every function")

	tbl := r.DB("test").Table("tbl")
	_ = tbl // Prevent any noused variable errors

	// arity.yaml line #8
	// db = r.db('test')
	suite.T().Log("Possibly executing: var db r.Term = r.DB('test')")

	db := r.DB("test")
	_ = db // Prevent any noused variable errors

	// arity.yaml line #9
	// obj = r.expr({'a':1})
	suite.T().Log("Possibly executing: var obj r.Term = r.Expr(map[interface{}]interface{}{'a': 1, })")

	obj := r.Expr(map[interface{}]interface{}{"a": 1})
	_ = obj // Prevent any noused variable errors

	// arity.yaml line #10
	// array = r.expr([1])
	suite.T().Log("Possibly executing: var array r.Term = r.Expr([]interface{}{1})")

	array := r.Expr([]interface{}{1})
	_ = array // Prevent any noused variable errors

	{
		// arity.yaml line #43
		/* err("ReqlQueryLogicError", "Empty ERROR term outside a default block.", []) */
		var expected_ Err = err("ReqlQueryLogicError", "Empty ERROR term outside a default block.")
		/* r.error() */

		suite.T().Log("About to run line #43: r.Error()")

		runAndAssert(suite.Suite, expected_, r.Error(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #43")
	}

	{
		// arity.yaml line #96
		/* err("ReqlQueryLogicError", "Expected type DATUM but found DATABASE:", []) */
		var expected_ Err = err("ReqlQueryLogicError", "Expected type DATUM but found DATABASE:")
		/* db.table_drop() */

		suite.T().Log("About to run line #96: db.TableDrop()")

		runAndAssert(suite.Suite, expected_, db.TableDrop(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #96")
	}

	{
		// arity.yaml line #209
		/* err("ReqlQueryLogicError", "Cannot call `branch` term with an even number of arguments.", []) */
		var expected_ Err = err("ReqlQueryLogicError", "Cannot call `branch` term with an even number of arguments.")
		/* r.branch(1,2,3,4) */

		suite.T().Log("About to run line #209: r.Branch(1, 2, 3, 4)")

		runAndAssert(suite.Suite, expected_, r.Branch(1, 2, 3, 4), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #209")
	}

	{
		// arity.yaml line #220
		/* 10 */
		var expected_ int = 10
		/* tbl.insert([{'id':0},{'id':1},{'id':2},{'id':3},{'id':4},{'id':5},{'id':6},{'id':7},{'id':8},{'id':9}]).get_field('inserted') */

		suite.T().Log("About to run line #220: tbl.Insert([]interface{}{map[interface{}]interface{}{'id': 0, }, map[interface{}]interface{}{'id': 1, }, map[interface{}]interface{}{'id': 2, }, map[interface{}]interface{}{'id': 3, }, map[interface{}]interface{}{'id': 4, }, map[interface{}]interface{}{'id': 5, }, map[interface{}]interface{}{'id': 6, }, map[interface{}]interface{}{'id': 7, }, map[interface{}]interface{}{'id': 8, }, map[interface{}]interface{}{'id': 9, }}).Field('inserted')")

		runAndAssert(suite.Suite, expected_, tbl.Insert([]interface{}{map[interface{}]interface{}{"id": 0}, map[interface{}]interface{}{"id": 1}, map[interface{}]interface{}{"id": 2}, map[interface{}]interface{}{"id": 3}, map[interface{}]interface{}{"id": 4}, map[interface{}]interface{}{"id": 5}, map[interface{}]interface{}{"id": 6}, map[interface{}]interface{}{"id": 7}, map[interface{}]interface{}{"id": 8}, map[interface{}]interface{}{"id": 9}}).Field("inserted"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #220")
	}

	{
		// arity.yaml line #223
		/* bag([0, 1, 2]) */
		var expected_ compare.Expected = compare.UnorderedMatch([]interface{}{0, 1, 2})
		/* tbl.get_all(0, 1, 2).get_field('id') */

		suite.T().Log("About to run line #223: tbl.GetAll(0, 1, 2).Field('id')")

		runAndAssert(suite.Suite, expected_, tbl.GetAll(0, 1, 2).Field("id"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #223")
	}

	{
		// arity.yaml line #226
		/* bag([0, 1, 2]) */
		var expected_ compare.Expected = compare.UnorderedMatch([]interface{}{0, 1, 2})
		/* tbl.get_all(r.args([]), 0, 1, 2).get_field('id') */

		suite.T().Log("About to run line #226: tbl.GetAll(r.Args([]interface{}{}), 0, 1, 2).Field('id')")

		runAndAssert(suite.Suite, expected_, tbl.GetAll(r.Args([]interface{}{}), 0, 1, 2).Field("id"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #226")
	}

	{
		// arity.yaml line #229
		/* bag([0, 1, 2]) */
		var expected_ compare.Expected = compare.UnorderedMatch([]interface{}{0, 1, 2})
		/* tbl.get_all(r.args([0]), 1, 2).get_field('id') */

		suite.T().Log("About to run line #229: tbl.GetAll(r.Args([]interface{}{0}), 1, 2).Field('id')")

		runAndAssert(suite.Suite, expected_, tbl.GetAll(r.Args([]interface{}{0}), 1, 2).Field("id"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #229")
	}

	{
		// arity.yaml line #232
		/* bag([0, 1, 2]) */
		var expected_ compare.Expected = compare.UnorderedMatch([]interface{}{0, 1, 2})
		/* tbl.get_all(r.args([0, 1]), 2).get_field('id') */

		suite.T().Log("About to run line #232: tbl.GetAll(r.Args([]interface{}{0, 1}), 2).Field('id')")

		runAndAssert(suite.Suite, expected_, tbl.GetAll(r.Args([]interface{}{0, 1}), 2).Field("id"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #232")
	}

	{
		// arity.yaml line #235
		/* bag([0, 1, 2]) */
		var expected_ compare.Expected = compare.UnorderedMatch([]interface{}{0, 1, 2})
		/* tbl.get_all(r.args([0, 1, 2])).get_field('id') */

		suite.T().Log("About to run line #235: tbl.GetAll(r.Args([]interface{}{0, 1, 2})).Field('id')")

		runAndAssert(suite.Suite, expected_, tbl.GetAll(r.Args([]interface{}{0, 1, 2})).Field("id"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #235")
	}

	{
		// arity.yaml line #238
		/* bag([0, 1, 2]) */
		var expected_ compare.Expected = compare.UnorderedMatch([]interface{}{0, 1, 2})
		/* tbl.get_all(r.args([0]), 1, r.args([2])).get_field('id') */

		suite.T().Log("About to run line #238: tbl.GetAll(r.Args([]interface{}{0}), 1, r.Args([]interface{}{2})).Field('id')")

		runAndAssert(suite.Suite, expected_, tbl.GetAll(r.Args([]interface{}{0}), 1, r.Args([]interface{}{2})).Field("id"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #238")
	}

	{
		// arity.yaml line #243
		/* 1 */
		var expected_ int = 1
		/* r.branch(true, 1, r.error("a")) */

		suite.T().Log("About to run line #243: r.Branch(true, 1, r.Error('a'))")

		runAndAssert(suite.Suite, expected_, r.Branch(true, 1, r.Error("a")), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #243")
	}

	{
		// arity.yaml line #246
		/* 1 */
		var expected_ int = 1
		/* r.branch(r.args([true, 1]), r.error("a")) */

		suite.T().Log("About to run line #246: r.Branch(r.Args([]interface{}{true, 1}), r.Error('a'))")

		runAndAssert(suite.Suite, expected_, r.Branch(r.Args([]interface{}{true, 1}), r.Error("a")), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #246")
	}

	{
		// arity.yaml line #249
		/* 1 */
		var expected_ int = 1
		/* r.expr(true).branch(1, 2) */

		suite.T().Log("About to run line #249: r.Expr(true).Branch(1, 2)")

		runAndAssert(suite.Suite, expected_, r.Expr(true).Branch(1, 2), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #249")
	}

	{
		// arity.yaml line #252
		/* err("ReqlUserError", "a", []) */
		var expected_ Err = err("ReqlUserError", "a")
		/* r.branch(r.args([true, 1, r.error("a")])) */

		suite.T().Log("About to run line #252: r.Branch(r.Args([]interface{}{true, 1, r.Error('a')}))")

		runAndAssert(suite.Suite, expected_, r.Branch(r.Args([]interface{}{true, 1, r.Error("a")})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #252")
	}

	{
		// arity.yaml line #258
		/* ([{'group':0, 'reduction':1}]) */
		var expected_ []interface{} = []interface{}{map[interface{}]interface{}{"group": 0, "reduction": 1}}
		/* tbl.group(lambda row:row['id'].mod(2)).count({'id':0}).ungroup() */

		suite.T().Log("About to run line #258: tbl.Group(func(row r.Term) interface{} { return row.AtIndex('id').Mod(2)}).Count(map[interface{}]interface{}{'id': 0, }).Ungroup()")

		runAndAssert(suite.Suite, expected_, tbl.Group(func(row r.Term) interface{} { return row.AtIndex("id").Mod(2) }).Count(map[interface{}]interface{}{"id": 0}).Ungroup(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #258")
	}

	{
		// arity.yaml line #263
		/* ([{'group':0, 'reduction':1}]) */
		var expected_ []interface{} = []interface{}{map[interface{}]interface{}{"group": 0, "reduction": 1}}
		/* tbl.group(r.row['id'].mod(2)).count(r.args([{'id':0}])).ungroup() */

		suite.T().Log("About to run line #263: tbl.Group(r.Row.AtIndex('id').Mod(2)).Count(r.Args([]interface{}{map[interface{}]interface{}{'id': 0, }})).Ungroup()")

		runAndAssert(suite.Suite, expected_, tbl.Group(r.Row.AtIndex("id").Mod(2)).Count(r.Args([]interface{}{map[interface{}]interface{}{"id": 0}})).Ungroup(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #263")
	}

	{
		// arity.yaml line #269
		/* ({'a':{'c':1}}) */
		var expected_ map[interface{}]interface{} = map[interface{}]interface{}{"a": map[interface{}]interface{}{"c": 1}}
		/* r.expr({'a':{'b':1}}).merge(r.args([{'a':r.literal({'c':1})}])) */

		suite.T().Log("About to run line #269: r.Expr(map[interface{}]interface{}{'a': map[interface{}]interface{}{'b': 1, }, }).Merge(r.Args([]interface{}{map[interface{}]interface{}{'a': r.Literal(map[interface{}]interface{}{'c': 1, }), }}))")

		runAndAssert(suite.Suite, expected_, r.Expr(map[interface{}]interface{}{"a": map[interface{}]interface{}{"b": 1}}).Merge(r.Args([]interface{}{map[interface{}]interface{}{"a": r.Literal(map[interface{}]interface{}{"c": 1})}})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #269")
	}
}
