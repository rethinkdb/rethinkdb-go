package reql_tests

import (
	"github.com/stretchr/testify/suite"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"gopkg.in/rethinkdb/rethinkdb-go.v6/internal/compare"
	"testing"
)

// Test edge cases of write hook
func TestWriteHookSuite(t *testing.T) {
	suite.Run(t, new(WriteHook))
}

type WriteHook struct {
	suite.Suite

	session *r.Session
}

func (suite *WriteHook) SetupTest() {
	suite.T().Log("Setting up WriteHook")

	session, err := r.Connect(r.ConnectOpts{
		Address: url,
	})
	suite.Require().NoError(err, "Error returned when connecting to server")
	suite.session = session

	r.DBDrop("db_hook").Exec(suite.session)
	err = r.DBCreate("db_hook").Exec(suite.session)
	suite.Require().NoError(err)
	err = r.DB("db_hook").Wait().Exec(suite.session)
	suite.Require().NoError(err)

	r.DB("db_hook").TableDrop("test").Exec(suite.session)
	err = r.DB("db_hook").TableCreate("test").Exec(suite.session)
	suite.Require().NoError(err)
	err = r.DB("db_hook").Table("test").Wait().Exec(suite.session)
	suite.Require().NoError(err)
}

func (suite *WriteHook) TearDownSuite() {
	suite.T().Log("Tearing down WriteHook")

	if suite.session != nil {
		err := r.DBDrop("db_hook").Exec(suite.session)
		suite.Require().NoError(err)

		suite.session.Close()
	}
}

func (suite *WriteHook) TestCases() {
	suite.T().Log("Running WriteHook: Test edge cases of write hooks")

	runOpts := r.RunOpts{
		GeometryFormat: "raw",
		GroupFormat:    "map",
	}

	table := r.DB("db_hook").Table("test")
	wcField := "write_counter"

	{
		var q = table.SetWriteHook(func(id r.Term, oldVal r.Term, newVal r.Term) r.Term {
			return newVal
		})
		var expected_ = compare.PartialMatch(map[string]interface{}{"created": 1})

		suite.T().Logf("About to run line #1: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #1")
	}

	{
		var q = table.SetWriteHook(nil)
		var expected_ = compare.PartialMatch(map[string]interface{}{"deleted": 1})

		suite.T().Logf("About to run line #2: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #2")
	}

	{
		var q = table.SetWriteHook(func(id r.Term, oldVal r.Term, newVal r.Term) r.Term {
			return newVal
		})
		var expected_ = compare.PartialMatch(map[string]interface{}{"created": 1})

		suite.T().Logf("About to run line #3: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #3")
	}

	{
		var q = table.SetWriteHook(func(id r.Term, oldVal r.Term, newVal r.Term) r.Term {
			return r.Branch(oldVal.And(newVal),
				newVal.Merge(map[string]r.Term{wcField: oldVal.Field(wcField).Add(1)}),
				newVal,
				newVal.Merge(r.Expr(map[string]int{wcField: 1})),
				r.Error("no delete"),
			)
		})
		var expected_ = compare.PartialMatch(map[string]interface{}{"replaced": 1})

		suite.T().Logf("About to run line #4: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #4")
	}

	{
		var q = table.Insert(map[string]interface{}{"id": 1}, r.InsertOpts{ReturnChanges: true})
		var expected_ = compare.PartialMatch(map[string]interface{}{
			"changes": []interface{}{
				map[string]interface{}{
					"new_val": map[string]interface{}{"id": 1, wcField: 1},
					"old_val": interface{}(nil),
				}},
			"inserted": 1,
		})

		suite.T().Logf("About to run line #5: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #5")
	}

	{
		var q = table.Replace(map[string]interface{}{"id": 1, "text": "abc"}, r.ReplaceOpts{ReturnChanges: true})
		var expected_ = compare.PartialMatch(map[string]interface{}{
			"changes": []interface{}{
				map[string]interface{}{
					"new_val": map[string]interface{}{"id": 1, wcField: 2, "text": "abc"},
					"old_val": map[string]interface{}{"id": 1, wcField: 1},
				}},
			"replaced": 1,
		})

		suite.T().Logf("About to run line #6: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #6")
	}

	{
		var q = table.Get(1).Update(map[string]interface{}{"text": "def"}, r.UpdateOpts{ReturnChanges: true})
		var expected_ = compare.PartialMatch(map[string]interface{}{
			"changes": []interface{}{
				map[string]interface{}{
					"new_val": map[string]interface{}{"id": 1, wcField: 3, "text": "def"},
					"old_val": map[string]interface{}{"id": 1, wcField: 2, "text": "abc"},
				}},
			"replaced": 1,
		})

		suite.T().Logf("About to run line #7: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #7")
	}

	{
		var q = table.Get(1).Delete()
		var expected_ = compare.PartialMatch(map[string]interface{}{
			"first_error": "Error in write hook: no delete",
			"errors":      1,
		})

		suite.T().Logf("About to run line #8: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #8")
	}

	{
		var q = table.Insert(map[string]interface{}{"id": 2}, r.InsertOpts{ReturnChanges: true, IgnoreWriteHook: true})
		var expected_ = compare.PartialMatch(map[string]interface{}{
			"changes": []interface{}{
				map[string]interface{}{
					"new_val": map[string]interface{}{"id": 2},
					"old_val": interface{}(nil),
				}},
			"inserted": 1,
		})

		suite.T().Logf("About to run line #9: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #9")
	}

	{
		var q = table.Replace(map[string]interface{}{"id": 2, "text": "abc"}, r.ReplaceOpts{ReturnChanges: true, IgnoreWriteHook: true})
		var expected_ = compare.PartialMatch(map[string]interface{}{
			"changes": []interface{}{
				map[string]interface{}{
					"new_val": map[string]interface{}{"id": 2, "text": "abc"},
					"old_val": map[string]interface{}{"id": 2},
				}},
			"replaced": 1,
		})

		suite.T().Logf("About to run line #10: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #10")
	}

	{
		var q = table.Get(2).Update(map[string]interface{}{"text": "def"}, r.UpdateOpts{ReturnChanges: true, IgnoreWriteHook: true})
		var expected_ = compare.PartialMatch(map[string]interface{}{
			"changes": []interface{}{
				map[string]interface{}{
					"new_val": map[string]interface{}{"id": 2, "text": "def"},
					"old_val": map[string]interface{}{"id": 2, "text": "abc"},
				}},
			"replaced": 1,
		})

		suite.T().Logf("About to run line #11: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #11")
	}

	{
		var q = table.Get(2).Delete(r.DeleteOpts{IgnoreWriteHook: true})
		var expected_ = compare.PartialMatch(map[string]interface{}{"deleted": 1})

		suite.T().Logf("About to run line #12: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #12")
	}
}
