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

// Test the native shims.
func TestTimesShimSuite(t *testing.T) {
	suite.Run(t, new(TimesShimSuite))
}

type TimesShimSuite struct {
	suite.Suite

	session *r.Session
}

func (suite *TimesShimSuite) SetupTest() {
	suite.T().Log("Setting up TimesShimSuite")
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

}

func (suite *TimesShimSuite) TearDownSuite() {
	suite.T().Log("Tearing down TimesShimSuite")

	if suite.session != nil {
		r.DB("rethinkdb").Table("_debug_scratch").Delete().Exec(suite.session)
		r.DBDrop("test").Exec(suite.session)

		suite.session.Close()
	}
}

func (suite *TimesShimSuite) TestCases() {
	suite.T().Log("Running TimesShimSuite: Test the native shims.")

	// times/shim.yaml line #4
	// t = 1375147296.68
	suite.T().Log("Possibly executing: var t float64 = 1375147296.68")

	t := 1375147296.68
	_ = t // Prevent any noused variable errors

	{
		// times/shim.yaml line #8
		/* ("2013-07-29T18:21:36.680-07:00") */
		var expected_ string = "2013-07-29T18:21:36.680-07:00"
		/* r.expr(datetime.fromtimestamp(t, PacificTimeZone())).to_iso8601() */

		suite.T().Log("About to run line #8: r.Expr(Ast.Fromtimestamp(t, PacificTimeZone())).ToISO8601()")

		runAndAssert(suite.Suite, expected_, r.Expr(Ast.Fromtimestamp(t, PacificTimeZone())).ToISO8601(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #8")
	}

	{
		// times/shim.yaml line #12
		/* ("2013-07-30T01:21:36.680+00:00") */
		var expected_ string = "2013-07-30T01:21:36.680+00:00"
		/* r.expr(datetime.fromtimestamp(t, UTCTimeZone())).to_iso8601() */

		suite.T().Log("About to run line #12: r.Expr(Ast.Fromtimestamp(t, UTCTimeZone())).ToISO8601()")

		runAndAssert(suite.Suite, expected_, r.Expr(Ast.Fromtimestamp(t, UTCTimeZone())).ToISO8601(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #12")
	}

	{
		// times/shim.yaml line #16
		/* (1375147296.68) */
		var expected_ float64 = 1375147296.68
		/* r.expr(datetime.fromtimestamp(t, PacificTimeZone())).to_epoch_time() */

		suite.T().Log("About to run line #16: r.Expr(Ast.Fromtimestamp(t, PacificTimeZone())).ToEpochTime()")

		runAndAssert(suite.Suite, expected_, r.Expr(Ast.Fromtimestamp(t, PacificTimeZone())).ToEpochTime(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #16")
	}

	{
		// times/shim.yaml line #20
		/* (1375147296.68) */
		var expected_ float64 = 1375147296.68
		/* r.expr(datetime.fromtimestamp(t, UTCTimeZone())).to_epoch_time() */

		suite.T().Log("About to run line #20: r.Expr(Ast.Fromtimestamp(t, UTCTimeZone())).ToEpochTime()")

		runAndAssert(suite.Suite, expected_, r.Expr(Ast.Fromtimestamp(t, UTCTimeZone())).ToEpochTime(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #20")
	}
}
