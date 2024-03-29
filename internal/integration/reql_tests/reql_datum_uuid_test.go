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

// Test that UUIDs work
func TestDatumUuidSuite(t *testing.T) {
	suite.Run(t, new(DatumUuidSuite))
}

type DatumUuidSuite struct {
	suite.Suite

	session *r.Session
}

func (suite *DatumUuidSuite) SetupTest() {
	suite.T().Log("Setting up DatumUuidSuite")
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

func (suite *DatumUuidSuite) TearDownSuite() {
	suite.T().Log("Tearing down DatumUuidSuite")

	if suite.session != nil {
		r.DB("rethinkdb").Table("_debug_scratch").Delete().Exec(suite.session)
		r.DBDrop("test").Exec(suite.session)

		suite.session.Close()
	}
}

func (suite *DatumUuidSuite) TestCases() {
	suite.T().Log("Running DatumUuidSuite: Test that UUIDs work")

	{
		// datum/uuid.yaml line #3
		/* uuid() */
		var expected_ compare.Regex = compare.IsUUID()
		/* r.uuid() */

		suite.T().Log("About to run line #3: r.UUID()")

		runAndAssert(suite.Suite, expected_, r.UUID(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #3")
	}

	{
		// datum/uuid.yaml line #5
		/* uuid() */
		var expected_ compare.Regex = compare.IsUUID()
		/* r.expr(r.uuid()) */

		suite.T().Log("About to run line #5: r.Expr(r.UUID())")

		runAndAssert(suite.Suite, expected_, r.Expr(r.UUID()), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #5")
	}

	{
		// datum/uuid.yaml line #7
		/* 'STRING' */
		var expected_ string = "STRING"
		/* r.type_of(r.uuid()) */

		suite.T().Log("About to run line #7: r.TypeOf(r.UUID())")

		runAndAssert(suite.Suite, expected_, r.TypeOf(r.UUID()), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #7")
	}

	{
		// datum/uuid.yaml line #9
		/* true */
		var expected_ bool = true
		/* r.uuid().ne(r.uuid()) */

		suite.T().Log("About to run line #9: r.UUID().Ne(r.UUID())")

		runAndAssert(suite.Suite, expected_, r.UUID().Ne(r.UUID()), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #9")
	}

	{
		// datum/uuid.yaml line #11
		/* ('97dd10a5-4fc4-554f-86c5-0d2c2e3d5330') */
		var expected_ string = "97dd10a5-4fc4-554f-86c5-0d2c2e3d5330"
		/* r.uuid('magic') */

		suite.T().Log("About to run line #11: r.UUID('magic')")

		runAndAssert(suite.Suite, expected_, r.UUID("magic"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #11")
	}

	{
		// datum/uuid.yaml line #13
		/* true */
		var expected_ bool = true
		/* r.uuid('magic').eq(r.uuid('magic')) */

		suite.T().Log("About to run line #13: r.UUID('magic').Eq(r.UUID('magic'))")

		runAndAssert(suite.Suite, expected_, r.UUID("magic").Eq(r.UUID("magic")), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #13")
	}

	{
		// datum/uuid.yaml line #15
		/* true */
		var expected_ bool = true
		/* r.uuid('magic').ne(r.uuid('beans')) */

		suite.T().Log("About to run line #15: r.UUID('magic').Ne(r.UUID('beans'))")

		runAndAssert(suite.Suite, expected_, r.UUID("magic").Ne(r.UUID("beans")), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #15")
	}

	{
		// datum/uuid.yaml line #17
		/* 10 */
		var expected_ int = 10
		/* r.expr([1,2,3,4,5,6,7,8,9,10]).map(lambda u:r.uuid()).distinct().count() */

		suite.T().Log("About to run line #17: r.Expr([]interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).Map(func(u r.Term) interface{} { return r.UUID()}).Distinct().Count()")

		runAndAssert(suite.Suite, expected_, r.Expr([]interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).Map(func(u r.Term) interface{} { return r.UUID() }).Distinct().Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #17")
	}
}
