package reql_tests

import (
	"github.com/stretchr/testify/suite"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"testing"
)

// Test edge cases of bitwise operations
func TestBitwiseSuite(t *testing.T) {
	suite.Run(t, new(BitwiseSuite))
}

type BitwiseSuite struct {
	suite.Suite

	session *r.Session
}

func (suite *BitwiseSuite) SetupTest() {
	suite.T().Log("Setting up BitwiseSuite")

	session, err := r.Connect(r.ConnectOpts{
		Address: url,
	})
	suite.Require().NoError(err, "Error returned when connecting to server")
	suite.session = session
}

func (suite *BitwiseSuite) TearDownSuite() {
	suite.T().Log("Tearing down BitwiseSuite")

	if suite.session != nil {
		suite.session.Close()
	}
}

func (suite *BitwiseSuite) TestCases() {
	suite.T().Log("Running BitwiseSuite: Test edge cases of bitwise operations")

	runOpts := r.RunOpts{
		GeometryFormat: "raw",
		GroupFormat:    "map",
	}

	{
		var q = r.BitAnd(3, 5)
		var expected_ = 3 & 5

		suite.T().Logf("About to run line #1: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #1")
	}

	{
		var q = r.Expr(3).BitAnd(5)
		var expected_ = 3 & 5

		suite.T().Logf("About to run line #2: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #2")
	}

	{
		var q = r.BitOr(3, 5)
		var expected_ = 3 | 5

		suite.T().Logf("About to run line #3: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #3")
	}

	{
		var q = r.Expr(3).BitOr(5)
		var expected_ = 3 | 5

		suite.T().Logf("About to run line #4: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #4")
	}

	{
		var q = r.BitXor(3, 5)
		var expected_ = 3 ^ 5

		suite.T().Logf("About to run line #5: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #5")
	}

	{
		var q = r.Expr(3).BitXor(5)
		var expected_ = 3 ^ 5

		suite.T().Logf("About to run line #6: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #6")
	}

	{
		var q = r.BitNot(3)
		var expected_ = ^3

		suite.T().Logf("About to run line #7: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #7")
	}

	{
		var q = r.Expr(3).BitNot()
		var expected_ = ^3

		suite.T().Logf("About to run line #8: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #8")
	}

	{
		var q = r.BitSal(3, 5)
		var expected_ = 3 << 5

		suite.T().Logf("About to run line #9: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #9")
	}

	{
		var q = r.Expr(3).BitSal(5)
		var expected_ = 3 << 5

		suite.T().Logf("About to run line #10: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #10")
	}

	{
		var q = r.BitSar(3, 5)
		var expected_ = 3 >> 5

		suite.T().Logf("About to run line #11: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #11")
	}

	{
		var q = r.Expr(3).BitSar(5)
		var expected_ = 3 >> 5

		suite.T().Logf("About to run line #12: %v", q)

		runAndAssert(suite.Suite, expected_, q, suite.session, runOpts)
		suite.T().Log("Finished running line #12")
	}
}
