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

// Tests bitwise operators
func TestMathLogicBitSuite(t *testing.T) {
	suite.Run(t, new(MathLogicBitSuite))
}

type MathLogicBitSuite struct {
	suite.Suite

	session *r.Session
}

func (suite *MathLogicBitSuite) SetupTest() {
	suite.T().Log("Setting up MathLogicBitSuite")
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

func (suite *MathLogicBitSuite) TearDownSuite() {
	suite.T().Log("Tearing down MathLogicBitSuite")

	if suite.session != nil {
		r.DB("rethinkdb").Table("_debug_scratch").Delete().Exec(suite.session)
		r.DBDrop("test").Exec(suite.session)

		suite.session.Close()
	}
}

func (suite *MathLogicBitSuite) TestCases() {
	suite.T().Log("Running MathLogicBitSuite: Tests bitwise operators")

	{
		// math_logic/bit.yaml line #4
		/* 2 */
		var expected_ int = 2
		/* r.expr(3).bit_and(2) */

		suite.T().Log("About to run line #4: r.Expr(3).BitAnd(2)")

		runAndAssert(suite.Suite, expected_, r.Expr(3).BitAnd(2), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #4")
	}

	{
		// math_logic/bit.yaml line #7
		/* 2 */
		var expected_ int = 2
		/* r.expr(-2).bit_and(3) */

		suite.T().Log("About to run line #7: r.Expr(-2).BitAnd(3)")

		runAndAssert(suite.Suite, expected_, r.Expr(-2).BitAnd(3), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #7")
	}

	{
		// math_logic/bit.yaml line #10
		/* err('ReqlQueryLogicError', 'Integer too large: 9007199254740992') */
		var expected_ Err = err("ReqlQueryLogicError", "Integer too large: 9007199254740992")
		/* r.expr(9007199254740992).bit_and(0) */

		suite.T().Log("About to run line #10: r.Expr(9007199254740992).BitAnd(0)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740992).BitAnd(0), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #10")
	}

	{
		// math_logic/bit.yaml line #13
		/* err('ReqlQueryLogicError', 'Number not an integer (>2^53): 9007199254740994') */
		var expected_ Err = err("ReqlQueryLogicError", "Number not an integer (>2^53): 9007199254740994")
		/* r.expr(9007199254740994).bit_and(0) */

		suite.T().Log("About to run line #13: r.Expr(9007199254740994).BitAnd(0)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740994).BitAnd(0), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #13")
	}

	{
		// math_logic/bit.yaml line #16
		/* 23 */
		var expected_ int = 23
		/* r.expr(9007199254740991).bit_and(23) */

		suite.T().Log("About to run line #16: r.Expr(9007199254740991).BitAnd(23)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740991).BitAnd(23), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #16")
	}

	{
		// math_logic/bit.yaml line #19
		/* 0 */
		var expected_ int = 0
		/* r.expr(-9007199254740992).bit_and(12345) */

		suite.T().Log("About to run line #19: r.Expr(-9007199254740992).BitAnd(12345)")

		runAndAssert(suite.Suite, expected_, r.Expr(-9007199254740992).BitAnd(12345), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #19")
	}

	{
		// math_logic/bit.yaml line #22
		/* 3 */
		var expected_ int = 3
		/* r.expr(1).bit_or(2) */

		suite.T().Log("About to run line #22: r.Expr(1).BitOr(2)")

		runAndAssert(suite.Suite, expected_, r.Expr(1).BitOr(2), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #22")
	}

	{
		// math_logic/bit.yaml line #25
		/* err('ReqlQueryLogicError', 'Integer too large: 9007199254740992') */
		var expected_ Err = err("ReqlQueryLogicError", "Integer too large: 9007199254740992")
		/* r.expr(9007199254740992).bit_or(0) */

		suite.T().Log("About to run line #25: r.Expr(9007199254740992).BitOr(0)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740992).BitOr(0), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #25")
	}

	{
		// math_logic/bit.yaml line #28
		/* 9007199254740991 */
		var expected_ int = 9007199254740991
		/* r.expr(9007199254740991).bit_or(0) */

		suite.T().Log("About to run line #28: r.Expr(9007199254740991).BitOr(0)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740991).BitOr(0), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #28")
	}

	{
		// math_logic/bit.yaml line #31
		/* -1 */
		var expected_ int = -1
		/* r.expr(9007199254740991).bit_or(-1) */

		suite.T().Log("About to run line #31: r.Expr(9007199254740991).BitOr(-1)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740991).BitOr(-1), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #31")
	}

	{
		// math_logic/bit.yaml line #34
		/* 5 */
		var expected_ int = 5
		/* r.expr(3).bit_xor(6) */

		suite.T().Log("About to run line #34: r.Expr(3).BitXor(6)")

		runAndAssert(suite.Suite, expected_, r.Expr(3).BitXor(6), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #34")
	}

	{
		// math_logic/bit.yaml line #37
		/* -3 */
		var expected_ int = -3
		/* r.expr(2).bit_not() */

		suite.T().Log("About to run line #37: r.Expr(2).BitNot()")

		runAndAssert(suite.Suite, expected_, r.Expr(2).BitNot(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #37")
	}

	{
		// math_logic/bit.yaml line #40
		/* -9007199254740992 */
		var expected_ int = -9007199254740992
		/* r.expr(9007199254740991).bit_not() */

		suite.T().Log("About to run line #40: r.Expr(9007199254740991).BitNot()")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740991).BitNot(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #40")
	}

	{
		// math_logic/bit.yaml line #43
		/* 9007199254740991 */
		var expected_ int = 9007199254740991
		/* r.expr(9007199254740991).bit_not().bit_not() */

		suite.T().Log("About to run line #43: r.Expr(9007199254740991).BitNot().BitNot()")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740991).BitNot().BitNot(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #43")
	}

	{
		// math_logic/bit.yaml line #47
		/* err('ReqlQueryLogicError', 'Integer too large: 9007199254740992') */
		var expected_ Err = err("ReqlQueryLogicError", "Integer too large: 9007199254740992")
		/* r.expr(9007199254740992).bit_sar(0) */

		suite.T().Log("About to run line #47: r.Expr(9007199254740992).BitSar(0)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740992).BitSar(0), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #47")
	}

	{
		// math_logic/bit.yaml line #50
		/* -9007199254740992 */
		var expected_ int = -9007199254740992
		/* r.expr(-9007199254740992).bit_sar(0) */

		suite.T().Log("About to run line #50: r.Expr(-9007199254740992).BitSar(0)")

		runAndAssert(suite.Suite, expected_, r.Expr(-9007199254740992).BitSar(0), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #50")
	}

	{
		// math_logic/bit.yaml line #53
		/* -4503599627370496 */
		var expected_ int = -4503599627370496
		/* r.expr(-9007199254740992).bit_sar(1) */

		suite.T().Log("About to run line #53: r.Expr(-9007199254740992).BitSar(1)")

		runAndAssert(suite.Suite, expected_, r.Expr(-9007199254740992).BitSar(1), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #53")
	}

	{
		// math_logic/bit.yaml line #56
		/* -2 */
		var expected_ int = -2
		/* r.expr(-9007199254740992).bit_sar(52) */

		suite.T().Log("About to run line #56: r.Expr(-9007199254740992).BitSar(52)")

		runAndAssert(suite.Suite, expected_, r.Expr(-9007199254740992).BitSar(52), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #56")
	}

	{
		// math_logic/bit.yaml line #59
		/* -1 */
		var expected_ int = -1
		/* r.expr(-9007199254740992).bit_sar(53) */

		suite.T().Log("About to run line #59: r.Expr(-9007199254740992).BitSar(53)")

		runAndAssert(suite.Suite, expected_, r.Expr(-9007199254740992).BitSar(53), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #59")
	}

	{
		// math_logic/bit.yaml line #62
		/* -1 */
		var expected_ int = -1
		/* r.expr(-9007199254740992).bit_sar(54) */

		suite.T().Log("About to run line #62: r.Expr(-9007199254740992).BitSar(54)")

		runAndAssert(suite.Suite, expected_, r.Expr(-9007199254740992).BitSar(54), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #62")
	}

	{
		// math_logic/bit.yaml line #65
		/* 9007199254740991 */
		var expected_ int = 9007199254740991
		/* r.expr(9007199254740991).bit_sar(0) */

		suite.T().Log("About to run line #65: r.Expr(9007199254740991).BitSar(0)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740991).BitSar(0), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #65")
	}

	{
		// math_logic/bit.yaml line #68
		/* 4503599627370495 */
		var expected_ int = 4503599627370495
		/* r.expr(9007199254740991).bit_sar(1) */

		suite.T().Log("About to run line #68: r.Expr(9007199254740991).BitSar(1)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740991).BitSar(1), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #68")
	}

	{
		// math_logic/bit.yaml line #71
		/* 1 */
		var expected_ int = 1
		/* r.expr(9007199254740991).bit_sar(52) */

		suite.T().Log("About to run line #71: r.Expr(9007199254740991).BitSar(52)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740991).BitSar(52), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #71")
	}

	{
		// math_logic/bit.yaml line #74
		/* 0 */
		var expected_ int = 0
		/* r.expr(9007199254740991).bit_sar(53) */

		suite.T().Log("About to run line #74: r.Expr(9007199254740991).BitSar(53)")

		runAndAssert(suite.Suite, expected_, r.Expr(9007199254740991).BitSar(53), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #74")
	}

	{
		// math_logic/bit.yaml line #78
		/* 0 */
		var expected_ int = 0
		/* r.expr(0).bit_sal(999999) */

		suite.T().Log("About to run line #78: r.Expr(0).BitSal(999999)")

		runAndAssert(suite.Suite, expected_, r.Expr(0).BitSal(999999), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #78")
	}

	{
		// math_logic/bit.yaml line #81
		/* 0 */
		var expected_ int = 0
		/* r.expr(0).bit_sal(3000) */

		suite.T().Log("About to run line #81: r.Expr(0).BitSal(3000)")

		runAndAssert(suite.Suite, expected_, r.Expr(0).BitSal(3000), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #81")
	}

	{
		// math_logic/bit.yaml line #84
		/* 0 */
		var expected_ int = 0
		/* r.expr(0).bit_sal(500) */

		suite.T().Log("About to run line #84: r.Expr(0).BitSal(500)")

		runAndAssert(suite.Suite, expected_, r.Expr(0).BitSal(500), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #84")
	}

	{
		// math_logic/bit.yaml line #87
		/* 0 */
		var expected_ int = 0
		/* r.expr(0).bit_sal(0) */

		suite.T().Log("About to run line #87: r.Expr(0).BitSal(0)")

		runAndAssert(suite.Suite, expected_, r.Expr(0).BitSal(0), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #87")
	}

	{
		// math_logic/bit.yaml line #90
		/* 1 */
		var expected_ int = 1
		/* r.expr(1).bit_sal(0) */

		suite.T().Log("About to run line #90: r.Expr(1).BitSal(0)")

		runAndAssert(suite.Suite, expected_, r.Expr(1).BitSal(0), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #90")
	}

	{
		// math_logic/bit.yaml line #93
		/* 2 */
		var expected_ int = 2
		/* r.expr(1).bit_sal(1) */

		suite.T().Log("About to run line #93: r.Expr(1).BitSal(1)")

		runAndAssert(suite.Suite, expected_, r.Expr(1).BitSal(1), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #93")
	}

	{
		// math_logic/bit.yaml line #96
		/* 8 */
		var expected_ int = 8
		/* r.expr(1).bit_sal(3) */

		suite.T().Log("About to run line #96: r.Expr(1).BitSal(3)")

		runAndAssert(suite.Suite, expected_, r.Expr(1).BitSal(3), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #96")
	}

	{
		// math_logic/bit.yaml line #99
		/* -8 */
		var expected_ int = -8
		/* r.expr(-1).bit_sal(3) */

		suite.T().Log("About to run line #99: r.Expr(-1).BitSal(3)")

		runAndAssert(suite.Suite, expected_, r.Expr(-1).BitSal(3), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #99")
	}

	{
		// math_logic/bit.yaml line #102
		/* -18014398509481984 */
		var expected_ int = -18014398509481984
		/* r.expr(-1).bit_sal(54) */

		suite.T().Log("About to run line #102: r.Expr(-1).BitSal(54)")

		runAndAssert(suite.Suite, expected_, r.Expr(-1).BitSal(54), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #102")
	}

	{
		// math_logic/bit.yaml line #105
		/* 18014398509481984 */
		var expected_ int = 18014398509481984
		/* r.expr(1).bit_sal(54) */

		suite.T().Log("About to run line #105: r.Expr(1).BitSal(54)")

		runAndAssert(suite.Suite, expected_, r.Expr(1).BitSal(54), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #105")
	}

	{
		// math_logic/bit.yaml line #108
		/* -18014398509481984 */
		var expected_ int = -18014398509481984
		/* r.expr(-2).bit_sal(53) */

		suite.T().Log("About to run line #108: r.Expr(-2).BitSal(53)")

		runAndAssert(suite.Suite, expected_, r.Expr(-2).BitSal(53), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #108")
	}

	{
		// math_logic/bit.yaml line #111
		/* 18014398509481984 */
		var expected_ int = 18014398509481984
		/* r.expr(2).bit_sal(53) */

		suite.T().Log("About to run line #111: r.Expr(2).BitSal(53)")

		runAndAssert(suite.Suite, expected_, r.Expr(2).BitSal(53), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #111")
	}

	{
		// math_logic/bit.yaml line #114
		/* err('ReqlQueryLogicError', 'Cannot bit-shift by a negative value') */
		var expected_ Err = err("ReqlQueryLogicError", "Cannot bit-shift by a negative value")
		/* r.expr(5).bit_sal(-1) */

		suite.T().Log("About to run line #114: r.Expr(5).BitSal(-1)")

		runAndAssert(suite.Suite, expected_, r.Expr(5).BitSal(-1), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #114")
	}

	{
		// math_logic/bit.yaml line #117
		/* err('ReqlQueryLogicError', 'Cannot bit-shift by a negative value') */
		var expected_ Err = err("ReqlQueryLogicError", "Cannot bit-shift by a negative value")
		/* r.expr(5).bit_sar(-1) */

		suite.T().Log("About to run line #117: r.Expr(5).BitSar(-1)")

		runAndAssert(suite.Suite, expected_, r.Expr(5).BitSar(-1), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #117")
	}

	{
		// math_logic/bit.yaml line #121
		/* err('ReqlQueryLogicError', 'Expected type NUMBER but found STRING.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "Expected type NUMBER but found STRING.")
		/* r.expr('a').bit_and(12) */

		suite.T().Log("About to run line #121: r.Expr('a').BitAnd(12)")

		runAndAssert(suite.Suite, expected_, r.Expr("a").BitAnd(12), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #121")
	}

	{
		// math_logic/bit.yaml line #124
		/* err('ReqlQueryLogicError', 'Expected type NUMBER but found STRING.', [1]) */
		var expected_ Err = err("ReqlQueryLogicError", "Expected type NUMBER but found STRING.")
		/* r.expr(12).bit_and('a') */

		suite.T().Log("About to run line #124: r.Expr(12).BitAnd('a')")

		runAndAssert(suite.Suite, expected_, r.Expr(12).BitAnd("a"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #124")
	}

	{
		// math_logic/bit.yaml line #127
		/* err('ReqlQueryLogicError', 'Number not an integer: 1.5') */
		var expected_ Err = err("ReqlQueryLogicError", "Number not an integer: 1.5")
		/* r.expr(1.5).bit_and(3) */

		suite.T().Log("About to run line #127: r.Expr(1.5).BitAnd(3)")

		runAndAssert(suite.Suite, expected_, r.Expr(1.5).BitAnd(3), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #127")
	}
}
