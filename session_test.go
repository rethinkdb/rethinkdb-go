package gorethink

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestSessionConnectError(c *test.C) {
	var err error
	_, err = Connect(map[string]interface{}{
		"address":   "nonexistanturl",
		"maxIdle":   3,
		"maxActive": 3,
	})
	c.Assert(err, test.NotNil)
}
