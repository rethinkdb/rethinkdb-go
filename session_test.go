package gorethink

import (
	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestSessionConnectError(c *test.C) {
	var err error
	_, err = Connect(ConnectOpts{
		Address:   "nonexistanturl",
		MaxIdle:   3,
		MaxActive: 3,
	})
	c.Assert(err, test.NotNil)
}
