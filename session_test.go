package gorethink

import (
	"os"

	test "gopkg.in/check.v1"
)

func (s *RethinkSuite) TestSessionConnect(c *test.C) {
	session, err := Connect(ConnectOpts{
		Address: url,
		AuthKey: os.Getenv("RETHINKDB_AUTHKEY"),
	})
	c.Assert(err, test.IsNil)

	row, err := Expr("Hello World").Run(session)
	c.Assert(err, test.IsNil)

	var response string
	err = row.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "Hello World")
}

func (s *RethinkSuite) TestSessionReconnect(c *test.C) {
	session, err := Connect(ConnectOpts{
		Address: url,
		AuthKey: os.Getenv("RETHINKDB_AUTHKEY"),
	})
	c.Assert(err, test.IsNil)

	row, err := Expr("Hello World").Run(session)
	c.Assert(err, test.IsNil)

	var response string
	err = row.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "Hello World")

	err = session.Reconnect()
	c.Assert(err, test.IsNil)

	row, err = Expr("Hello World 2").Run(session)
	c.Assert(err, test.IsNil)

	err = row.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "Hello World 2")
}

func (s *RethinkSuite) TestSessionConnectError(c *test.C) {
	var err error
	_, err = Connect(ConnectOpts{
		Address: "nonexistanturl",
	})
	c.Assert(err, test.NotNil)
}
