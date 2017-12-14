// +build cluster

package tests

import (
	"fmt"
	"time"

	test "gopkg.in/check.v1"
	r "gopkg.in/gorethink/gorethink.v4"
)

func (s *RethinkSuite) TestClusterConnect(c *test.C) {
	session, err := r.Connect(r.ConnectOpts{
		Addresses: []string{url1, url2, url3},
	})
	c.Assert(err, test.IsNil)

	row, err := r.Expr("Hello World").Run(session)
	c.Assert(err, test.IsNil)

	var response string
	err = row.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "Hello World")
}

func (s *RethinkSuite) TestClusterMultipleQueries(c *test.C) {
	session, err := r.Connect(r.ConnectOpts{
		Addresses: []string{url1, url2, url3},
	})
	c.Assert(err, test.IsNil)

	for i := 0; i < 1000; i++ {
		row, err := r.Expr(fmt.Sprintf("Hello World %v", i)).Run(session)
		c.Assert(err, test.IsNil)

		var response string
		err = row.One(&response)
		c.Assert(err, test.IsNil)
		c.Assert(response, test.Equals, fmt.Sprintf("Hello World %v", i))
	}
}

func (s *RethinkSuite) TestClusterConnectError(c *test.C) {
	var err error
	_, err = r.Connect(r.ConnectOpts{
		Addresses: []string{"nonexistanturl"},
		Timeout:   time.Second,
	})
	c.Assert(err, test.NotNil)
}

func (s *RethinkSuite) TestClusterConnectDatabase(c *test.C) {
	session, err := r.Connect(r.ConnectOpts{
		Addresses: []string{url1, url2, url3},
		Database:  "test2",
	})
	c.Assert(err, test.IsNil)

	_, err = r.Table("test2").Run(session)
	c.Assert(err, test.NotNil)
	c.Assert(err.Error(), test.Equals, "gorethink: Database `test2` does not exist. in:\nr.Table(\"test2\")")
}
