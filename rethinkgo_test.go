package rethinkgo

import (
	"flag"
	test "launchpad.net/gocheck"
	"os"
	"testing"
)

var session *Session
var debug = flag.Bool("test.debug", false, "debug: print query trees")

func init() {
	flag.Parse()
}

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { test.TestingT(t) }

type RethinkSuite struct{}

var _ = test.Suite(&RethinkSuite{})

func (s *RethinkSuite) SetUpSuite(c *test.C) {
	// If the test is being run by wercker look for the rethink url
	url := os.Getenv("WERCKER_RETHINKDB_URL")
	if url == "" {
		url = "localhost:28015"
	}

	db := os.Getenv("WERCKER_RETHINKDB_DB")
	if db == "" {
		db = "test"
	}

	// SetDebug(*debug)
	var err error
	session, err = Connect(map[string]interface{}{
		"address": url,
	})
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TearDownSuite(c *test.C) {
	session.Close()
}
