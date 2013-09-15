package rethinkgo

import (
	"encoding/json"
	"flag"
	test "launchpad.net/gocheck"
	"os"
	"testing"
)

var conn *Connection
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
	conn, err = Connect(map[string]interface{}{
		"address": url,
	})
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TearDownSuite(c *test.C) {
	conn.Close()
}

type jsonChecker struct {
	info *test.CheckerInfo
}

func (j jsonChecker) Info() *test.CheckerInfo {
	return j.info
}

func (j jsonChecker) Check(params []interface{}, names []string) (result bool, error string) {
	var jsonParams []interface{}
	for _, param := range params {
		jsonParam, err := json.Marshal(param)
		if err != nil {
			return false, err.Error()
		}
		jsonParams = append(jsonParams, jsonParam)
	}
	return test.DeepEquals.Check(jsonParams, names)
}

// JsonEquals compares two interface{} objects by converting them to JSON and
// seeing if the strings match
var JsonEquals = &jsonChecker{
	&test.CheckerInfo{Name: "JsonEquals", Params: []string{"obtained", "expected"}},
}
