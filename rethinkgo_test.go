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

// Expressions used in tests
var arr = []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}
var darr = []interface{}{1, 1, 2, 2, 3, 3, 5, 5, 6}
var obj = map[string]interface{}{"a": 1, "b": 2, "c": 3}
var objList = []interface{}{
	map[string]interface{}{"id": 1, "g1": 1, "g2": 1, "num": 0},
	map[string]interface{}{"id": 2, "g1": 2, "g2": 2, "num": 5},
	map[string]interface{}{"id": 3, "g1": 3, "g2": 2, "num": 10},
	map[string]interface{}{"id": 4, "g1": 2, "g2": 3, "num": 0},
	map[string]interface{}{"id": 5, "g1": 2, "g2": 3, "num": 100},
	map[string]interface{}{"id": 6, "g1": 1, "g2": 1, "num": 15},
	map[string]interface{}{"id": 7, "g1": 1, "g2": 2, "num": 0},
	map[string]interface{}{"id": 8, "g1": 4, "g2": 2, "num": 50},
	map[string]interface{}{"id": 9, "g1": 2, "g2": 3, "num": 25},
}
