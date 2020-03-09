//go:generate ../gen_tests/gen_tests.sh

package reql_tests

import (
	"flag"
	"os"
	"runtime"

	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

var url string

func init() {
	// Fixing test.testlogfile parsing error on Go 1.13+.
	if runtime.Version() < "go1.13" {
		flag.Parse()
	}

	r.SetVerbose(true)

	// If the test is being run by wercker look for the rethink url
	url = os.Getenv("RETHINKDB_URL")
	if url == "" {
		url = "localhost:28015"
	}
}
