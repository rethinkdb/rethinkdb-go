// +build cluster
// +build integration

package tests

import (
	"time"

	test "gopkg.in/check.v1"
	r "gopkg.in/gorethink/gorethink.v4"
	"strings"
	"strconv"
)

func (s *RethinkSuite) TestClusterDetectNewNode(c *test.C) {
	h1, p1 := splitAddress(url)
	h2, p2 := splitAddress(url2)
	hosts := []r.Host{r.NewHost(h1, p1), r.NewHost(h2, p2)}

	cluster, err := r.NewCluster(hosts, &r.ConnectOpts{
		Addresses:           []string{url, url2},
		DiscoverHosts:       true,
		NodeRefreshInterval: time.Second,
	})
	c.Assert(err, test.IsNil)

	t := time.NewTimer(time.Second * 30)
	for {
		select {
		// Fail if deadline has passed
		case <-t.C:
			c.Fatal("No node was added to the cluster")
		default:
			// Pass if another node was added
			if len(cluster.GetNodes()) >= 3 {
				return
			}
		}
	}
}

func (s *RethinkSuite) TestClusterRecoverAfterNoNodes(c *test.C) {
	h1, p1 := splitAddress(url)
	h2, p2 := splitAddress(url2)
	hosts := []r.Host{r.NewHost(h1, p1), r.NewHost(h2, p2)}

	cluster, err := r.NewCluster(hosts, &r.ConnectOpts{
		Addresses:           []string{url, url2},
		DiscoverHosts:       true,
		NodeRefreshInterval: time.Second,
	})
	c.Assert(err, test.IsNil)

	t := time.NewTimer(time.Second * 30)
	hasHadZeroNodes := false
	for {
		select {
		// Fail if deadline has passed
		case <-t.C:
			c.Fatal("No node was added to the cluster")
		default:
			// Check if there are no nodes
			if len(cluster.GetNodes()) == 0 {
				hasHadZeroNodes = true
			}

			// Pass if another node was added
			if len(cluster.GetNodes()) >= 1 && hasHadZeroNodes {
				return
			}
		}
	}
}

func (s *RethinkSuite) TestClusterNodeHealth(c *test.C) {
	session, err := r.Connect(r.ConnectOpts{
		Addresses:           []string{url1, url2, url3},
		DiscoverHosts:       true,
		NodeRefreshInterval: time.Second,
		InitialCap:          50,
		MaxOpen:             200,
	})
	c.Assert(err, test.IsNil)

	attempts := 0
	failed := 0
	seconds := 0

	t := time.NewTimer(time.Second * 30)
	tick := time.NewTicker(time.Second)
	for {
		select {
		// Fail if deadline has passed
		case <-tick.C:
			seconds++
			c.Logf("%ds elapsed", seconds)
		case <-t.C:
			// Execute queries for 10s and check that at most 5% of the queries fail
			c.Logf("%d of the %d(%d%%) queries failed", failed, attempts, (failed / attempts))
			c.Assert(failed <= 100, test.Equals, true)
			return
		default:
			attempts++
			if err := r.Expr(1).Exec(session); err != nil {
				c.Logf("Query failed, %s", err)
				failed++
			}
		}
	}
}

func splitAddress(address string) (hostname string, port int) {
	hostname = "localhost"
	port = 28015

	addrParts := strings.Split(address, ":")

	if len(addrParts) >= 1 {
		hostname = addrParts[0]
	}
	if len(addrParts) >= 2 {
		port, _ = strconv.Atoi(addrParts[1])
	}

	return
}
