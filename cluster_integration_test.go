// +build cluster
// +build integration

package gorethink

import (
	"time"

	test "gopkg.in/check.v1"
)

func (s *RethinkSuite) TestClusterDetectNewNode(c *test.C) {
	cluster, err := ConnectCluster(ClusterOpts{
		Hosts:         []string{url, url2},
		DiscoverHosts: true,
	})
	c.Assert(err, test.IsNil)

	t := time.NewTimer(time.Minute * 5)
	for {
		select {
		// Fail if deadline has passed
		case <-t.C:
			c.Fatal("No node was added to the cluster")
		default:
			// Pass if another node was added
			if cluster.nodes.Size() >= 3 {
				return
			}
		}
	}
}

func (s *RethinkSuite) TestClusterDetectRemovedNode(c *test.C) {
	cluster, err := ConnectCluster(ClusterOpts{
		Hosts:         []string{url, url2, url3},
		DiscoverHosts: true,
	})
	c.Assert(err, test.IsNil)

	t := time.NewTimer(time.Minute * 5)
	for {
		select {
		// Fail if deadline has passed
		case <-t.C:
			c.Fatal("No node was removed from the cluster")
		default:
			// Pass if another node was added
			if cluster.nodes.Size() < 3 {
				return
			}
		}
	}
}
