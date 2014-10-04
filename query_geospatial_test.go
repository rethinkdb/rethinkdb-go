package gorethink

import test "gopkg.in/check.v1"

func (s *RethinkSuite) TestGeospatialGeometryPseudoType(c *test.C) {
	var response Geometry
	res, err := Expr(map[string]interface{}{
		"$reql_type$": "GEOMETRY",
		"type":        "Polygon",
		"coordinates": []interface{}{
			[]interface{}{
				[]interface{}{-122.423246, 37.779388},
				[]interface{}{-122.423246, 37.329898},
				[]interface{}{-121.88642, 37.329898},
				[]interface{}{-121.88642, 37.779388},
				[]interface{}{-122.423246, 37.779388},
			},
		},
	}).Run(sess)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.DeepEquals, Geometry{
		Type: "Polygon",
		Lines: Lines{
			Line{
				Point{-122.423246, 37.779388},
				Point{-122.423246, 37.329898},
				Point{-121.88642, 37.329898},
				Point{-121.88642, 37.779388},
				Point{-122.423246, 37.779388},
			},
		},
	})
}
