package rethinkdb

import (
	test "gopkg.in/check.v1"
	"gopkg.in/rethinkdb/rethinkdb-go.v6/internal/integration/tests"
)

type CursorSuite struct{}

var _ = test.Suite(&CursorSuite{})

func (s *CursorSuite) TestCursor_One_Ok(c *test.C) {
	data := map[string]interface{}{
		"A": 1,
		"B": true,
	}

	mock := NewMock()
	ch := make(chan []interface{})
	mock.On(DB("test").Table("test")).Return(ch, nil)
	go func() {
		ch <- []interface{}{data}
		close(ch)
	}()
	res, err := DB("test").Table("test").Run(mock)
	c.Assert(err, test.IsNil)

	var response interface{}
	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, tests.JsonEquals, data)
	mock.AssertExpectations(c)
}
