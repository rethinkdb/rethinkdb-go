package gorethink

import (
	"fmt"

	test "gopkg.in/check.v1"
)

func (s *RethinkSuite) TestMockExecSuccess(c *test.C) {
	mock := NewMock()
	mock.On(DB("test").Table("test").Insert(map[string]string{
		"id": "mocked",
	})).Return(nil, nil)

	err := DB("test").Table("test").Insert(map[string]string{
		"id": "mocked",
	}).Exec(mock)
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TestMockExecFail(c *test.C) {
	mock := NewMock()
	mock.On(DB("test").Table("test").Insert(map[string]string{
		"id": "mocked",
	})).Return(nil, fmt.Errorf("Expected error"))

	err := DB("test").Table("test").Insert(map[string]string{
		"id": "mocked",
	}).Exec(mock)
	c.Assert(err, test.NotNil)
}

func (s *RethinkSuite) TestMockRunSuccessSingle(c *test.C) {
	mock := NewMock()
	mock.On(DB("test").Table("test").Get("mocked")).Return(map[string]interface{}{
		"id": "mocked",
	}, nil)

	res, err := DB("test").Table("test").Get("mocked").Run(mock)
	c.Assert(err, test.IsNil)

	var response interface{}
	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, jsonEquals, map[string]interface{}{"id": "mocked"})

	res.Close()
}

func (s *RethinkSuite) TestMockRunSuccessMultiple(c *test.C) {
	mock := NewMock()
	mock.On(DB("test").Table("test")).Return([]interface{}{
		map[string]interface{}{"id": "mocked"},
	}, nil)

	res, err := DB("test").Table("test").Run(mock)
	c.Assert(err, test.IsNil)

	var response []interface{}
	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, jsonEquals, []interface{}{map[string]interface{}{"id": "mocked"}})

	res.Close()
}
