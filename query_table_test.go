package gorethink

import (
	"sync"

	test "launchpad.net/gocheck"
)

func (s *RethinkSuite) TestTableCreate(c *test.C) {
	var response interface{}

	Db("test").TableDrop("test").Exec(sess)

	// Test database creation
	query := Db("test").TableCreate("test")

	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"created": 1})
}

func (s *RethinkSuite) TestTableCreatePrimaryKey(c *test.C) {
	var response interface{}

	Db("test").TableDrop("testOpts").Exec(sess)

	// Test database creation
	query := Db("test").TableCreate("testOpts", TableCreateOpts{
		PrimaryKey: "it",
	})

	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"created": 1})
}

func (s *RethinkSuite) TestTableCreateSoftDurability(c *test.C) {
	var response interface{}

	Db("test").TableDrop("testOpts").Exec(sess)

	// Test database creation
	query := Db("test").TableCreate("testOpts", TableCreateOpts{
		Durability: "soft",
	})

	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"created": 1})
}

func (s *RethinkSuite) TestTableCreateSoftMultipleOpts(c *test.C) {
	var response interface{}

	Db("test").TableDrop("testOpts").Exec(sess)

	// Test database creation
	query := Db("test").TableCreate("testOpts", TableCreateOpts{
		PrimaryKey: "it",
		Durability: "soft",
	})

	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"created": 1})

	Db("test").TableDrop("test").Exec(sess)
}

func (s *RethinkSuite) TestTableList(c *test.C) {
	var response []interface{}

	Db("test").TableCreate("test").Exec(sess)

	// Try and find it in the list
	success := false
	row, err := Db("test").TableList().Run(sess)
	c.Assert(err, test.IsNil)

	row.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.FitsTypeOf, []interface{}{})

	for _, db := range response {
		if db == "test" {
			success = true
		}
	}

	c.Assert(success, test.Equals, true)
}

func (s *RethinkSuite) TestTableDelete(c *test.C) {
	var response interface{}

	Db("test").TableCreate("test").Exec(sess)

	// Test database creation
	query := Db("test").TableDrop("test")

	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"dropped": 1})
}

func (s *RethinkSuite) TestTableIndexCreate(c *test.C) {
	var response interface{}

	Db("test").TableCreate("test").Exec(sess)
	Db("test").Table("test").IndexDrop("test").Exec(sess)

	// Test database creation
	query := Db("test").Table("test").IndexCreate("test", IndexCreateOpts{
		Multi: true,
	})

	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"created": 1})
}

func (s *RethinkSuite) TestTableCompoundIndexCreate(c *test.C) {
	DbCreate("test").Exec(sess)
	Db("test").TableDrop("TableCompound").Exec(sess)
	Db("test").TableCreate("TableCompound").Exec(sess)
	response, err := Db("test").Table("TableCompound").IndexCreateFunc("full_name", func(row Term) interface{} {
		return []interface{}{row.Field("first_name"), row.Field("last_name")}
	}).RunWrite(sess)
	c.Assert(err, test.IsNil)
	c.Assert(response.Created, test.Equals, 1)
}

func (s *RethinkSuite) TestTableIndexList(c *test.C) {
	var response []interface{}

	Db("test").TableCreate("test").Exec(sess)
	Db("test").Table("test").IndexCreate("test").Exec(sess)

	// Try and find it in the list
	success := false
	row, err := Db("test").Table("test").IndexList().Run(sess)
	c.Assert(err, test.IsNil)

	err = row.ScanAll(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.FitsTypeOf, []interface{}{})

	for _, db := range response {
		if db == "test" {
			success = true
		}
	}

	c.Assert(success, test.Equals, true)
}

func (s *RethinkSuite) TestTableIndexDelete(c *test.C) {
	var response interface{}

	Db("test").TableCreate("test").Exec(sess)
	Db("test").Table("test").IndexCreate("test").Exec(sess)

	// Test database creation
	query := Db("test").Table("test").IndexDrop("test")

	r, err := query.RunRow(sess)
	c.Assert(err, test.IsNil)

	err = r.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"dropped": 1})
}

func (s *RethinkSuite) TestTableChanges(c *test.C) {
	Db("test").TableDrop("changes").Exec(sess)
	Db("test").TableCreate("changes").Exec(sess)

	var n int

	rows, err := Db("test").Table("changes").Changes().Run(sess)
	if err != nil {
		c.Fatal(err.Error())
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Use goroutine to wait for changes. Prints the first 10 results
	go func() {
		for n < 10 && rows.Next() {
			var response interface{}
			err = rows.Scan(&response)
			if err != nil {
				c.Fatal(err.Error())
			}

			n++
		}

		if rows.Err() != nil {
			c.Fatal(rows.Err())
		}

		wg.Done()
	}()

	Db("test").Table("changes").Insert(map[string]interface{}{"n": 1}).Exec(sess)
	Db("test").Table("changes").Insert(map[string]interface{}{"n": 2}).Exec(sess)
	Db("test").Table("changes").Insert(map[string]interface{}{"n": 3}).Exec(sess)
	Db("test").Table("changes").Insert(map[string]interface{}{"n": 4}).Exec(sess)
	Db("test").Table("changes").Insert(map[string]interface{}{"n": 5}).Exec(sess)
	Db("test").Table("changes").Insert(map[string]interface{}{"n": 6}).Exec(sess)
	Db("test").Table("changes").Insert(map[string]interface{}{"n": 7}).Exec(sess)
	Db("test").Table("changes").Insert(map[string]interface{}{"n": 8}).Exec(sess)
	Db("test").Table("changes").Insert(map[string]interface{}{"n": 9}).Exec(sess)
	Db("test").Table("changes").Insert(map[string]interface{}{"n": 10}).Exec(sess)

	wg.Wait()

	c.Assert(n, test.Equals, 10)
}
