package tests

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"
	"time"

	test "gopkg.in/check.v1"
	r "gopkg.in/gorethink/gorethink.v4"
)

func (s *RethinkSuite) TestQueryRun(c *test.C) {
	var response string

	res, err := r.Expr("Test").Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "Test")
}

func (s *RethinkSuite) TestQueryReadOne(c *test.C) {
	var response string

	err := r.Expr("Test").ReadOne(&response, session)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, "Test")
}

func (s *RethinkSuite) TestQueryReadAll(c *test.C) {
	var response []int

	err := r.Expr([]int{1, 2, 3}).ReadAll(&response, session)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.HasLen, 3)
	c.Assert(response, test.DeepEquals, []int{1, 2, 3})
}

func (s *RethinkSuite) TestQueryExec(c *test.C) {
	err := r.Expr("Test").Exec(session)
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TestQueryRunWrite(c *test.C) {
	query := r.DB("test").Table("test").Insert([]interface{}{
		map[string]interface{}{"num": 1},
		map[string]interface{}{"num": 2},
	}, r.InsertOpts{ReturnChanges: true})
	res, err := query.RunWrite(session)
	c.Assert(err, test.IsNil)
	c.Assert(res.Inserted, test.Equals, 2)
	c.Assert(len(res.Changes), test.Equals, 2)
}

func (s *RethinkSuite) TestQueryProfile(c *test.C) {
	var response string

	res, err := r.Expr("Test").Run(session, r.RunOpts{
		Profile: true,
	})
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(res.Profile(), test.NotNil)
	c.Assert(response, test.Equals, "Test")
}

func (s *RethinkSuite) TestQueryRunRawTime(c *test.C) {
	var response map[string]interface{}

	res, err := r.Now().Run(session, r.RunOpts{
		TimeFormat: "raw",
	})
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response["$reql_type$"], test.NotNil)
	c.Assert(response["$reql_type$"], test.Equals, "TIME")
}

func (s *RethinkSuite) TestQueryRunNil(c *test.C) {
	res, err := r.Expr("Test").Run(nil)
	c.Assert(res, test.IsNil)
	c.Assert(err, test.NotNil)
	c.Assert(err, test.Equals, r.ErrConnectionClosed)
}

func (s *RethinkSuite) TestQueryRunNotConnected(c *test.C) {
	res, err := r.Expr("Test").Run(&r.Session{})
	c.Assert(res, test.IsNil)
	c.Assert(err, test.NotNil)
	c.Assert(err, test.Equals, r.ErrConnectionClosed)
}

func (s *RethinkSuite) TestControlExprNil(c *test.C) {
	var response interface{}
	query := r.Expr(nil)
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.Equals, r.ErrEmptyResult)
	c.Assert(response, test.Equals, nil)
}

func (s *RethinkSuite) TestControlExprSimple(c *test.C) {
	var response int
	query := r.Expr(1)
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
}

func (s *RethinkSuite) TestControlExprList(c *test.C) {
	var response []interface{}
	query := r.Expr(narr)
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.All(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{
		1, 2, 3, 4, 5, 6, []interface{}{
			7.1, 7.2, 7.3,
		},
	})
}

func (s *RethinkSuite) TestControlExprObj(c *test.C) {
	var response map[string]interface{}
	query := r.Expr(nobj)
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"A": 1,
		"B": 2,
		"C": map[string]interface{}{
			"1": 3,
			"2": 4,
		},
	})
}

func (s *RethinkSuite) TestControlStruct(c *test.C) {
	var response map[string]interface{}
	query := r.Expr(str)
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"id": "A",
		"B":  1,
		"D":  map[string]interface{}{"D2": "2", "D1": 1},
		"E":  []interface{}{"E1", "E2", "E3", 4},
		"F": map[string]interface{}{
			"XA": 2,
			"XB": "B",
			"XC": []interface{}{"XC1", "XC2"},
			"XD": map[string]interface{}{
				"YA": 3,
				"YB": map[string]interface{}{
					"1": "1",
					"2": "2",
					"3": 3,
				},
				"YC": map[string]interface{}{
					"YC1": "YC1",
				},
				"YD": map[string]interface{}{
					"YD1": "YD1",
				},
			},
			"XE": "XE",
			"XF": []interface{}{"XE1", "XE2"},
		},
	})
}

func (s *RethinkSuite) TestControlStructTags(c *test.C) {
	r.SetTags("gorethink", "json")
	defer r.SetTags()

	var response map[string]interface{}
	query := r.Expr(TagsTest{"1", "2", "3"})
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{
		"a": "1", "b": "2", "c1": "3",
	})

}

func (s *RethinkSuite) TestControlMapTypeAlias(c *test.C) {
	var response TMap
	query := r.Expr(TMap{"A": 1, "B": 2})
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, TMap{"A": 1, "B": 2})
}

func (s *RethinkSuite) TestControlStringTypeAlias(c *test.C) {
	var response TStr
	query := r.Expr(TStr("Hello"))
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, TStr("Hello"))
}

func (s *RethinkSuite) TestControlExprTypes(c *test.C) {
	var response []interface{}
	query := r.Expr([]interface{}{int64(1), uint64(1), float64(1.0), int32(1), uint32(1), float32(1), "1", true, false})
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.All(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{int64(1), uint64(1), float64(1.0), int32(1), uint32(1), float32(1), "1", true, false})
}

func (s *RethinkSuite) TestControlJs(c *test.C) {
	var response int
	query := r.JS("1;")
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
}

func (s *RethinkSuite) TestControlHttp(c *test.C) {
	if testing.Short() {
		c.Skip("-short set")
	}

	var response map[string]interface{}
	query := r.HTTP("httpbin.org/get?data=1")
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response["args"], JsonEquals, map[string]interface{}{
		"data": "1",
	})
}

func (s *RethinkSuite) TestControlError(c *test.C) {
	query := r.Error("An error occurred")
	err := query.Exec(session)
	c.Assert(err, test.NotNil)

	c.Assert(err, test.NotNil)
	c.Assert(err, test.FitsTypeOf, r.RQLUserError{})

	c.Assert(err.Error(), test.Equals, "gorethink: An error occurred in:\nr.Error(\"An error occurred\")")
}

func (s *RethinkSuite) TestControlDoNothing(c *test.C) {
	var response []interface{}
	query := r.Do([]interface{}{map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2}, map[string]interface{}{"a": 3}})
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.All(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []interface{}{map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2}, map[string]interface{}{"a": 3}})
}

func (s *RethinkSuite) TestControlArgs(c *test.C) {
	var response time.Time
	query := r.Time(r.Args(r.Expr([]interface{}{2014, 7, 12, "Z"})))
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response.Unix(), test.Equals, int64(1405123200))
}

func (s *RethinkSuite) TestControlBinaryByteArray(c *test.C) {
	var response []byte

	query := r.Binary([]byte("Hello World"))
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(bytes.Equal(response, []byte("Hello World")), test.Equals, true)
}

type byteArray []byte

func (s *RethinkSuite) TestControlBinaryByteArrayAlias(c *test.C) {
	var response []byte

	query := r.Binary(byteArray("Hello World"))
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(bytes.Equal(response, []byte("Hello World")), test.Equals, true)
}

func (s *RethinkSuite) TestControlBinaryByteSlice(c *test.C) {
	var response [5]byte

	query := r.Binary([5]byte{'h', 'e', 'l', 'l', 'o'})
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, [5]byte{'h', 'e', 'l', 'l', 'o'})
}

func (s *RethinkSuite) TestControlBinaryExpr(c *test.C) {
	var response []byte

	query := r.Expr([]byte("Hello World"))
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(bytes.Equal(response, []byte("Hello World")), test.Equals, true)
}

func (s *RethinkSuite) TestControlBinaryExprAlias(c *test.C) {
	var response []byte

	query := r.Expr(byteArray("Hello World"))
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(bytes.Equal(response, []byte("Hello World")), test.Equals, true)
}

func (s *RethinkSuite) TestControlBinaryTerm(c *test.C) {
	var response []byte

	query := r.Binary(r.Expr([]byte("Hello World")))
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(bytes.Equal(response, []byte("Hello World")), test.Equals, true)
}

func (s *RethinkSuite) TestControlBinaryElemTerm(c *test.C) {
	var response map[string]interface{}

	query := r.Expr(map[string]interface{}{
		"bytes": []byte("Hello World"),
	})
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(bytes.Equal(response["bytes"].([]byte), []byte("Hello World")), test.Equals, true)
}

func (s *RethinkSuite) TestExprInvalidType(c *test.C) {
	query := r.Expr(map[struct{ string }]string{})
	_, err := query.Run(session)
	c.Assert(err, test.NotNil)
}

func (s *RethinkSuite) TestRawQuery(c *test.C) {
	var response int
	query := r.RawQuery([]byte(`1`))
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, test.Equals, 1)
}

func (s *RethinkSuite) TestRawQuery_advanced(c *test.C) {
	var response []int
	// r.expr([1,2,3]).map(function(v) { return v.add(1)})
	query := r.RawQuery([]byte(`[38,[[2,[1,2,3]],[69,[[2,[25]],[24,[[10,[25]],1]]]]]]`))
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.All(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, []int{2, 3, 4})
}

func (s *RethinkSuite) TestTableChanges(c *test.C) {
	r.DB("test").TableDrop("changes").Exec(session)
	r.DB("test").TableCreate("changes").Exec(session)
	r.DB("test").Table("changes").Wait().Exec(session)

	var n int

	res, err := r.DB("test").Table("changes").Changes().Run(session)
	if err != nil {
		c.Fatal(err.Error())
	}
	c.Assert(res.Type(), test.Equals, "Feed")

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Use goroutine to wait for changes. Prints the first 10 results
	go func() {
		var response interface{}
		for n < 10 && res.Next(&response) {
			n++
		}

		defer wg.Done()
		if res.Err() != nil {
			c.Fatal(res.Err())
		}
	}()

	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 1}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 2}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 3}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 4}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 5}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 6}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 7}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 8}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 9}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 10}).Exec(session)

	wg.Wait()

	c.Assert(n, test.Equals, 10)
}

func (s *RethinkSuite) TestTableChangesExit(c *test.C) {
	r.DB("test").TableDrop("changes").Exec(session)
	r.DB("test").TableCreate("changes").Exec(session)
	r.DB("test").Table("changes").Wait().Exec(session)

	var n int

	res, err := r.DB("test").Table("changes").Changes().Run(session)
	if err != nil {
		c.Fatal(err.Error())
	}
	c.Assert(res.Type(), test.Equals, "Feed")

	change := make(chan r.ChangeResponse)

	// Close cursor after one second
	go func() {
		<-time.After(time.Second)
		res.Close()
	}()

	// Insert 5 docs
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 1}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 2}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 3}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 4}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 5}).Exec(session)

	// Listen for changes
	res.Listen(change)
	for _ = range change {
		n++
	}

	c.Assert(n, test.Equals, 5)
}

func (s *RethinkSuite) TestTableChangesExitNoResults(c *test.C) {
	r.DB("test").TableDrop("changes").Exec(session)
	r.DB("test").TableCreate("changes").Exec(session)
	r.DB("test").Table("changes").Wait().Exec(session)

	var n int

	res, err := r.DB("test").Table("changes").Changes().Run(session)
	if err != nil {
		c.Fatal(err.Error())
	}
	c.Assert(res.Type(), test.Equals, "Feed")

	change := make(chan r.ChangeResponse)

	// Close cursor after one second
	go func() {
		<-time.After(time.Second)
		res.Close()
	}()

	// Listen for changes
	res.Listen(change)
	for _ = range change {
		n++
	}

	c.Assert(n, test.Equals, 0)
}

func (s *RethinkSuite) TestTableChangesIncludeInitial(c *test.C) {
	r.DB("test").TableDrop("changes").Exec(session)
	r.DB("test").TableCreate("changes").Exec(session)
	r.DB("test").Table("changes").Wait().Exec(session)

	// Insert 5 documents to table initially
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 1}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 2}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 3}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 4}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 5}).Exec(session)

	var n int

	res, err := r.DB("test").Table("changes").Changes(r.ChangesOpts{IncludeInitial: true}).Run(session)
	if err != nil {
		c.Fatal(err.Error())
	}
	c.Assert(res.Type(), test.Equals, "Feed")

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Use goroutine to wait for changes. Prints the first 10 results
	go func() {
		var response interface{}
		for n < 10 && res.Next(&response) {
			n++
		}

		defer wg.Done()
		if res.Err() != nil {
			c.Fatal(res.Err())
		}
	}()

	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 6}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 7}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 8}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 9}).Exec(session)
	r.DB("test").Table("changes").Insert(map[string]interface{}{"n": 10}).Exec(session)

	wg.Wait()

	c.Assert(n, test.Equals, 10)
}

func (s *RethinkSuite) TestWriteReference(c *test.C) {
	author := Author{
		ID:   "1",
		Name: "JRR Tolkien",
	}

	book := Book{
		ID:     "1",
		Title:  "The Lord of the Rings",
		Author: author,
	}

	r.DB("test").TableDrop("authors").Exec(session)
	r.DB("test").TableDrop("books").Exec(session)
	r.DB("test").TableCreate("authors").Exec(session)
	r.DB("test").TableCreate("books").Exec(session)
	r.DB("test").Table("authors").Wait().Exec(session)
	r.DB("test").Table("books").Wait().Exec(session)

	_, err := r.DB("test").Table("authors").Insert(author).RunWrite(session)
	c.Assert(err, test.IsNil)

	_, err = r.DB("test").Table("books").Insert(book).RunWrite(session)
	c.Assert(err, test.IsNil)

	// Read back book + author and check result
	cursor, err := r.DB("test").Table("books").Get("1").Merge(func(p r.Term) interface{} {
		return map[string]interface{}{
			"author_id": r.DB("test").Table("authors").Get(p.Field("author_id")),
		}
	}).Run(session)
	c.Assert(err, test.IsNil)

	var out Book
	err = cursor.One(&out)
	c.Assert(err, test.IsNil)

	c.Assert(out.Title, test.Equals, "The Lord of the Rings")
	c.Assert(out.Author.Name, test.Equals, "JRR Tolkien")
}

func (s *RethinkSuite) TestWriteConflict(c *test.C) {
	r.DB("test").TableDrop("test").Exec(session)
	r.DB("test").TableCreate("test").Exec(session)
	r.DB("test").Table("test").Wait().Exec(session)

	query := r.DB("test").Table("test").Insert(map[string]interface{}{"id": "a"})
	_, err := query.RunWrite(session)
	c.Assert(err, test.IsNil)

	query = r.DB("test").Table("test").Insert(map[string]interface{}{"id": "a"})
	_, err = query.RunWrite(session)
	c.Assert(r.IsConflictErr(err), test.Equals, true)
}

func (s *RethinkSuite) TestTimeTime(c *test.C) {
	var response time.Time
	res, err := r.Time(1986, 11, 3, 12, 30, 15, "Z").Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response.Equal(time.Date(1986, 11, 3, 12, 30, 15, 0, time.UTC)), test.Equals, true)
}

func (s *RethinkSuite) TestTimeExpr(c *test.C) {
	var response time.Time
	t := time.Unix(531360000, 0)
	res, err := r.Expr(r.Expr(t)).Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
}

func (s *RethinkSuite) TestTimeExprMillisecond(c *test.C) {
	var response time.Time
	t := time.Unix(531360000, 679000000)
	res, err := r.Expr(t).Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(float64(response.UnixNano()), test.Equals, float64(t.UnixNano()))
}

func (s *RethinkSuite) TestTimeISO8601(c *test.C) {
	var t1, t2 time.Time
	t2, _ = time.Parse("2006-01-02T15:04:05-07:00", "1986-11-03T08:30:00-07:00")
	res, err := r.ISO8601("1986-11-03T08:30:00-07:00").Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&t1)
	c.Assert(err, test.IsNil)
	c.Assert(t1.Equal(t2), test.Equals, true)
}

func (s *RethinkSuite) TestTimeInTimezone(c *test.C) {
	var response []time.Time
	res, err := r.Expr([]interface{}{r.Now(), r.Now().InTimezone("-07:00")}).Run(session)
	c.Assert(err, test.IsNil)

	err = res.All(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response[1].Equal(response[0]), test.Equals, true)
}

func (s *RethinkSuite) TestSelectJSONNumbers(c *test.C) {
	session, err := r.Connect(r.ConnectOpts{
		Address:       url,
		UseJSONNumber: true,
	})
	c.Assert(err, test.IsNil)
	defer session.Close()
	// Ensure table + database exist
	r.DBCreate("test").Exec(session)
	r.DB("test").TableCreate("Table1").Exec(session)
	r.DB("test").Table("Table1").Wait().Exec(session)

	// Insert rows
	r.DB("test").Table("Table1").Insert(objList).Exec(session)

	// Test query
	var response interface{}
	query := r.DB("test").Table("Table1").Get(6)
	res, err := query.Run(session)
	c.Assert(err, test.IsNil)

	err = res.One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response, JsonEquals, map[string]interface{}{"id": json.Number("6"), "g1": json.Number("1"), "g2": json.Number("1"), "num": json.Number("15")})

	res.Close()
}

func (s *RethinkSuite) TestSelectManyRows(c *test.C) {
	// Ensure table + database exist
	r.DBCreate("test").Exec(session)
	r.DB("test").TableCreate("TestMany").Exec(session)
	r.DB("test").Table("TestMany").Wait().Exec(session)
	r.DB("test").Table("TestMany").Delete().Exec(session)

	// Insert rows
	for i := 0; i < 100; i++ {
		data := []interface{}{}

		for j := 0; j < 100; j++ {
			data = append(data, map[string]interface{}{
				"i": i,
				"j": j,
			})
		}

		r.DB("test").Table("TestMany").Insert(data).Exec(session)
	}

	// Test query
	res, err := r.DB("test").Table("TestMany").Run(session, r.RunOpts{
		MaxBatchRows: 1,
	})
	c.Assert(err, test.IsNil)

	var n int
	var response map[string]interface{}
	for res.Next(&response) {
		n++
	}

	c.Assert(res.Err(), test.IsNil)
	c.Assert(n, test.Equals, 10000)

	res.Close()
}
