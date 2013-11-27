package gorethink

import (
	test "launchpad.net/gocheck"
	"time"
)

func (s *RethinkSuite) TestTimeTime(c *test.C) {
	var response time.Time
	row, err := Time(1986, 11, 3, 12, 30, 15, "Z").RunRow(sess)
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response.Equal(time.Date(1986, 11, 3, 12, 30, 15, 0, time.UTC)), test.Equals, true)
}

func (s *RethinkSuite) TestTimeTimeMillisecond(c *test.C) {
	var response time.Time
	row, err := Time(1986, 11, 3, 12, 30, 15.679, "Z").RunRow(sess)
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response.Equal(time.Date(1986, 11, 3, 12, 30, 15, 679.00002*1000*1000, time.UTC)), test.Equals, true)
}

func (s *RethinkSuite) TestTimeEpochTime(c *test.C) {
	var response time.Time
	row, err := EpochTime(531360000).RunRow(sess)
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response.Equal(time.Date(1986, 11, 3, 0, 0, 0, 0, time.UTC)), test.Equals, true)
}

func (s *RethinkSuite) TestTimeISO8601(c *test.C) {
	var t1, t2 time.Time
	t2, _ = time.Parse("2006-01-02T15:04:05-07:00", "1986-11-03T08:30:00-07:00")
	row, err := ISO8601("1986-11-03T08:30:00-07:00").RunRow(sess)
	c.Assert(err, test.IsNil)

	err = row.Scan(&t1)
	c.Assert(err, test.IsNil)
	c.Assert(t1.Equal(t2), test.Equals, true)
}

func (s *RethinkSuite) TestTimeInTimezone(c *test.C) {
	loc, err := time.LoadLocation("MST")
	c.Assert(err, test.IsNil)
	var response []time.Time
	row, err2 := Expr([]interface{}{Now(), Now().InTimezone("-07:00")}).Run(sess)
	c.Assert(err2, test.IsNil)

	err = row.ScanAll(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response[1].Equal(response[0].In(loc)), test.Equals, true)
}

func (s *RethinkSuite) TestTimeBetween(c *test.C) {
	var response interface{}

	times := Expr([]interface{}{
		Time(1986, 9, 3, 12, 30, 15, "Z"),
		Time(1986, 10, 3, 12, 30, 15, "Z"),
		Time(1986, 11, 3, 12, 30, 15, "Z"),
		Time(1986, 12, 3, 12, 30, 15, "Z"),
	})
	row, err := times.Filter(func(row RqlTerm) RqlTerm {
		return row.During(Time(1986, 9, 3, 12, 30, 15, "Z"), Time(1986, 11, 3, 12, 30, 15, "Z"))
	}).Count().RunRow(sess)
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(int(response.(float64)), test.Equals, 2)
}

func (s *RethinkSuite) TestTimeYear(c *test.C) {
	var response interface{}

	row, err := Time(1986, 12, 3, 12, 30, 15, "Z").Year().RunRow(sess)
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(int(response.(float64)), test.Equals, 1986)
}

func (s *RethinkSuite) TestTimeMonth(c *test.C) {
	var response interface{}

	row, err := Time(1986, 12, 3, 12, 30, 15, "Z").Month().Eq(December()).RunRow(sess)
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response.(bool), test.Equals, true)
}

func (s *RethinkSuite) TestTimeDay(c *test.C) {
	var response interface{}

	row, err := Time(1986, 12, 3, 12, 30, 15, "Z").Day().Eq(Wednesday()).RunRow(sess)
	c.Assert(err, test.IsNil)

	err = row.Scan(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response.(bool), test.Equals, true)
}
