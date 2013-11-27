package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Returns a time object representing the current time in UTC
func Now() RqlTerm {
	return newRqlTerm("Now", p.Term_NOW, []interface{}{}, map[string]interface{}{})
}

// Create a time object for a specific time
func Time(year, month, day, hour, min, sec interface{}, tz string) RqlTerm {
	return newRqlTerm("Time", p.Term_TIME, []interface{}{year, month, day, hour, min, sec, tz}, map[string]interface{}{})
}

// Returns a time object based on seconds since epoch
func EpochTime(epochtime interface{}) RqlTerm {
	return newRqlTerm("EpochTime", p.Term_EPOCH_TIME, []interface{}{epochtime}, map[string]interface{}{})
}

type ISO8601Opts struct {
	DefaultTimezone interface{} `gorethink:"default_timezone,omitempty"`
}

func (o *ISO8601Opts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Returns a time object based on an ISO8601 formatted date-time string
//
// Optional arguments (see http://www.rethinkdb.com/api/#js:dates_and_times-iso8601 for more information):
// "default_timezone" (string)
func ISO8601(date interface{}, optArgs ...ISO8601Opts) RqlTerm {

	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTerm("ISO8601", p.Term_ISO8601, []interface{}{date}, opts)
}

// Returns a new time object with a different time zone. While the time
// stays the same, the results returned by methods such as hours() will
// change since they take the timezone into account. The timezone argument
// has to be of the ISO 8601 format.
func (t RqlTerm) InTimezone(tz interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "InTimezone", p.Term_IN_TIMEZONE, []interface{}{tz}, map[string]interface{}{})
}

// Returns the timezone of the time object
func (t RqlTerm) Timezone() RqlTerm {
	return newRqlTermFromPrevVal(t, "Timezone", p.Term_TIMEZONE, []interface{}{}, map[string]interface{}{})
}

type DuringOpts struct {
	LeftBound  interface{} `gorethink:"left_bound,omitempty"`
	RightBound interface{} `gorethink:"right_bound,omitempty"`
}

func (o *DuringOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Returns true if a time is between two other times
// (by default, inclusive for the start, exclusive for the end).
//
// Optional arguments (see http://www.rethinkdb.com/api/#js:dates_and_times-during for more information):
// "left_bound" and "right_bound" ("open" for exclusive or "closed" for inclusive)
func (t RqlTerm) During(startTime, endTime interface{}, optArgs ...DuringOpts) RqlTerm {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTermFromPrevVal(t, "During", p.Term_DURING, []interface{}{startTime, endTime}, opts)
}

// Return a new time object only based on the day, month and year
// (ie. the same day at 00:00).
func (t RqlTerm) Date() RqlTerm {
	return newRqlTermFromPrevVal(t, "Date", p.Term_DATE, []interface{}{}, map[string]interface{}{})
}

// Return the number of seconds elapsed since the beginning of the
// day stored in the time object.
func (t RqlTerm) TimeOfDay() RqlTerm {
	return newRqlTermFromPrevVal(t, "TimeOfDay", p.Term_TIME_OF_DAY, []interface{}{}, map[string]interface{}{})
}

// Return the year of a time object.
func (t RqlTerm) Year() RqlTerm {
	return newRqlTermFromPrevVal(t, "Year", p.Term_YEAR, []interface{}{}, map[string]interface{}{})
}

// Return the month of a time object as a number between 1 and 12.
// For your convenience, the terms r.January(), r.February() etc. are
// defined and map to the appropriate integer.
func (t RqlTerm) Month() RqlTerm {
	return newRqlTermFromPrevVal(t, "Month", p.Term_MONTH, []interface{}{}, map[string]interface{}{})
}

// Return the day of a time object as a number between 1 and 31.
func (t RqlTerm) Day() RqlTerm {
	return newRqlTermFromPrevVal(t, "Day", p.Term_DAY, []interface{}{}, map[string]interface{}{})
}

// Return the day of week of a time object as a number between
// 1 and 7 (following ISO 8601 standard). For your convenience,
// the terms r.Monday(), r.Tuesday() etc. are defined and map to
// the appropriate integer.
func (t RqlTerm) DayOfWeek() RqlTerm {
	return newRqlTermFromPrevVal(t, "DayOfWeek", p.Term_DAY_OF_WEEK, []interface{}{}, map[string]interface{}{})
}

// Return the day of the year of a time object as a number between
// 1 and 366 (following ISO 8601 standard).
func (t RqlTerm) DayOfYear() RqlTerm {
	return newRqlTermFromPrevVal(t, "DayOfYear", p.Term_DAY_OF_YEAR, []interface{}{}, map[string]interface{}{})
}

// Return the hour in a time object as a number between 0 and 23.
func (t RqlTerm) Hours() RqlTerm {
	return newRqlTermFromPrevVal(t, "Hours", p.Term_HOURS, []interface{}{}, map[string]interface{}{})
}

// Return the minute in a time object as a number between 0 and 59.
func (t RqlTerm) Minutes() RqlTerm {
	return newRqlTermFromPrevVal(t, "Minutes", p.Term_MINUTES, []interface{}{}, map[string]interface{}{})
}

// Return the seconds in a time object as a number between 0 and
// 59.999 (double precision).
func (t RqlTerm) Seconds() RqlTerm {
	return newRqlTermFromPrevVal(t, "Seconds", p.Term_SECONDS, []interface{}{}, map[string]interface{}{})
}

// Convert a time object to its iso 8601 format.
func (t RqlTerm) ToISO8601() RqlTerm {
	return newRqlTermFromPrevVal(t, "ToISO8601", p.Term_TO_ISO8601, []interface{}{}, map[string]interface{}{})
}

// Convert a time object to its epoch time.
func (t RqlTerm) ToEpochTime() RqlTerm {
	return newRqlTermFromPrevVal(t, "ToEpochTime", p.Term_TO_EPOCH_TIME, []interface{}{}, map[string]interface{}{})
}

// Days
func Monday() RqlTerm {
	return newRqlTerm("Monday", p.Term_MONDAY, []interface{}{}, map[string]interface{}{})
}
func Tuesday() RqlTerm {
	return newRqlTerm("Tuesday", p.Term_TUESDAY, []interface{}{}, map[string]interface{}{})
}
func Wednesday() RqlTerm {
	return newRqlTerm("Wednesday", p.Term_WEDNESDAY, []interface{}{}, map[string]interface{}{})
}
func Thursday() RqlTerm {
	return newRqlTerm("Thursday", p.Term_THURSDAY, []interface{}{}, map[string]interface{}{})
}
func Friday() RqlTerm {
	return newRqlTerm("Friday", p.Term_FRIDAY, []interface{}{}, map[string]interface{}{})
}
func Saturday() RqlTerm {
	return newRqlTerm("Saturday", p.Term_SATURDAY, []interface{}{}, map[string]interface{}{})
}
func Sunday() RqlTerm {
	return newRqlTerm("Sunday", p.Term_SUNDAY, []interface{}{}, map[string]interface{}{})
}

// Months
func January() RqlTerm {
	return newRqlTerm("January", p.Term_JANUARY, []interface{}{}, map[string]interface{}{})
}
func February() RqlTerm {
	return newRqlTerm("February", p.Term_FEBRUARY, []interface{}{}, map[string]interface{}{})
}
func March() RqlTerm {
	return newRqlTerm("March", p.Term_MARCH, []interface{}{}, map[string]interface{}{})
}
func April() RqlTerm {
	return newRqlTerm("April", p.Term_APRIL, []interface{}{}, map[string]interface{}{})
}
func May() RqlTerm {
	return newRqlTerm("May", p.Term_MAY, []interface{}{}, map[string]interface{}{})
}
func June() RqlTerm {
	return newRqlTerm("June", p.Term_JUNE, []interface{}{}, map[string]interface{}{})
}
func July() RqlTerm {
	return newRqlTerm("July", p.Term_JULY, []interface{}{}, map[string]interface{}{})
}
func August() RqlTerm {
	return newRqlTerm("August", p.Term_AUGUST, []interface{}{}, map[string]interface{}{})
}
func September() RqlTerm {
	return newRqlTerm("September", p.Term_SEPTEMBER, []interface{}{}, map[string]interface{}{})
}
func October() RqlTerm {
	return newRqlTerm("October", p.Term_OCTOBER, []interface{}{}, map[string]interface{}{})
}
func November() RqlTerm {
	return newRqlTerm("November", p.Term_NOVEMBER, []interface{}{}, map[string]interface{}{})
}
func December() RqlTerm {
	return newRqlTerm("December", p.Term_DECEMBER, []interface{}{}, map[string]interface{}{})
}
