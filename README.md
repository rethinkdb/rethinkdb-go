rethinkgo
=========

[Go language](http://golang.org/) driver for [RethinkDB](http://www.rethinkdb.com/) made by [Christopher Hesse](http://www.christopherhesse.com/)

Current supported RethinkDB version: 1.7.1

Installation
============

    go get -u github.com/christopherhesse/rethinkgo

If you do not have the [goprotobuf](https://code.google.com/p/goprotobuf/) runtime installed, it is required:

    brew install mercurial  # if you do not have mercurial installed
    go get code.google.com/p/goprotobuf/{proto,protoc-gen-go}


Example
===================

    package main

    import (
        "fmt"
        r "github.com/christopherhesse/rethinkgo"
    )

    type Employee struct {
        FirstName string
        LastName  string
        Job       string
        Id        string `json:"id,omitempty"` // (will appear in json as "id", and not be sent if empty)
    }

    func main() {
        // To access a RethinkDB database, you connect to it with the Connect function
        session, err := r.Connect("localhost:28015", "company_info")
        if err != nil {
            fmt.Println("error connecting:", err)
            return
        }

        var response []Employee
        // Using .All(), we can read the entire response into a slice, without iteration
        err = r.Table("employees").Run(session).All(&response)
        if err != nil {
            fmt.Println("err:", err)
        } else {
            fmt.Println("response:", response)
        }

        // If we want to iterate over each result individually, we can use the rows
        // object as an iterator
        rows := r.Table("employees").Run(session)
        for rows.Next() {
            var row Employee
            if err = rows.Scan(&row); err != nil {
                fmt.Println("err:", err)
                break
            }
            fmt.Println("row:", row)
        }
        if err = rows.Err(); err != nil {
            fmt.Println("err:", err)
        }
    }


Overview
========

The Go driver is most similar to the [official Javascript driver](http://www.rethinkdb.com/api/#js).

Most of the functions have the same names as in the Javascript driver, only with the first letter capitalized.  See [Go Driver Documentation](http://godoc.org/github.com/christopherhesse/rethinkgo) for examples and documentation for each function.

To use RethinkDB with this driver, you build a query using r.Table(), for example, and then call query.Run(session) to execute the query on the server and return an iterator for the results.

There are 3 convenience functions on the iterator if you don't want to iterate over the results, .One(&result) for a query that returns a single result, .All(&results) for multiple results, and .Exec() for a query that returns an empty response (for instance, r.TableCreate(string)) or to ignore the response.

The important types are r.Exp (for RethinkDB expressions), r.Query (interface for all queries, including expressions), r.List (used for Arrays, an alias for []interface{}), and r.Map (used for Objects, an alias for map[string]interface{}).

The function r.Expr() can take arbitrary structs and uses the "json" module to serialize them.  This means that structs can use the json.Marshaler interface (define a method MarshalJSON() on the struct).  Also, struct fields can also be annotated to specify their JSON equivalents:

    type MyStruct struct {
        MyField int `json:"my_field"` // (will appear in json as my_field)
        OtherField int // (will appear in json as OtherField)
    }

See the [json docs](http://golang.org/pkg/encoding/json/) for more information.

Changelog
=========

Changes for RethinkDB 1.7.1:
* Added Json() and a couple of changes, see http://rethinkdb.com/blog/1.7-release/

Changes for RethinkDB 1.6.1:
* Added a number of new functions, auth support etc, see http://rethinkdb.com/blog/1.6-release/
* Removed connection pooling (this didn't seem that helpful and may have caused problems for some users) you no longer need to use rows.Close(), and sessions should not be shared between goroutines.  Either make a new connection inside each goroutine or create a pool (with some channels or maybe a sync.Mutex).

Differences from official RethinkDB drivers
===========================================

* When running queries, getting results is a little different from the more dynamic languages.  .Run(*Session) returns a *Rows iterator object with the following methods that put the response into a variable `dest`, here's when you should use the different methods:
    * You want to iterate through the results of the query individually: rows.Next() and rows.Scan(&dest)
    * The query always returns a single response: .One(&dest)
    * The query returns a list of responses: .All(&dest)
    * The query returns an empty response (or you want to ignore the result): .Exec()
* No errors are generated when creating queries, only when running them, so Table(string) returns only an Exp instance, but sess.Run(Query).Err() will tell you if your query could not be serialized for the server.  To check just the serialization of the query before calling .Run(*Session), use .Check(*Session)
* Go does not have optional args, most optional args are either require or separate methods.
    * .Atomic(bool), .Overwrite(bool), .UseOutdated(bool) are methods on any Table() or other Exp (will apply to all tables, inserts, etc that have already been specified)
    * .TableCreate(string) has a variant called TableCreateWithSpec(TableSpec) which takes a TableSpec instance specifying the parameters for the table
* There's no r(attributeName) or row[attributeName] function call / item indexing to get attributes of the "current" row or a specific row respectively.  Instead, there is a .Attr() method on the global "Row" object (r.Row) and any row Expressions that can be used to access attributes.  Examples:

        r.Table("marvel").OuterJoin(r.Table("dc"),
            func(marvel, dc r.Exp) interface{} {
                return marvel.Attr("strength").Eq(dc.Attr("strength"))
            })

        r.Table("marvel").Map(r.Row.Attr("strength").Mul(2))
