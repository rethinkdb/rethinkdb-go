// Package rethinkgo implements the RethinkDB API for Go.  RethinkDB
// (http://www.rethinkdb.com/) is an open-source distributed database in the
// style of MongoDB.
//
// If you haven't tried it out, it takes about a minute to setup and has a sweet
// web console.  Even runs on Mac OS X.
// (http://www.rethinkdb.com/docs/guides/quickstart/)
//
// If you are familiar with the RethinkDB API, this package should be
// straightforward to use.  If not, the docs for this package contain plenty of
// examples, but you may want to look through the RethinkDB tutorials
// (http://www.rethinkdb.com/docs/).
//
// Example usage:
//
//  package main
//
//  import (
//      "fmt"
//      r "github.com/christopherhesse/rethinkgo"
//  )
//
//  type Employee struct {
//      FirstName string
//      LastName  string
//      Job       string
//      Id        string `json:"id,omitempty"` // (will appear in json as "id", and not be sent if empty)
//  }
//
//  func main() {
//      // To access a RethinkDB database, you connect to it with the Connect function
//      session, err := r.Connect("localhost:28015", "company_info")
//      if err != nil {
//          fmt.Println("error connecting:", err)
//          return
//      }
//
//      var response []Employee
//      // Using .All(), we can read the entire response into a slice, without iteration
//      err = r.Table("employees").Run(session).All(&response)
//      if err != nil {
//          fmt.Println("err:", err)
//      } else {
//          fmt.Println("response:", response)
//      }
//
//      // If we want to iterate over each result individually, we can use the rows
//      // object as an iterator
//      rows := r.Table("employees").Run(session)
//      for rows.Next() {
//          var row Employee
//          if err = rows.Scan(&row); err != nil {
//              fmt.Println("err:", err)
//              break
//          }
//          fmt.Println("row:", row)
//      }
//      if err = rows.Err(); err != nil {
//          fmt.Println("err:", err)
//      }
//  }
//
// Besides this simple read query, you can run almost arbitrary expressions on
// the server, even Javascript code.  See the rest of these docs for more
// details.
package rethinkgo
