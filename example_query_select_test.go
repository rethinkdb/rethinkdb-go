package gorethink_test

import (
	"fmt"
	"log"

	r "github.com/dancannon/gorethink"
)

func ExampleRqlTerm_Get() {
	type Person struct {
		Id        string `gorethink:"id, omitempty"`
		FirstName string `gorethink:"first_name"`
		LastName  string `gorethink:"last_name"`
		Gender    string `gorethink:"gender"`
	}

	sess, err := r.Connect(r.ConnectOpts{
		Address: url,
	})

	// Setup table
	r.Db("test").TableDrop("table").Run(sess)
	r.Db("test").TableCreate("table").Run(sess)
	r.Db("test").Table("table").Insert(Person{"1", "John", "Smith", "M"}).Run(sess)

	// Fetch the row from the database
	row, err := r.Db("test").Table("table").Get("1").RunRow(sess)
	if err != nil {
		log.Fatalf("Error finding person: %s", err)
	}

	if row.IsNil() {
		log.Fatalf("Person not found")
	}

	// Scan query result into the person variable
	var person Person
	err = row.Scan(&person)
	if err != nil {
		log.Fatalf("Error scanning database result: %s", err)
	}
	fmt.Printf("%s %s (%s)", person.FirstName, person.LastName, person.Gender)

	// Output:
	// John Smith (M)
}

func ExampleRqlTerm_GetAll_compound() {
	type Person struct {
		Id        string `gorethink:"id, omitempty"`
		FirstName string `gorethink:"first_name"`
		LastName  string `gorethink:"last_name"`
		Gender    string `gorethink:"gender"`
	}

	sess, err := r.Connect(r.ConnectOpts{
		Address: url,
	})

	// Setup table
	r.Db("test").TableDrop("table").Run(sess)
	r.Db("test").TableCreate("table").Run(sess)
	r.Db("test").Table("table").Insert(Person{"1", "John", "Smith", "M"}).Run(sess)
	r.Db("test").Table("table").IndexCreateFunc("full_name", func(row r.RqlTerm) interface{} {
		return []interface{}{row.Field("first_name"), row.Field("last_name")}
	}).Run(sess)
	r.Db("test").Table("table").IndexWait().Run(sess)

	// Fetch the row from the database
	row, err := r.Db("test").Table("table").GetAllByIndex("full_name", []interface{}{"John", "Smith"}).RunRow(sess)
	if err != nil {
		log.Fatalf("Error finding person: %s", err)
	}

	if row.IsNil() {
		log.Fatalf("Person not found")
	}

	// Scan query result into the person variable
	var person Person
	row.Scan(&person)

	fmt.Printf("%s %s (%s)", person.FirstName, person.LastName, person.Gender)

	// Output:
	// John Smith (M)
}
