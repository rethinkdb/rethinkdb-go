package gorethink

import (
	"fmt"
)

func ExampleTerm_Get() {
	type Person struct {
		ID        string `gorethink:"id, omitempty"`
		FirstName string `gorethink:"first_name"`
		LastName  string `gorethink:"last_name"`
		Gender    string `gorethink:"gender"`
	}

	// Setup table
	DB("test").TableDrop("table").Run(session)
	DB("test").TableCreate("table").Run(session)
	DB("test").Table("table").Insert(Person{"1", "John", "Smith", "M"}).Run(session)

	// Fetch the row from the database
	res, err := DB("test").Table("table").Get("1").Run(session)
	if err != nil {
		log.Fatalf("Error finding person: %s", err)
	}

	if res.IsNil() {
		log.Fatalf("Person not found")
	}

	// Scan query result into the person variable
	var person Person
	err = res.One(&person)
	if err != nil {
		log.Fatalf("Error scanning database result: %s", err)
	}
	fmt.Printf("%s %s (%s)", person.FirstName, person.LastName, person.Gender)

	// Output:
	// John Smith (M)
}

func ExampleTerm_GetAll_compound() {
	type Person struct {
		ID        string `gorethink:"id, omitempty"`
		FirstName string `gorethink:"first_name"`
		LastName  string `gorethink:"last_name"`
		Gender    string `gorethink:"gender"`
	}

	// Setup table
	DB("test").TableDrop("table").Run(session)
	DB("test").TableCreate("table").Run(session)
	DB("test").Table("table").Insert(Person{"1", "John", "Smith", "M"}).Run(session)
	DB("test").Table("table").IndexCreateFunc("full_name", func(row Term) interface{} {
		return []interface{}{row.Field("first_name"), row.Field("last_name")}
	}).Run(session)
	DB("test").Table("table").IndexWait().Run(session)

	// Fetch the row from the database
	res, err := DB("test").Table("table").GetAllByIndex("full_name", []interface{}{"John", "Smith"}).Run(session)
	if err != nil {
		log.Fatalf("Error finding person: %s", err)
	}

	if res.IsNil() {
		log.Fatalf("Person not found")
	}

	// Scan query result into the person variable
	var person Person
	err = res.One(&person)
	if err == ErrEmptyResult {
		log.Fatalf("Person not found")
	} else if err != nil {
		log.Fatalf("Error scanning database result: %s", err)
	}

	fmt.Printf("%s %s (%s)", person.FirstName, person.LastName, person.Gender)

	// Output:
	// John Smith (M)
}
