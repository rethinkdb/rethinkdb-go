package gorethink

import (
	"fmt"
)

func Example_Get() {
	type Person struct {
		ID        string `gorethink:"id, omitempty"`
		FirstName string `gorethink:"first_name"`
		LastName  string `gorethink:"last_name"`
		Gender    string `gorethink:"gender"`
	}

	sess, err := Connect(ConnectOpts{
		Address: url,
		AuthKey: authKey,
	})
	if err != nil {
		log.Fatalf("Error connecting to DB: %s", err)
	}

	// Setup table
	DB("test").TableDrop("table").Run(sess)
	DB("test").TableCreate("table").Run(sess)
	DB("test").Table("table").Insert(Person{"1", "John", "Smith", "M"}).Run(sess)

	// Fetch the row from the database
	res, err := DB("test").Table("table").Get("1").Run(sess)
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

func Example_GetAll_Compound() {
	type Person struct {
		ID        string `gorethink:"id, omitempty"`
		FirstName string `gorethink:"first_name"`
		LastName  string `gorethink:"last_name"`
		Gender    string `gorethink:"gender"`
	}

	sess, err := Connect(ConnectOpts{
		Address: url,
		AuthKey: authKey,
	})
	if err != nil {
		log.Fatalf("Error connecting to DB: %s", err)
	}

	// Setup table
	DB("test").TableDrop("table").Run(sess)
	DB("test").TableCreate("table").Run(sess)
	DB("test").Table("table").Insert(Person{"1", "John", "Smith", "M"}).Run(sess)
	DB("test").Table("table").IndexCreateFunc("full_name", func(row Term) interface{} {
		return []interface{}{row.Field("first_name"), row.Field("last_name")}
	}).Run(sess)
	DB("test").Table("table").IndexWait().Run(sess)

	// Fetch the row from the database
	res, err := DB("test").Table("table").GetAllByIndex("full_name", []interface{}{"John", "Smith"}).Run(sess)
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
