package tests

import (
	"fmt"

	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

// Create a table named "table" with the default settings.
func ExampleTerm_TableCreate() {
	// Setup database
	r.DB("examples").TableDrop("table").Run(session)

	response, err := r.DB("examples").TableCreate("table").RunWrite(session)
	if err != nil {
		panic(fmt.Errorf("Error creating table: %w", err))
	}

	fmt.Printf("%d table created", response.TablesCreated)

	// Output:
	// 1 table created
}

// Create a simple index based on the field name.
func ExampleTerm_IndexCreate() {
	// Setup database
	r.DB("examples").TableDrop("table").Run(session)
	r.DB("examples").TableCreate("table").Run(session)

	response, err := r.DB("examples").Table("table").IndexCreate("name").RunWrite(session)
	if err != nil {
		panic(fmt.Errorf("Error creating index: %w", err))
	}

	fmt.Printf("%d index created", response.Created)

	// Output:
	// 1 index created
}

// Create a compound index based on the fields first_name and last_name.
func ExampleTerm_IndexCreate_compound() {
	// Setup database
	r.DB("examples").TableDrop("table").Run(session)
	r.DB("examples").TableCreate("table").Run(session)

	response, err := r.DB("examples").Table("table").IndexCreateFunc("full_name", func(row r.Term) interface{} {
		return []interface{}{row.Field("first_name"), row.Field("last_name")}
	}).RunWrite(session)
	if err != nil {
		panic(fmt.Errorf("Error creating index: %w", err))
	}

	fmt.Printf("%d index created", response.Created)

	// Output:
	// 1 index created
}
