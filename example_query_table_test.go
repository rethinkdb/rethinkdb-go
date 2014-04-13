package gorethink_test

import (
	"fmt"
	"log"

	r "github.com/dancannon/gorethink"
)

func ExampleRqlTerm_TableCreate() {
	sess, err := r.Connect(r.ConnectOpts{
		Address: url,
	})

	// Setup database
	r.Db("test").TableDrop("table").Run(sess)

	response, err := r.Db("test").TableCreate("table").RunWrite(sess)
	if err != nil {
		log.Fatalf("Error creating table: %s", err)
	}

	fmt.Printf("%d table created", response.Created)

	// Output:
	// 1 table created
}

func ExampleRqlTerm_IndexCreate() {
	sess, err := r.Connect(r.ConnectOpts{
		Address: url,
	})

	// Setup database
	r.Db("test").TableDrop("table").Run(sess)
	r.Db("test").TableCreate("table").Run(sess)

	response, err := r.Db("test").Table("table").IndexCreate("name").RunWrite(sess)
	if err != nil {
		log.Fatalf("Error creating index: %s", err)
	}

	fmt.Printf("%d index created", response.Created)

	// Output:
	// 1 index created
}

func ExampleRqlTerm_IndexCreate_compound() {
	sess, err := r.Connect(r.ConnectOpts{
		Address: url,
	})

	// Setup database
	r.Db("test").TableDrop("table").Run(sess)
	r.Db("test").TableCreate("table").Run(sess)

	response, err := r.Db("test").Table("table").IndexCreateFunc("full_name", func(row r.RqlTerm) interface{} {
		return []interface{}{row.Field("first_name"), row.Field("last_name")}
	}).RunWrite(sess)
	if err != nil {
		log.Fatalf("Error creating index: %s", err)
	}

	fmt.Printf("%d index created", response.Created)

	// Output:
	// 1 index created
}
