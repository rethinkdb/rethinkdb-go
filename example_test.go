package gorethink_test

import (
	"fmt"
	"log"
	"os"

	r "github.com/dancannon/gorethink"
)

var session *r.Session
var url string

func init() {
	// Needed for wercker. By default url is "localhost:28015"
	url = os.Getenv("WERCKER_RETHINKDB_URL")
	if url == "" {
		url = "localhost:28015"
	}
}

func Example() {
	session, err := r.Connect(r.ConnectOpts{
		Address: url,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	row, err := r.Expr("Hello World").RunRow(session)
	if err != nil {
		log.Fatalln(err.Error())
	}

	var response string
	err = row.Scan(&response)
	if err != nil {
		log.Fatalln(err.Error())
	}

	fmt.Println(response)
}
