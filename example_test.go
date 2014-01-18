package gorethink_test

import (
	"fmt"
	r "github.com/dancannon/gorethink"
	"log"
	"os"
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
	session, err := r.Connect(map[string]interface{}{
		"address": url,
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
