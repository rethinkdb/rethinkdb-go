package gorethink_test

import (
	"fmt"
	r "github.com/dancannon/gorethink"
	"log"
)

var session *r.Session

func Example() {
	session, err := r.Connect(map[string]interface{}{
		"address": "localhost:28015",
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
