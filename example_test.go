package gorethink_test

import (
	"fmt"
	"log"

	r "github.com/dancannon/gorethink"
)

func Example() {
	session, err := r.Connect(ConnectOpts{
		Address: url,
	})
	if err != nil {
		log.Fatalln(err)
	}

	res, err := Expr("Hello World").Run(session)
	if err != nil {
		log.Fatalln(err)
	}

	var response string
	err = res.One(&response)
	if err != nil {
		Log.Fatalln(err)
	}

	fmt.Println(response)

	// Output:
	// Hello World
}
