package gorethink

import (
	"fmt"
)

func Example() {
	session, err := Connect(ConnectOpts{
		Address: url,
		AuthKey: authKey,
	})
	if err != nil {
		log.Fatalf("Error connecting to DB: %s", err)
	}

	res, err := Expr("Hello World").Run(session)
	if err != nil {
		log.Fatalln(err.Error())
	}

	var response string
	err = res.One(&response)
	if err != nil {
		log.Fatalln(err.Error())
	}

	fmt.Println(response)
}
