package gorethink

import (
	"fmt"
)

func Example() {
	res, err := Expr("Hello World").Run(sess)
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
