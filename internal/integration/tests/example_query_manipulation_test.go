package tests

import (
	"fmt"
	r "gopkg.in/gorethink/gorethink.v4"
)

// Get john's age
func ExampleTerm_Field() {
	cur, err := r.DB("examples").Table("users").Get("john").Field("age").Run(session)
	if err != nil {
		fmt.Print(err)
		return
	}

	var res int
	err = cur.One(&res)
	if err != nil {
		fmt.Print(err)
		return
	}

	fmt.Print(res)

	// Output: 19
}
