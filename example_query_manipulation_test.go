package gorethink

import (
	"fmt"
)

// Get john's age
func ExampleField() {
	cur, err := DB("examples").Table("users").Get("john").Field("age").Run(session)
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
