package gorethink_test

import r "github.com/dancannon/gorethink"

func ExampleDbCreate() {
	// Setup database
	r.DbCreate("test").Run(session)

}
