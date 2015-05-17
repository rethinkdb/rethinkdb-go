package gorethink

func ExampleDBCreate() {
	// Setup database
	DBCreate("test").Run(sess)
}
