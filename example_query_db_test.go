package gorethink

func ExampleDBCreate() {
	// Setup database
	DBCreate("test").Run(session)
}
