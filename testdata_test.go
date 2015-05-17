package gorethink

func setupTestData() {
	// Delete any preexisting databases
	DBDrop("test").Exec(session)
	DBDrop("superheroes").Exec(session)

	DBCreate("test").Exec(session)

	DB("test").TableCreate("posts").Exec(session)
	DB("test").TableCreate("heroes").Exec(session)
	DB("test").TableCreate("users").Exec(session)

	// Create heroes table
	DB("test").Table("heroes").Insert([]interface{}{
		map[string]interface{}{
			"id":        1,
			"code_name": "batman",
			"name":      "Batman",
		},
		map[string]interface{}{
			"id":        2,
			"code_name": "man_of_steel",
			"name":      "Superman",
		},
		map[string]interface{}{
			"id":        3,
			"code_name": "ant_man",
			"name":      "Ant Man",
		},
		map[string]interface{}{
			"id":        4,
			"code_name": "flash",
			"name":      "The Flash",
		},
	}).Exec(session)

	// Create users table
	DB("test").Table("users").Insert([]interface{}{
		map[string]interface{}{
			"id":    "william",
			"email": "william@rethinkdb.com",
			"age":   30,
		},
		map[string]interface{}{
			"id":    "lara",
			"email": "lara@rethinkdb.com",
			"age":   30,
		},
		map[string]interface{}{
			"id":    "john",
			"email": "john@rethinkdb.com",
			"age":   19,
		},
		map[string]interface{}{
			"id":    "jane",
			"email": "jane@rethinkdb.com",
			"age":   45,
		},
		map[string]interface{}{
			"id":    "bob",
			"email": "bob@rethinkdb.com",
			"age":   24,
		},
		map[string]interface{}{
			"id":    "brad",
			"email": "brad@gmail.com",
			"age":   15,
		},
	}).Exec(session)
}
