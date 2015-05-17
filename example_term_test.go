package gorethink

func ExampleTerm_Insert() {
	Table("user").Insert(map[string]interface{}{
		"name": "Micheal",
		"age":  26,
	}).RunWrite(sess)
}

func ExampleTerm_Insert_multiple() {
	Table("user").Insert([]interface{}{
		map[string]interface{}{
			"name": "Micheal",
			"age":  26,
		},
		map[string]interface{}{
			"name": "Slava",
			"age":  30,
		},
	}).RunWrite(sess)
}
