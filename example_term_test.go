package gorethink_test

import (
	r "github.com/dancannon/gorethink"
)

func ExampleTerm_Insert() {
	r.Table("user").Insert(map[string]interface{}{
		"name": "Micheal",
		"age":  26,
	}).RunWrite(session)
}

func ExampleTerm_Insert_multiple() {
	r.Table("user").Insert([]interface{}{
		map[string]interface{}{
			"name": "Micheal",
			"age":  26,
		},
		map[string]interface{}{
			"name": "Slava",
			"age":  30,
		},
	}).RunWrite(session)
}
