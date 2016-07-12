package gorethink_test

import (
	"log"

	r "github.com/dancannon/gorethink"
)

var session *r.Session

func ExampleConnect() {
	var err error

	session, err = r.Connect(r.ConnectOpts{
		Address: url,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func ExampleConnect_connectionPool() {
	var err error

	session, err = r.Connect(r.ConnectOpts{
		Address: url,
		MaxIdle: 10,
		MaxOpen: 10,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func ExampleConnect_cluster() {
	var err error

	session, err = r.Connect(r.ConnectOpts{
		Addresses: []string{url},
		//  Addresses: []string{url1, url2, url3, ...},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}
