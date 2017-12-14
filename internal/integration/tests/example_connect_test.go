package tests

import (
	"log"
	"os"

	r "gopkg.in/gorethink/gorethink.v4"
)

var sessionEx *r.Session
var urlEx string

func init() {
	// If the test is being run by wercker look for the rethink url
	urlEx = os.Getenv("RETHINKDB_URL")
	if urlEx == "" {
		urlEx = "localhost:28015"
	}
}

func ExampleConnect() {
	var err error

	sessionEx, err = r.Connect(r.ConnectOpts{
		Address: urlEx,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func ExampleConnect_connectionPool() {
	var err error

	sessionEx, err = r.Connect(r.ConnectOpts{
		Address:    urlEx,
		InitialCap: 10,
		MaxOpen:    10,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func ExampleConnect_cluster() {
	var err error

	sessionEx, err = r.Connect(r.ConnectOpts{
		Addresses: []string{urlEx},
		//  Addresses: []string{url1, url2, url3, ...},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}
