test:
	test -d ${GOPATH}/src/gopkg.in/rethinkdb/rethinkdb-go.v5 && mv ${GOPATH}/src/gopkg.in/rethinkdb/rethinkdb-go.v5 ${GOPATH}/src/gopkg.in/rethinkdb/rethinkdb-go.v5.bak; true
	cp -R . ${GOPATH}/src/gopkg.in/rethinkdb/rethinkdb-go.v5
	go test -coverprofile=cover.out -race gopkg.in/rethinkdb/rethinkdb-go.v5; true
	go tool cover -html=cover.out -o cover.html; true
	rm -f cover.out; true
	rm -rf ${GOPATH}/src/gopkg.in/rethinkdb/rethinkdb-go.v5
	test -d ${GOPATH}/src/gopkg.in/rethinkdb/rethinkdb-go.v5.bak && mv ${GOPATH}/src/gopkg.in/rethinkdb/rethinkdb-go.v5.bak ${GOPATH}/src/gopkg.in/rethinkdb/rethinkdb-go.v5; true
