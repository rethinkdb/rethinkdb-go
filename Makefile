test:
	test -d ${GOPATH}/src/gopkg.in/gorethink/gorethink.v3 && mv ${GOPATH}/src/gopkg.in/gorethink/gorethink.v3 ${GOPATH}/src/gopkg.in/gorethink/gorethink.v3.bak; true
	cp -R . ${GOPATH}/src/gopkg.in/gorethink/gorethink.v3
	go test -coverprofile=cover.out -race gopkg.in/gorethink/gorethink.v3; true
	go tool cover -html=cover.out -o cover.html; true
	rm -f cover.out; true
	rm -rf ${GOPATH}/src/gopkg.in/gorethink/gorethink.v3
	test -d ${GOPATH}/src/gopkg.in/gorethink/gorethink.v3.bak && mv ${GOPATH}/src/gopkg.in/gorethink/gorethink.v3.bak ${GOPATH}/src/gopkg.in/gorethink/gorethink.v3; true
