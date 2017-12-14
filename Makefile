test:
	test -d ${GOPATH}/src/gopkg.in/gorethink/gorethink.v4 && mv ${GOPATH}/src/gopkg.in/gorethink/gorethink.v4 ${GOPATH}/src/gopkg.in/gorethink/gorethink.v4.bak; true
	cp -R . ${GOPATH}/src/gopkg.in/gorethink/gorethink.v4
	go test -coverprofile=cover.out -race gopkg.in/gorethink/gorethink.v4; true
	go tool cover -html=cover.out -o cover.html; true
	rm -f cover.out; true
	rm -rf ${GOPATH}/src/gopkg.in/gorethink/gorethink.v4
	test -d ${GOPATH}/src/gopkg.in/gorethink/gorethink.v4.bak && mv ${GOPATH}/src/gopkg.in/gorethink/gorethink.v4.bak ${GOPATH}/src/gopkg.in/gorethink/gorethink.v4; true
