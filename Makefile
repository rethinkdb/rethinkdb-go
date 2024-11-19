test:
	go test -v -coverprofile=cover.out -race gopkg.in/rethinkdb/rethinkdb-go.v6 gopkg.in/rethinkdb/rethinkdb-go.v6/encoding gopkg.in/rethinkdb/rethinkdb-go.v6/types
	go tool cover -html=cover.out -o cover.html
	rm -f cover.out

integration:
	go test -v -race gopkg.in/rethinkdb/rethinkdb-go.v6/internal/integration/...

benchpool:
	go test -v -cpu 1,2,4,8,16,24,32,64,128,256 -bench=BenchmarkConnectionPool -run ^$ ./internal/integration/tests/

generate:
	go generate ./internal/...

