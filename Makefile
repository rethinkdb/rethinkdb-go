test: ut it

ut:
	go test -coverprofile=cover.out -race . ./encoding
	go tool cover -html=cover.out -o cover.html
	rm -f cover.out

it:
	go test -race ./internal/integration/reql_tests ./internal/integration/tests

bench:
	# better run with rethinkdb tmpfs
	go test -bench=. -benchmem ./internal/integration/benchmark

fmt:
	go fmt ./...
