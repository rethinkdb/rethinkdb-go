MAKEFLAGS += --no-print-directory
BUF_VERSION := 1.47.2

all:: protoc

_buf:
	docker run --rm \
		--entrypoint sh \
		-v $(PWD)/..:/workspace \
		-w /workspace/ql2 \
		bufbuild/buf:$(BUF_VERSION) \
		-c "buf --timeout 5m $(CMD) && chown -R $(shell id -u):$(shell id -g) /workspace"

.PHONY: protolint
protolint:
	@$(MAKE)	_buf	CMD="lint -v"

.PHONY: protofmt
protofmt:
	@$(MAKE)	_buf	CMD="format -w ."

.PHONY: protoc
protoc: protofmt
	@$(MAKE)	_buf	CMD="generate -v"
