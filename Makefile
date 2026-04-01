GOCACHE ?= $(CURDIR)/.gocache
BIN_DIR ?= $(CURDIR)/bin

.PHONY: test generate-openapi generate-openapi-check build build-api build-controller build-agent build-cli run-api run-controller run-agent

test:
	env GOCACHE=$(GOCACHE) go run ./cmd/fugue-openapi-gen -spec openapi/openapi.yaml -routes-out internal/api/routes_gen.go -spec-out internal/apispec/spec_gen.go -check
	env GOCACHE=$(GOCACHE) go test ./...

generate-openapi:
	env GOCACHE=$(GOCACHE) go generate ./internal/apispec

generate-openapi-check:
	env GOCACHE=$(GOCACHE) go run ./cmd/fugue-openapi-gen -spec openapi/openapi.yaml -routes-out internal/api/routes_gen.go -spec-out internal/apispec/spec_gen.go -check

build: build-api build-controller build-agent build-cli

build-api:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-api ./cmd/fugue-api

build-controller:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-controller ./cmd/fugue-controller

build-agent:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-agent ./cmd/fugue-agent

build-cli:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue ./cmd/fugue

run-api: build-api
	FUGUE_BIND_ADDR=:8080 $(BIN_DIR)/fugue-api

run-controller: build-controller
	$(BIN_DIR)/fugue-controller

run-agent: build-agent
	$(BIN_DIR)/fugue-agent
