GOCACHE ?= $(CURDIR)/.gocache
BIN_DIR ?= $(CURDIR)/bin
DOCKER ?= docker

.PHONY: test test-scripts prepush generate-openapi generate-openapi-check build build-api build-controller build-agent build-drain-agent build-telemetry-agent build-observability-pilot build-image-cache build-edge build-dns build-cli build-app-ssh-image run-api run-controller run-agent run-telemetry-agent

test:
	bash ./scripts/scan_hardcoded_production_facts.sh
	python3 -m scripts.test_prepush
	python3 ./scripts/test_verify_registry_image.py
	env GOCACHE=$(GOCACHE) go test ./internal/declarativerelease ./cmd/fugue-declarative-release
	env GOCACHE=$(GOCACHE) go run ./cmd/fugue-openapi-gen -spec openapi/openapi.yaml -routes-out internal/api/routes_gen.go -spec-out internal/apispec/spec_gen.go -check
	bash ./scripts/test_render_fugue_systemd_units.sh
	bash ./scripts/test_probe_fugue_public_dns.sh
	bash ./scripts/test_traffic_safety_stage0.sh
	python3 ./scripts/test_validate_managed_app_crd_transition.py
	env GOCACHE=$(GOCACHE) go test ./...

test-scripts:
	python3 -m scripts.test_prepush
	python3 ./scripts/test_verify_registry_image.py
	bash ./scripts/test_render_fugue_systemd_units.sh
	bash ./scripts/test_probe_fugue_public_dns.sh
	bash ./scripts/test_traffic_safety_stage0.sh
	python3 ./scripts/test_validate_managed_app_crd_transition.py

prepush:
	@python3 ./scripts/prepush.py

generate-openapi:
	env GOCACHE=$(GOCACHE) go generate ./internal/apispec

generate-openapi-check:
	env GOCACHE=$(GOCACHE) go run ./cmd/fugue-openapi-gen -spec openapi/openapi.yaml -routes-out internal/api/routes_gen.go -spec-out internal/apispec/spec_gen.go -check

build: build-api build-controller build-agent build-drain-agent build-telemetry-agent build-observability-pilot build-image-cache build-edge build-dns build-cli

build-api:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-api ./cmd/fugue-api

build-controller:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-controller ./cmd/fugue-controller

build-agent:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-agent ./cmd/fugue-agent

build-drain-agent:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-drain-agent ./cmd/fugue-drain-agent

build-telemetry-agent:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-telemetry-agent ./cmd/fugue-telemetry-agent

build-observability-pilot:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-observability-pilot ./cmd/fugue-observability-pilot

build-image-cache:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-image-cache ./cmd/fugue-image-cache

build-edge:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-edge ./cmd/fugue-edge
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-edge-front ./cmd/fugue-edge-front
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-ssh-front ./cmd/fugue-ssh-front

build-dns:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue-dns ./cmd/fugue-dns

build-cli:
	mkdir -p $(BIN_DIR)
	env GOCACHE=$(GOCACHE) go build -o $(BIN_DIR)/fugue ./cmd/fugue

build-app-ssh-image:
	$(DOCKER) build -f Dockerfile.app-ssh -t fugue-app-ssh:dev .

run-api: build-api
	FUGUE_BIND_ADDR=:8080 $(BIN_DIR)/fugue-api

run-controller: build-controller
	$(BIN_DIR)/fugue-controller

run-agent: build-agent
	$(BIN_DIR)/fugue-agent

run-telemetry-agent: build-telemetry-agent
	$(BIN_DIR)/fugue-telemetry-agent
