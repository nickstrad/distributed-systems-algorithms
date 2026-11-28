BIN := bin/dsasim
SEEDS ?= 50
SUITE ?= job-buffered
CONFIG ?=
PKG ?= ./sim
FUZZ ?= FuzzSimSchedule
FUZZTIME ?= 10s

.PHONY: help build test test-race test-unit test-prop test-e2e test-suite test-all-suites story report sweep fuzz vet fmt fmt-check check clean

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "; printf "\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the dsasim CLI binary into bin/dsasim
	@mkdir -p bin
	go build -o $(BIN) ./cmd/dsasim

test: ## Run all Go tests
	go test -count=1 ./...

test-race: ## Run all Go tests with race detector and statement coverage
	go test -race -cover -count=1 ./...

test-unit: ## Run fast unit tests (-short)
	go test -short -count=1 ./...

test-prop: ## Run all rapid property-based tests
	go test -race -count=1 -run '^TestProp' ./...

test-e2e: build ## Exercise the built CLI from JSON config through CSV report
	sh scripts/test-cli-e2e.sh

test-suite: build ## Run one algorithm suite (SUITE=name or CONFIG=path/to/config.json)
	@if [ -n "$(CONFIG)" ]; then \
		$(BIN) run --config "$(CONFIG)" --check-determinism; \
	else \
		$(BIN) run "$(SUITE)" --check-determinism; \
	fi

test-all-suites: build ## Run every config file under configs/*.json with determinism checks
	@for cfg in configs/*.json; do \
		echo "==> $$cfg"; \
		$(BIN) run --config "$$cfg" --check-determinism || exit 1; \
	done

story: build ## Run a suite and print its full narrative story + VERDICT table (SUITE=name or CONFIG=path)
	@if [ -n "$(CONFIG)" ]; then \
		$(BIN) run --config "$(CONFIG)" --story --check-determinism; \
	else \
		$(BIN) run "$(SUITE)" --story --check-determinism; \
	fi

report: build ## Render a narrative analysis report from a flushed CSV directory (CSV_DIR=path)
	@test -n "$(CSV_DIR)" || (echo "usage: make report CSV_DIR=path/to/run"; exit 2)
	$(BIN) report "$(CSV_DIR)"

sweep: ## Run multi-seed DST sweeps (override with SEEDS=100)
	DST_SEEDS=$(SEEDS) go test -count=1 ./...

fuzz: ## Run a Go fuzz target (PKG=./sim FUZZ=FuzzSimSchedule FUZZTIME=10s)
	go test -fuzz='^$(FUZZ)$$' -fuzztime=$(FUZZTIME) -run='^$$' $(PKG)

vet: ## Run go vet across all packages
	go vet ./...

fmt: ## Format all Go files with gofmt
	gofmt -w .

fmt-check: ## Fail if any Go file is not gofmt-formatted
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "Unformatted files:"; echo "$$unformatted"; exit 1; \
	fi

check: fmt-check vet test-race test-all-suites test-e2e ## Full local check: fmt, vet, race tests, all config suites, and CLI end-to-end

clean: ## Remove build artifacts
	rm -rf bin test-artifacts
