# Forge -- distributed job processing platform.
#
# Everything here works without Docker except the compose/bench-compose
# targets; unit tests need nothing running, integration tests need a local
# Postgres and Redis (see test-integration).

GO        ?= go
BIN_DIR   := bin
PKGS      := ./...

# Integration tests are env-gated, not build-tag-gated: a test that finds no
# FORGE_TEST_* env skips itself, so `go test ./...` is always safe to run.
TEST_DATABASE_URL ?= postgres://postgres:postgres@127.0.0.1:5432/forge_test?sslmode=disable
TEST_REDIS_ADDR   ?= 127.0.0.1:6379

.PHONY: build
build:
	$(GO) build -o $(BIN_DIR)/forge-api    ./cmd/forge-api
	$(GO) build -o $(BIN_DIR)/forge-worker ./cmd/forge-worker
	$(GO) build -o $(BIN_DIR)/forge-bench  ./cmd/forge-bench
	$(GO) build -o $(BIN_DIR)/forgectl     ./cmd/forgectl

.PHONY: test
test:
	$(GO) test -race $(PKGS)

.PHONY: test-integration
test-integration:
	FORGE_TEST_DATABASE_URL="$(TEST_DATABASE_URL)" \
	FORGE_TEST_REDIS_ADDR="$(TEST_REDIS_ADDR)" \
	$(GO) test -race -count=1 $(PKGS)

.PHONY: lint
lint:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi
	$(GO) vet $(PKGS)

.PHONY: tidy
tidy:
	$(GO) mod tidy

# Regenerate gRPC code (protoc + protoc-gen-go + protoc-gen-go-grpc must be
# installed; generated code is committed so day-to-day builds never need this).
.PHONY: proto
proto:
	protoc --go_out=. --go_opt=module=github.com/jguapp/forge \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/jguapp/forge \
	       proto/control.proto

.PHONY: compose-up
compose-up:
	docker compose up --build -d

.PHONY: compose-down
compose-down:
	docker compose down -v

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)
