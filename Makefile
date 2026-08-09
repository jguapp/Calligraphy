# Caligraphy -- distributed job processing platform.
#
# Everything here works without Docker except the compose/bench-compose
# targets; unit tests need nothing running, integration tests need a local
# Postgres and Redis (see test-integration).

GO        ?= go
BIN_DIR   := bin
PKGS      := ./...

# Integration tests are env-gated, not build-tag-gated: a test that finds no
# CALIGRAPHY_TEST_* env skips itself, so `go test ./...` is always safe to run.
TEST_DATABASE_URL ?= postgres://postgres:postgres@127.0.0.1:5432/caligraphy_test?sslmode=disable
TEST_REDIS_ADDR   ?= 127.0.0.1:6379

.PHONY: build
build:
	$(GO) build -o $(BIN_DIR)/ ./cmd/...

.PHONY: test
test:
	$(GO) test -race $(PKGS)

# -p 1 serializes PACKAGES (not tests): the integration suites share one
# real Redis and one real Postgres, and each package flushes them in its
# setup — run in parallel they'd wipe each other's in-flight state. Found
# the honest way: a job "stuck at PENDING" whose stream entry a sibling
# package's FlushDB had eaten.
.PHONY: test-integration
test-integration:
	CALIGRAPHY_TEST_DATABASE_URL="$(TEST_DATABASE_URL)" \
	CALIGRAPHY_TEST_REDIS_ADDR="$(TEST_REDIS_ADDR)" \
	$(GO) test -race -count=1 -p 1 $(PKGS)

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
	protoc --go_out=. --go_opt=module=github.com/jguapp/caligraphy \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/jguapp/caligraphy \
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
