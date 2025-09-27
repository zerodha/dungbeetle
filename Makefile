LAST_COMMIT := $(shell git rev-parse --short HEAD)
LAST_COMMIT_DATE := $(shell git show -s --format=%ci ${LAST_COMMIT})
VERSION := $(shell git describe --abbrev=1)
BUILDSTR := ${VERSION} (build "\\\#"${LAST_COMMIT} $(shell date '+%Y-%m-%d %H:%M:%S'))

GOPATH ?= $(HOME)/go

BIN := dungbeetle

.PHONY: build
build: $(BIN)

$(BIN): $(shell find . -type f -name "*.go")
	CGO_ENABLED=0 go build -o ${BIN} -ldflags="-s -w -X 'main.buildString=${BUILDSTR}'" ./cmd/*.go

.PHONY: run
run:
	CGO_ENABLED=0 go run -ldflags="-s -w -X 'main.buildString=${BUILDSTR}'" ./cmd

.PHONY: dist
dist: build

# Run tests in sequence
.PHONY: test
test:
	go test ./... -v -p 1

# Local testing targets using unified test script
.PHONY: test-local
test-local:
	./scripts/test.sh all

.PHONY: test-setup
test-setup:
	./scripts/test.sh setup

.PHONY: test-run
test-run:
	./scripts/test.sh run

.PHONY: test-cleanup
test-cleanup:
	./scripts/test.sh cleanup

.PHONY: test-postgres
test-postgres:
	./scripts/test.sh run --postgres

.PHONY: test-mysql
test-mysql:
	./scripts/test.sh run --mysql

# Use goreleaser to do a dry run producing local builds.
.PHONY: release-dry
release-dry:
	goreleaser --parallelism 1 --rm-dist --snapshot --skip-validate --skip-publish

# Use goreleaser to build production releases and publish them.
.PHONY: release
release:
	goreleaser --parallelism 1 --rm-dist --skip-validate
