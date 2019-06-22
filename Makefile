LAST_COMMIT := $(shell git rev-parse --short HEAD)
LAST_COMMIT_DATE := $(shell git show -s --format=%ci ${LAST_COMMIT})
VERSION := $(shell git describe --abbrev=1)

BUILDSTR := ${VERSION} (build "\\\#"${LAST_COMMIT} $(shell date '+%Y-%m-%d %H:%M:%S'))

BIN := sql-jobber

.PHONY: build
build:
	# Compile the main application.
	go build -o ${BIN} -ldflags="-s -w -X 'main.buildString=${BUILDSTR}'"

.PHONY: test
test:
	go test

.PHONY: clean
clean:
	go clean
	- rm -f ${BIN}
