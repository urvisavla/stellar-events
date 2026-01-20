# Stellar Events Makefile

BINARY := stellar-events

.PHONY: build build-linux build-macos clean deps

# Default build (auto-detect OS)
build:
ifeq ($(shell uname),Darwin)
	$(MAKE) build-macos
else
	$(MAKE) build-linux
endif

# macOS (Homebrew)
BREW_PREFIX ?= $(shell brew --prefix 2>/dev/null)
build-macos:
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(BREW_PREFIX)/include" \
	CGO_LDFLAGS="-L$(BREW_PREFIX)/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
	go build -o $(BINARY) ./cmd/

# Linux (adjust ROCKSDB_DIR as needed)
GO := $(HOME)/go1.24/go/bin/go
ROCKSDB_DIR ?= $(HOME)/workspace/rocksdb-dev
build-linux:
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_DIR)/include" \
	CGO_LDFLAGS="-L$(ROCKSDB_DIR) -lrocksdb -lstdc++ -ldl -lm -lz -lbz2 -lsnappy -llz4 -lzstd -pthread" \
	$(GO) build -o $(BINARY) ./cmd/

clean:
	rm -f $(BINARY)

deps:
	go mod download
