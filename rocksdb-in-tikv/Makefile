ENABLE_FEATURES ?= default

ifeq ($(ROCKSDB_SYS_STATIC),1)
endif

ifeq ($(ROCKSDB_SYS_PORTABLE),1)
ENABLE_FEATURES += portable
endif

ifeq ($(ROCKSDB_SYS_SSE),1)
ENABLE_FEATURES += sse
endif

PROJECT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

BIN_PATH = $(CURDIR)/bin
CARGO_TARGET_DIR ?= $(CURDIR)/target

default: all

.PHONY: all

all: build

build:
	RUST_BACKTRACE=1 cargo build --release --features "${ENABLE_FEATURES}"
	@mkdir -p ${BIN_PATH}
	cp -f ${CARGO_TARGET_DIR}/release/rocksdb-in-tikv ${BIN_PATH}/

static_release:
	ROCKSDB_SYS_STATIC=1 ROCKSDB_SYS_PORTABLE=1 ROCKSDB_SYS_SSE=1  make release

static_unportable_release:
	ROCKSDB_SYS_STATIC=1 ROCKSDB_SYS_SSE=1  make release

clean:
	cargo clean
