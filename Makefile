SHELL := /bin/sh
.PHONY: all build debug release test clean install help

all: release

help:
	@printf '%s\n' \
	  'targets:' \
	  '  make release   - cargo build --release  (default; produces target/release/libstryke_postgres.{dylib,so})' \
	  '  make debug     - cargo build  (faster compile, slower binary)' \
	  '  make test      - cargo test then `s test t/`  (skips when $$DATABASE_URL unset)' \
	  '  make install   - `s pkg install -g .` (cdylib lands in ~/.stryke/store/postgres@<ver>/)' \
	  '  make clean     - cargo clean'

release:
	cargo build --release

debug build:
	cargo build

test:
	cargo test
	s test t/ || true

install: release
	s pkg install -g .

clean:
	cargo clean
