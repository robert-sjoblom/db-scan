.PHONY: lint test

lint:
	cargo clippy
	cargo clippy --features prometheus -- -D warnings

test:
	cargo test
	cargo test --features prometheus
