.PHONY: lint test

lint:
	cargo clippy -D warnings

test:
	cargo test
