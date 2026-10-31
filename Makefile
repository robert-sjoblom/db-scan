.PHONY: lint test

lint:
	cargo clippy --all-targets -- -D warnings

test:
	cargo test
