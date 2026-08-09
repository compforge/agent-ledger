.PHONY: fix lint test build

fix:
	uv run --no-sync ruff format .
	uv run --no-sync ruff check --fix .

lint:
	uv run --no-sync ruff format --check .
	uv run --no-sync ruff check .
	MYPYPATH=src uv run --no-sync mypy

test:
	PYTHONPATH=src uv run --no-sync pytest

build:
	uv build
