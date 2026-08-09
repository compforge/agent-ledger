.PHONY: fix lint test build

fix:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy

test:
	uv run pytest

build:
	uv build

