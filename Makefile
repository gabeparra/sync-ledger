.PHONY: setup sync dev worker migrate test test-cov lint format typecheck check docker-up docker-down dlq-list report demo clean

UV ?= uv
PYTHON ?= $(UV) run python

setup:
	$(UV) sync --python 3.12

sync:
	$(UV) sync

dev:
	$(UV) run uvicorn sync_ledger.main:app --reload --host 0.0.0.0 --port 8000

worker:
	$(UV) run sync-ledger worker run

migrate:
	$(UV) run alembic upgrade head

test:
	$(UV) run pytest

test-cov:
	$(UV) run pytest --cov=sync_ledger --cov-report=term-missing

lint:
	$(UV) run ruff check src tests

format:
	$(UV) run ruff format src tests

typecheck:
	$(UV) run mypy src

check: lint typecheck test

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

dlq-list:
	$(UV) run sync-ledger dlq list

report:
	$(UV) run sync-ledger reconcile report

demo:
	$(UV) run sync-ledger demo run

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage dist build
