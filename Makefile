# Targets: up, down, test, lint, fmt, migrate. load, chaos and bench arrive in later weeks.
COMPOSE := docker compose -f docker/compose.yaml --env-file .env

.PHONY: env up down logs migrate test lint fmt

env:
	@test -f .env || cp .env.example .env

up: env
	$(COMPOSE) up -d --build postgres redis
	$(COMPOSE) run --rm --build migrate
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

migrate:
	uv run alembic upgrade head

test:
	uv run pytest -q

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy
