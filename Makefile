# Targets: up, down, test, lint, fmt, migrate, alerts. load, chaos and bench arrive in later weeks.
COMPOSE := docker compose -f docker/compose.yaml --env-file .env
PROMTOOL := docker run --rm -w /etc/prometheus -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" --entrypoint promtool prom/prometheus:v3.15.0

.PHONY: env up down logs migrate test lint fmt alerts

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

# Check the Prometheus config and unit-test the alert rules (as CI does).
alerts:
	$(PROMTOOL) check config prometheus.yml
	$(PROMTOOL) test rules alerts_test.yml
