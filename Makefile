# Targets: up, down, test, lint, fmt, migrate, alerts, rehearse, chaos, chaos-control, demo, load, bench.
COMPOSE := docker compose -f docker/compose.yaml --env-file .env
PROMTOOL := docker run --rm -w /etc/prometheus -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" --entrypoint promtool prom/prometheus:v3.15.0

.PHONY: env up down logs migrate test lint fmt alerts rehearse chaos chaos-control demo load bench

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

fmt:
	uv run ruff format .
	uv run ruff check --fix .

# Check the Prometheus config and unit-test the alert rules (as CI does).
alerts:
	$(PROMTOOL) check config prometheus.yml
	$(PROMTOOL) test rules alerts_test.yml

# Deploys, broken releases and rollbacks on a throwaway Docker host (docker:dind), as CI does.
rehearse:
	bash deploy/rehearse-local.sh

# Chaos test (chaos/kill_workers.py): 10,000 jobs while workers are killed for 5 minutes, then
# SQL checks that none was lost. Report in chaos/results/. chaos-control is the negative
# control (workers ack before running) and must lose jobs.
chaos: up
	python3 chaos/kill_workers.py

chaos-control: up
	python3 chaos/kill_workers.py --mode ack-before-run

# The one-minute demo (docs/demo.md): each step runs when you press Enter.
demo: up
	python3 chaos/demo.py

# Load test (loadtest/bench.py, k6 in Docker). load: scenario A, enqueue throughput.
# bench: scenarios A-D, three runs each, about two hours. Results in loadtest/results/.
load: up
	python3 loadtest/bench.py --scenario A

bench: up
	python3 loadtest/bench.py --scenario all
