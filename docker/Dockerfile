FROM python:3.12.15-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.22 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12.15-slim
# /var/lib/minibroker: where the mini broker (the stretch goal) keeps its log when it runs from
# this image; owned by app, so a fresh Docker volume mounted there is writable.
RUN useradd --create-home app && install -d -o app -g app /var/lib/minibroker
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY --from=build /app/migrations /app/migrations
COPY --from=build /app/alembic.ini /app/alembic.ini
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
# The deploy bundle: this release's production compose file, Caddy, Prometheus and Grafana
# config, deploy script and smoke test. deploy/deploy.sh copies it out of the image, so the
# config on the host always matches the code it ships with, and a rollback restores both.
COPY docker/compose.prod.yaml /app/release/docker/compose.prod.yaml
COPY deploy /app/release/deploy
USER app
# Exec form, so uvicorn is the process that receives SIGTERM. uvloop and httptools in place of
# asyncio's loop and the pure-Python h11 parser: the load test found the API's CPU is what
# saturates first. Named here, not left to uvicorn's "auto", so a missing one fails at start.
CMD ["uvicorn", "hopper.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "uvloop", "--http", "httptools"]
