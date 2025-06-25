# Only bitnami spark 3.5.5 uses python 3.12 version higher than 3.9
# builder stage
FROM bitnami/spark:3.5.5 AS builder
COPY --from=ghcr.io/astral-sh/uv:0.7.6 /uv /uvx /bin/

WORKDIR /app

ENV UV_CACHE_DIR=/app/.cache/uv
ENV XDG_DATA_HOME=/app/.local/share

COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock

RUN uv sync

# runtime stage
FROM bitnami/spark:3.5.5 AS runtime

WORKDIR /app

ENV PATH="/app/.venv/bin:${PATH}"
ENV PYTHONPATH="/app"

COPY --from=builder /app/.venv /app/.venv

COPY ./src/jobs jobs
COPY ./src/utils utils