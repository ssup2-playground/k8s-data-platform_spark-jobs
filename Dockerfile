# builder stage
FROM apache/spark:3.5.3 AS builder
COPY --from=ghcr.io/astral-sh/uv:0.7.6 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock

ENV UV_CACHE_DIR=/app/.cache/uv
ENV XDG_DATA_HOME=/app/.local/share

RUN uv sync

COPY ./src/jobs jobs
COPY ./src/utils utils

# runtime stage
FROM apache/spark:3.5.3 AS runtime

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY ./src/jobs jobs
COPY ./src/utils utils

ENV PATH="/app/.venv/bin:${PATH}"
ENV PYTHONPATH="/app"