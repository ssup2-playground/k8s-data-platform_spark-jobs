FROM apache/spark:4.0.0-python3
COPY --from=ghcr.io/astral-sh/uv:0.7.6 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock

USER root
RUN uv sync
USER spark

COPY ./src/jobs jobs
COPY ./src/utils utils

ENV PATH="/app/.venv/bin:${PATH}"
ENV PYTHONPATH="/app"
