FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VIRTUALENVS_IN_PROJECT=false \
    POETRY_NO_INTERACTION=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 ingest

COPY pyproject.toml poetry.lock README.md ./
COPY src ./src
COPY contracts ./contracts
COPY .dlt/config.toml ./.dlt/config.toml

RUN pip install --no-cache-dir poetry==1.7.1 \
    && poetry install --only main --no-ansi \
    && pip uninstall -y poetry \
    && mkdir -p /app/.dlt/landing_zone /app/.dlt/pipelines \
    && chown -R ingest:ingest /app

USER ingest

ENTRYPOINT ["energy-ingest"]
CMD ["--all-contracts", "--destination", "databricks"]
