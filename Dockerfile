# syntax=docker/dockerfile:1.6

# pinning to patch tag, instead of
# FROM python:3.11-slim AS base
FROM python:3.11.8-slim-bookworm AS base


WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# Basic build deps (keep minimal; add build-essential only if you hit compile errors)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry==1.8.3

# ------------------------------------------------------------
# 1) cap_deps: installs all python deps (main + rag)
# ------------------------------------------------------------
FROM base AS cap_deps

# Copy manifests first for caching
COPY pyproject.toml poetry.lock ./

# IMPORTANT:
# - DO NOT use --only if you want rag too.
# - Default is "main", and --with rag adds rag.
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    --mount=type=cache,target=/root/.cache/pip \
    poetry config virtualenvs.create false \
 && poetry install --with rag --without dev --no-root --no-interaction --no-ansi

# ------------------------------------------------------------
# 2) cap_server: reuses deps layer, only copies code
# ------------------------------------------------------------
FROM cap_deps AS cap_server

COPY src/ src/

EXPOSE 8000

# Where the command actually comes from (if compose doesn't override it)
CMD ["uvicorn", "src.cap.main:app", "--host", "0.0.0.0", "--port", "8000"]