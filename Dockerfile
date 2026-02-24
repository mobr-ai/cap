# syntax=docker/dockerfile:1.6

FROM python:3.11-slim AS base
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Optional: basic OS deps that many wheels rely on (safe baseline)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry==1.8.3

# --------------------------
# Target A: cap_deps
# Builds an image layer with ALL runtime deps installed (main + rag).
# --------------------------
FROM base AS cap_deps

# Copy manifests first (max cache hit)
COPY pyproject.toml poetry.lock README.md ./

# Install deps to system site-packages (no venv)
# NOTE: "root" (your project package) is not installed (--no-root), which is fine
# because you set PYTHONPATH=/app/src and copy the src/ code in cap_server.
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    --mount=type=cache,target=/root/.cache/pip \
    poetry config virtualenvs.create false \
 && poetry install --only main --with rag --no-root --no-interaction --no-ansi \
 # hard fail if the deps you care about are missing
 && python -c "import sentence_transformers; import chromadb; print('deps OK')"

# --------------------------
# Target B: cap_server
# Fast rebuilds: reuses cap_deps layer, only copies code.
# --------------------------
FROM cap_deps AS cap_server

COPY src/ src/

EXPOSE 8000

# Compose currently overrides this CMD, but keep a sane default anyway
CMD ["uvicorn", "src.cap.main:app", "--host", "0.0.0.0", "--port", "8000"]