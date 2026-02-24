# syntax=docker/dockerfile:1.6
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

RUN pip install --no-cache-dir poetry==1.8.3

# Copy manifests first (cache-friendly)
COPY pyproject.toml poetry.lock README.md ./

# Ensure lock matches pyproject (without upgrading versions), then install main deps
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    --mount=type=cache,target=/root/.cache/pip \
    poetry config virtualenvs.create false \
 && poetry lock --no-update --no-interaction --no-ansi \
 && poetry install --only main --no-root --no-interaction --no-ansi

# Copy code last
COPY src/ src/

EXPOSE 8000
CMD ["uvicorn", "src.cap.main:app", "--host", "0.0.0.0", "--port", "8000"]