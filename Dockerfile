# syntax=docker/dockerfile:1.6
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# Install Poetry for group control
RUN pip install --no-cache-dir poetry==1.8.3

# Copy dependency manifests first (cache-friendly)
COPY pyproject.toml poetry.lock README.md ./

# Install only MAIN deps (no dev, no rag), and do NOT install the project itself here
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    --mount=type=cache,target=/root/.cache/pip \
    poetry config virtualenvs.create false \
 && poetry install --only main --no-root --no-interaction --no-ansi

# Copy code last (so code changes don’t invalidate deps)
COPY src/ src/

EXPOSE 8000
CMD ["uvicorn", "src.cap.main:app", "--host", "0.0.0.0", "--port", "8000"]