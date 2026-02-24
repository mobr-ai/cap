# syntax=docker/dockerfile:1.6
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

RUN pip install --no-cache-dir poetry==1.8.3

# Copy manifests first for layer caching
COPY pyproject.toml poetry.lock README.md ./

# Install exactly what the lock says
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    --mount=type=cache,target=/root/.cache/pip \
    poetry config virtualenvs.create false \
 && poetry install --only main --with rag --no-root --no-interaction --no-ansi

COPY src/ src/

EXPOSE 8000
CMD ["uvicorn", "src.cap.main:app", "--host", "0.0.0.0", "--port", "8000"]