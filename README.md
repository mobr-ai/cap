# CAP - Cardano Analytics Platform

## What is CAP?
Leveraging LLMs and analytics mechanisms to provide natural language queries, CAP simplifies Cardano data analysis through real-time insights and intuitive, customizable dashboards.

## How to run CAP

### Runing locally

#### CAP
Copy .env.example to a new .env file and set VIRTUOSO_HOST=localhost. Set the desired password and use the same password in the docker run command line below (virtuoso).

```bash
# run a jager docker image to support tracing on CAP
docker run -d --name jaeger \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 16686:16686 \
  jaegertracing/all-in-one:latest

# run a virtuoso docker image to support triplestore on CAP
docker run -d --name virtuoso \
  --platform linux/amd64 \
  -p 8890:8890 -p 1111:1111 \
  -e DBA_PASSWORD=mysecretpassword \
  -e SPARQL_UPDATE=true \
  tenforce/virtuoso

# install dependencies
pip install --no-cache-dir -e ".[dev]" 

# run CAP server
uvicorn src.cap.main:app --host 0.0.0.0 --port 8000
```
Now you can access CAP's API on your browser: 
http://localhost:8000/docs

#### Testing
With CAP and its dependencies running, you can also run its tests
```bash
# Run all tests
pytest

# Run specific test file
pytest src/tests/test_api.py

# Run specifit test function
pytest -s src/tests/test_integration.py::test_full_graph_lifecycle

# Run with coverage report
pytest --cov=src/cap
```

### Running CAP with Docker Compose

1. First, copy the environment file:
```bash
cp .env.example .env
```
Set VIRTUOSO_HOST=virtuoso in the .env file

If you are not using an ARM64 system (e.g. M1/M2/M3,... Mac), remove the platform on docker-compose.yml
lines with:      platform: linux/amd64

2. Build and start all services:
```bash
docker compose up -d
```

This will start:
- CAP API on http://localhost:8000/docs
- Virtuoso on http://localhost:8890
- Jaeger UI on http://localhost:16686

To view the logs:
```bash
# View all services logs
docker compose logs -f

# View specific service logs
docker compose logs -f api
```

To stop all services:
```bash
docker compose down
```

To stop all services and remove volumes:
```bash
docker compose down -v
```

To run the cap tests:
```bash
docker compose exec api pytest
```

## Development

### Prerequisites
- Python 3.11+
- Docker and Docker Compose

### API Documentation
Once the service is running, you can access the API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Cardano Ontology Documentation
You can access the Cardano Ontology documentation at:
- GitHub: https://github.com/mobr-ai/cap/documentation/ontology
- MOBR Website: https://mobr.ai/cardano

### Monitoring and Tracing
The application includes distributed tracing with Jaeger. You can access the Jaeger UI at http://localhost:16686 to monitor traces and debug performance issues.
