# CAP - Cardano Analytics Platform

## What is CAP?
Leveraging LLMs and analytics mechanisms to provide natural language queries, CAP simplifies Cardano data analysis through real-time insights and intuitive, customizable dashboards.

## Running CAP

## Setting Up Your Environment

### Prerequisites

Before running CAP, ensure you have the following installed on your system:

- **Python 3.11+**
- **Docker & Docker Compose**
- **Virtualenv** (for local setup)
- **Git**

### Installation Steps

#### macOS

1. **Install Homebrew (if not installed):**
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```
2. **Install dependencies:**
   ```bash
   brew install python@3.11 docker docker-compose
   ```
3. **Start Docker (if not already running):**
   ```bash
   open -a Docker
   ```

#### Linux (Ubuntu)

1. **Update system and install dependencies:**
   ```bash
   sudo apt update && sudo apt upgrade -y
   sudo apt install python3.11 python3.11-venv python3-pip docker.io docker-compose -y
   ```
2. **Start Docker service:**
   ```bash
   sudo systemctl start docker
   sudo systemctl enable docker
   ```

#### Windows (WSL2) - NOT OFFICIALLY SUPPORTED, TESTED NOR RECOMMENDED.

> **DISCLAIMER:** While CAP may work on WSL2, it is **not officially supported**. Some features, especially those relying on networking and Docker, may require additional configuration or may not work as expected. Use at your own discretion.  

1. **Enable WSL2 and Install Ubuntu:**
   - Follow Microsoft’s guide: [https://learn.microsoft.com/en-us/windows/wsl/install](https://learn.microsoft.com/en-us/windows/wsl/install)
2. **Install dependencies in WSL:**
   ```bash
   sudo apt update && sudo apt upgrade -y
   sudo apt install python3.11 python3.11-venv python3-pip docker.io docker-compose -y
   ```
3. **Start Docker within WSL:**
   ```bash
   sudo systemctl start docker
   ```

### Runing locally

#### CAP Setup

1. **Copy environment file:**

   ```bash
   cp .env.example .env
   ```

   Set `VIRTUOSO_HOST=localhost` and define your password (use the same password in the `docker run` command below).

2. **Run supporting services:**

   ```bash
   # Run Jaeger for tracing
   docker run -d --name jaeger \
     -p 4317:4317 \
     -p 4318:4318 \
     -p 16686:16686 \
     jaegertracing/all-in-one:latest

   # Run Virtuoso for triplestore
   docker run -d --name virtuoso \
     --platform linux/amd64 \
     -p 8890:8890 -p 1111:1111 \
     -e DBA_PASSWORD=mysecretpassword \
     -e SPARQL_UPDATE=true \
     tenforce/virtuoso
   ```

3. **Set up Python environment:**

   ```bash
   python3.11 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install --no-cache-dir -e ".[dev]"
   ```

4. **Run CAP server:**

   ```bash
   uvicorn src.cap.main:app --host 0.0.0.0 --port 8000
   ```

   Now, you can access CAP's API at: [http://localhost:8000/docs](http://localhost:8000/docs)


#### Testing
With CAP and its dependencies running, you can also run its tests
```bash
# activate virtual environment
source venv/bin/activate

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

1. **Copy the environment file:**

   ```bash
   cp .env.example .env
   ```

   Set `VIRTUOSO_HOST=virtuoso` in the `.env` file.

   **Note:** If you're not using an ARM64 system (e.g., Mac M1/M2/M3), remove `platform: linux/amd64` lines from `docker-compose.yml`.

2. **Build and start services:**

   ```bash
   docker compose up -d
   ```

   Wait a couple of minutes until the services are up. Check them on:
   - **Jaeger UI** → [http://localhost:16686](http://localhost:16686)
   - **Virtuoso** → [http://localhost:8890](http://localhost:8890)
   - **CAP API** → [http://localhost:8000/docs](http://localhost:8000/docs)

3. **View logs:**

   ```bash
   # View all service logs
   docker compose logs -f

   # View specific service logs
   docker compose logs -f api
   ```

4. **Stop services:**

   ```bash
   docker compose down
   ```

5. **Stop services and remove volumes:**

   ```bash
   docker compose down -v
   ```

6. **Run tests inside Docker:**

   ```bash
   docker compose exec api pytest
   ```

## Development

### API Documentation

Once running, access API documentation at:

- **Swagger UI:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc:** [http://localhost:8000/redoc](http://localhost:8000/redoc)

### Cardano Ontology Documentation

- **GitHub:** [https://github.com/mobr-ai/cap/documentation/ontology](https://github.com/mobr-ai/cap/documentation/ontology)
- **Live Website:** [https://mobr.ai/cardano](https://mobr.ai/cardano)

### Monitoring and Tracing

Distributed tracing is enabled with Jaeger. You can monitor traces and debug performance at:

- **Jaeger UI:** [http://localhost:16686](http://localhost:16686)
