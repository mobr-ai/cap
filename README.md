# CAP - Cardano Analytics Platform
<p align="center">
  <img src="https://github.com/mobr-ai/cap-frontend/raw/refs/heads/main/public/icons/logo.svg" width="180" />
</p>

<p align="center">
  <b>Cardano Analytics Platform Powered by LLM</b><br/>
  Semantic Knowledge Graph • Natural Language → SPARQL • Real-Time On-Chain Data
</p>

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

### Setting Up macOS

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

### Setting Up Linux (Ubuntu)

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

### Setting Up Windows (WSL2) - NOT OFFICIALLY SUPPORTED, TESTED NOR RECOMMENDED.

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

## Runing CAP

### Running locally



#### CAP Setup

1. **Config and environment files:**

   Run script to fetch necessary cardano-node and cardano-db-sync config files

   **ATTENTION**: If you wish to run on mainnet, just replace "preview" with "mainnet" in the fetch script and the names in the step by step.

   ```bash
   ./fetch_config_files.sh
   ```

   Create env file by copying provided example
   ```bash
   cp .env.example .env
   ```

   In the .env file, set `VIRTUOSO_HOST=localhost` and define your password (use the same password in the commands ahead).

2. **Run supporting services:**

   **ATTENTION**: remove --platform linux/amd64 if you are *NOT* using an amd64 platform (e.g. Mac with M1, M2, M3, M4 chips)

   Jaeger for CAP tracing support:
   ```bash
   docker run --platform linux/amd64 -d --name jaeger \
     -p 4317:4317 \
     -p 4318:4318 \
     -p 16686:16686 \
     jaegertracing/all-in-one:latest
   ```

   ```bash
   #OR check if it is running if you had it before
   docker ps --filter "name=jaeger"
   ```

   Virtuoso for CAP triplestore:
   ```bash
   docker run --platform linux/amd64 -d --name virtuoso \
     -p 8890:8890 -p 1111:1111 \
     -e DBA_PASSWORD=mysecretpassword \
     -e SPARQL_UPDATE=true \
     tenforce/virtuoso
   ```

   ```bash
   #OR check if it is running if you had it before
   docker ps --filter "name=virtuoso"
   ```

   PostgreSQL:
   ```bash
   docker run --platform linux/amd64 -d --name postgres \
      -v postgres-data:/var/lib/postgresql/data \
      -e POSTGRES_DB=cap \
      -e POSTGRES_USER=postgres \
      -e POSTGRES_PASSWORD=mysecretpassword \
      -p 5432:5432 \
      postgres:17.5-alpine
   ```

   ```bash
   #OR check if it is running if you had it before
   docker ps --filter "name=postgres"
   ```

   Verify PostgreSQL is running:
   ```bash
   docker ps
   ```

   Clone the cardano-db-sync Repository:
   ```bash
   #remember to cd to your preferred source code folder
   git clone https://github.com/IntersectMBO/cardano-db-sync.git
   cd cardano-db-sync
   ```

   Create a pgpass File:
   ```bash
   echo "localhost:5432:cap:postgres:mysecretpassword" > ~/.pgpass
   chmod 600 ~/.pgpass
   ```

   In the cardano-db-sync folder, run the DB Setup Script:
   ```bash
   docker run --rm \
      --platform linux/amd64 \
      --network host \
      -v $(pwd)/scripts:/scripts \
      -v ~/.pgpass:/root/.pgpass \
      -e PGPASSFILE=/root/.pgpass \
      --entrypoint /bin/bash \
      ghcr.io/intersectmbo/cardano-db-sync:13.6.0.2 \
      -c "/scripts/postgresql-setup.sh --createdb"
   ```

   Cardano Node:
   ```bash
   docker run --platform linux/amd64 -d --name cardano-node-preview \
      -v $HOME/cardano/preview-config:/config \
      -v $(pwd)/backend/assets:/assets \
      -v $HOME/cardano/db:/data \
      -p 3001:3001 \
      ghcr.io/intersectmbo/cardano-node:10.4.0 \
      run \
      --config /config/config.json \
      --topology /config/topology.json \
      --database-path /data \
      --socket-path /data/node.socket \
      --host-addr 0.0.0.0 \
      --port 3001
   ```

   ```bash
   #OR check if it is running if you had it before
   docker ps --filter "name=cardano-node-preview"
   ```

   cardano-db-sync:
   ```bash
   docker run --platform linux/amd64 -d --name cardano-db-sync-preview \
      --network host \
      -v $HOME/cardano/preview-config:/config \
      -v $HOME/cardano/db-sync-data:/var/lib/cexplorer \
      -v $HOME/cardano/db:/node-ipc \
      -e NETWORK=preview \
      -e POSTGRES_HOST=localhost \
      -e POSTGRES_PORT=5432 \
      -e POSTGRES_DB=cap \
      -e POSTGRES_USER=postgres \
      -e POSTGRES_PASSWORD=mysecretpassword \
      -e DB_SYNC_CONFIG=/config/db-sync-config.json \
      ghcr.io/intersectmbo/cardano-db-sync:13.6.0.2
   ```

   ```bash
   #OR check if it is running if you had it before
   docker ps --filter "name=cardano-db-sync-preview"
   ```

   Wait a few seconds and verify if database is syncing:
   ```bash
   docker exec -it postgres psql -U postgres -d cap -c "SELECT * FROM block LIMIT 10;"
   ```

   ollama:
   ```bash
   curl -fsSL https://ollama.com/install.sh | sh
   ```

3. **Set up Python environment:**

   ```bash
   virtualenv venv
   source venv/bin/activate

   #poetry install
   #strongly recommended NVIDIA GeForce RTX 5080 at least to run the model locally
   ollama ollama pull mobr/cap

   ```

4. **Run CAP server:**

   ```bash
   uvicorn src.cap.main:app --host 0.0.0.0 --port 8000
   ```

Now, you can access CAP's API at: [http://localhost:8000/docs](http://localhost:8000/docs)
You can also access CAP's chat UI via `http://localhost:8000/llm`.

#### Testing
With CAP and its dependencies running (i.e., cardano-node, cardano-db-sync, postgresql, virtuos, jaeger, and cap with uvicorn), you can now run its tests
```bash
# activate virtual environment
source venv/bin/activate

# install dev dependencies
poetry install --with dev

# Run all tests
pytest -v

# Run specific test file
pytest -v src/tests/test_api.py

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

6. **Pull mobr/cap LLM model inside Docker:**

   ```bash
   #strongly recommended NVIDIA GeForce RTX 5080 at least to run the model locally
   docker exec ollama ollama pull mobr/cap
   ```

Now, you can access CAP's API at: [http://localhost:8000/docs](http://localhost:8000/docs)
You can also access CAP's chat UI via `http://localhost:8000/llm`.

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

### Using virtuoso conductor to make queries

Queries is alse enabled with Virtuos Conductor. You can access the conductor at:

- **Virtuoso UI:** [http://localhost:8890](http://localhost:8890)
