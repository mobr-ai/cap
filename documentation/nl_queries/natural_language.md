# Querying the system via natural language

## Executive Summary

The Natural Language (NL) Query Engine in the CAP (Cardano Analytics Platform) project enables users to interact with Cardano blockchain data using intuitive, human-readable queries. Powered by a Large Language Model (LLM) via Ollama and integrated with a SPARQL endpoint on Virtuoso, the engine translates NL inputs into executable SPARQL queries, retrieves results from the triplestore, and generates contextualized natural language responses. This document introduces the engine's architecture, key components, and practical usage via API endpoints. The LLM model employed is `mobr/cap`, available at [ollama.com/mobr/cap](https://ollama.com/mobr/cap).

The engine supports streaming responses for real-time interaction and includes caching via Redis for performance optimization. It is exposed through RESTful FastAPI endpoints, accessible via multiple clients like curl, JavaScript, etc. In addition, there is CAP's Swagger UI available for testing the API.

## 1. Introduction

### 1.1 Purpose
The NL Query Engine bridges the gap between non-technical users and complex blockchain data. Users can ask simple questions like "What is a CNT?"; explorer-like questions such as "Show me the latest 5 blocks", or analysis-based question like "What is the average of transactions per block in the last week?", receiving synthesized answers without needing programming expertise. This democratizes access to Cardano's on-chain data, including blocks, transactions, stake pools, governance actions, and more, synced via the ETL pipeline from cardano-db-sync to CAP's knowledge graph.

### 1.2 Scope
This report focuses exclusively on the NL query workflow: NL-to-SPARQL translation, query execution, response contextualization, and API consumption. It excludes ETL syncing, data modeling, and other details.

### 1.3 Key Features
- **NL-to-SPARQL Translation**: LLM-driven conversion of user queries to valid SPARQL.
- **SPARQL Execution**: Queries the knowledge graph for precise data retrieval.
- **Response Synthesis**: LLM refines raw results into coherent, user-friendly answers.
- **Caching**: Redis stores query mappings and results to reduce latency and LLM calls.
- **Streaming**: Server-sent events (SSE) for progressive response delivery.
- **Health Monitoring**: Endpoint for service status checks.

## 2. Architecture Overview

The engine follows LLM's enhancement with KG architecture for queries: (parse NL input) -> (LLM to SPARQL) -> (context results) -> (synthesize response).

### 2.1 High-Level Flow
1. **Input**: User submits NL query via API (e.g., POST `/api/v1/nl/query`).
2. **Caching Check**: Redis lookup for prior executions of the same query.
3. **NL-to-SPARQL**: If uncached, MOBR's Ollama (`mobr/cap` model) generates SPARQL from the NL prompt.
4. **SPARQL Execution**: Query Virtuoso triplestore (graph: `http://www.mobr.ai/ontologies/cardano#`).
5. **Result Processing**: If successful, cache results; else, fallback to error handling.
6. **Contextualization**: `mobr/cap` model synthesizes a natural language answer from SPARQL results.
7. **Output**: Stream response chunks via SSE, including status updates.

### 2.2 Components
- **Ollama Integration**: Asynchronous client (`OllamaClient` in `./services/ollama_client.py`) handles LLM calls. Uses `mobr/cap` for low-temperature (0.0) SPARQL generation and moderate-temperature (0.3) response synthesis. Supports streaming via `/api/v1/nl/query` endpoint.
- **Redis Caching**: `RedisNLClient` (`./services/redis_client.py`) normalizes queries (lowercase, stripped) as keys (e.g., `nlq:cache:<normalized_query>`). Stores SPARQL, results, and hit counts.
- **Virtuoso SPARQL Endpoint**: Queries executed via `VirtuosoClient` (not detailed here, but integrated in the API layer).
- **API Layer**: FastAPI routes in the main app (inferred from endpoints; see `./main.py` or equivalent). Handles CORS, tracing (OpenTelemetry), and error propagation.
- **Frontend Integration**: A full example with a JavaScript client in `llm.html` demonstrates real-time UI with status indicators consuming SPARQL and Natural Language queries (e.g., sync health, processing states, queres, etc.).

## 3. LLM Integration

The engine leverages Ollama for two core tasks, using the `mobr/cap` model optimized for Cardano ontology (prefixes like `cardano:`, `blockchain:`).

### 3.1 Model Details
- **Model**: `mobr/cap` (prompt-engineered with Cardano ontology schema and few-shot examples for SPARQL generation).
- **Parameters**:
  - SPARQL Generation: Temperature=0.0 (deterministic), system prompt includes ontology schema.
  - Response Synthesis: Temperature=0.3 (natural variability).
- **Prompt Engineering**:
  - NL-to-SPARQL: "Convert this natural language query to SPARQL: [query]. Use prefixes: PREFIX cardano: <https://mobr.ai/ont/cardano#>..."
  - Contextualization: "User Question: [query]\nSPARQL: [sparql]\nResults: [json]\nProvide a clear answer:"

### 3.2 Performance Considerations
- Cache Hit: Direct Redis retrieval (sub-10ms).
- Cache Miss: ~2-5s end-to-end (LLM + SPARQL + synthesis).
- Pre-caching: Load common queries from files via `precache_from_file()` for faster cold starts.

## 4. API Endpoints

The engine exposes endpoints under `/api/v1/nl/`. Authentication is optional (add JWT via `Authorization: Bearer <token>` if enabled).

| Endpoint | Method | Description | Request Body | Response |
|----------|--------|-------------|--------------|----------|
| `/api/v1/nl/health` | GET | Service health check. | None | JSON: `{"status": "healthy"}` or error. |
| `/api/v1/nl/query` | POST | Submit NL query; streams response. | JSON: `{"query": "string"}` | SSE stream: `data: status: message\n data: chunk\n data: [DONE]` |
| `/api/v1/query` | POST | Direct SPARQL execution (for debugging). | JSON: `{"query": "sparql_string", "type": "SELECT\|ASK\|..."}` | JSON: SPARQL results (bindings). |

- **Error Responses**: HTTP 4xx/5xx with JSON: `{"error": "message"}`.
- **Streaming Format**: NDJSON lines prefixed with `data: `; ends with `data: [DONE]`.

## 5. Usage Examples

### 5.1 Using curl
Interact with the API via command-line for testing. Replace `http://localhost:8000` with your host/port.

**Health Check**:
```bash
curl "http://localhost:8000/api/v1/nl/health"
```
Response:
```json
{"status": "healthy"}
```

**NL Query (Streaming)**:
```bash
curl -X POST "http://localhost:8000/api/v1/nl/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is the total ADA in circulation?"
  }' \
  -N  # No buffering for streaming
```
- Output: Streams lines like `data: status: Generating SPARQL query...\ndata: The total ADA in circulation is approximately 45 billion.\ndata: [DONE]`
- Pipe to `grep 'data:' | sed 's/data: //'` for clean parsing.

**Direct SPARQL (for Validation)**:
```bash
curl -X POST "http://localhost:8000/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "PREFIX cardano: <https://mobr.ai/ont/cardano#> SELECT (COUNT(*) AS ?total) WHERE { ?tx a cardano:Transaction }",
    "type": "SELECT"
  }'
```

### 5.2 Using JavaScript
The provided `llm.html` demonstrates a chat UI. You can access it via `http://localhost:8000/llm`.

**Key Implementation**:
- **Health/Sync Polling**: Fetches `/api/v1/nl/health` and SPARQL for block heights every 30-60s; updates UI indicators (green pulse for "Online/Synced").
- **Query Submission**: POST to `/api/v1/nl/query` with `{query: value}`; parses SSE stream:
  ```javascript
  const reader = response.body.getReader();
  // ... (decodes chunks, handles 'status: ' prefixes, appends to UI bubbles)
  ```
- **UI Features**:
  - Thinking animation during processing.
  - Example chips (e.g., "Latest blocks") populate the textarea.
  - Auto-resize textarea, char count (max 1000).
  - Error bubbles for failures.

To integrate:
1. Save `llm.html` to your static dir (e.g., `/static/`).
2. Access via `http://localhost:8000/llm`.
3. Customize queries in `<div class="example-chip" data-query="...">`.

**Sample Snippet for Custom JS Client**:
```javascript
async function queryNL(question) {
  const response = await fetch('/api/v1/nl/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: question })
  });
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    console.log(decoder.decode(value));  // Process chunks
  }
}
queryNL("Show me the latest block");
```

### 5.3 Using Swagger UI (/docs)
FastAPI auto-generates interactive docs at `/docs`.

1. Start the server: `uvicorn main:app --host 0.0.0.0 --port 8000`.
2. Navigate to `http://localhost:8000/docs`.
3. **Interact**:
   - **Health**: Click `GET /api/v1/nl/health` > Execute > View JSON.
   - **NL Query**: Click `POST /api/v1/nl/query` > "Try it out" > Enter `{"query": "Total ADA?"}` > Execute. Response streams in the UI (refresh for full view).
   - **SPARQL**: Similar for `/api/v1/query`; schema validation ensures correct JSON.
4. **Advantages**: Schema docs, curl command generation, response examples.

## 6. Conclusion and Recommendations

The NL Query Engine provides a robust, LLM-augmented interface for Cardano data analysis, balancing usability and precision. With `mobr/cap` as the core model, it achieves high-fidelity SPARQL generation while caching ensures scalability.

For further details, refer to source files: `./services/ollama_client.py`, `./services/redis_client.py`, and the main FastAPI app.