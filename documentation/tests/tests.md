# CAP - Tests

## Implemented tests
The test modules, housed in the "tests/" directory, constitute a suite designed to validate the prototype's reliability, functionality, and performance across all components, ensuring high-quality deliverables aligned with the project's milestones. Utilizing frameworks like pytest for unit and integration testing, along with asynchronous support via pytest-asyncio, these modules cover a wide spectrum from individual ETL extractors to end-to-end natural language query pipelines. The suite emphasizes automation, with fixtures for database sessions and a triplestore client to simulate real-world environments, and includes assertions for data integrity, error handling, and expected outputs. Coverage is extensive, incorporating mock data for Cardano entities, health checks for services, and scenario-based validations to mimic user interactions. Logging is integrated to capture test results, facilitating debugging and continuous improvement. Overall, these tests confirm the platform's robustness, with pytests covering individual test cases spanning unit, integration, and system levels, and real test cases contributing to the successful public release and open-source repository.

### Pytest

"conftest.py" provides shared pytest fixtures, such as triplestore and async clients, enabling consistent setup across tests. This foundational module supports all others, ensuring efficient test execution and resource management. Collectively, these test modules underpin the project's trust and accountability, validating feasibility through empirical evidence and aligning with the open-source ethos by being publicly available for community scrutiny.

The "test_etl_extractors.py" module focuses on verifying the ETL extraction layer, testing the creation and functionality of extractors for various Cardano entities such as accounts, epochs, blocks, transactions, multi-assets, scripts, datums, stake addresses, stake pools, delegations, rewards, withdrawals, governance actions, DRep registrations, and treasuries. It employs pytest fixtures to create database sessions, asserting that extractors are properly instantiated via the ExtractorFactory, handle batch sizes correctly, retrieve total counts and last IDs accurately, and serialize data (e.g., epoch timestamps) without loss. Asynchronous tests validate batch extraction for large datasets, while error cases check for invalid extractor types, raising appropriate ValueErrors. This module ensures the data ingestion foundation is solid, preventing issues in downstream transformations and loads.

"test_etl_transformers.py" complements the extractors by testing the transformation of extracted relational data into RDF triples. It covers transformer classes mirroring entity types, such as transforming account data into RDF with predicates like "hasStakeAmount" or "delegatesTo". Using mock extracted data, tests assert correct ontology alignment, handling of nested structures (e.g., transaction outputs), and generation of Turtle-formatted RDF. The TransformerFactory is validated for dynamic creation, with edge cases for empty datasets or malformed inputs, ensuring semantic consistency in the knowledge base.

The "test_etl_loader.py" module rigorously evaluates the loading process into the Virtuoso triplestore. It includes tests for initialization, and progress metadata saving using ETLProgress and ETLStatus models. Asynchronous fixtures manage graph cleanup before and after tests to isolate environments. Key validations cover empty data handling (no graph creation), data integrity checks (comparing expected vs. actual triple counts), graph statistics retrieval (subjects, predicates, objects, triples), and clearing non-existent or populated graphs. Error scenarios, like saving progress with ETLStatus.ERROR, are tested to confirm no persistence occurs. This module guarantees reliable data population and metadata management, critical for milestone deliverables like knowledge base bootstrapping.

"test_etl_service.py" assesses the overarching ETL workflow orchestration, testing the ETL service's ability to coordinate extraction, transformation, and loading across entities. It verifies progress tracking, status updates (e.g., RUNNING to COMPLETED), and error recovery, using mock dependencies to simulate full pipelines. Asynchronous tests measure performance under load, ensuring scalability for real-time Cardano data ingestion.

"test_etl_pipeline.py" provides end-to-end pipeline testing, integrating extractors, transformers, and loaders in sequence. It simulates complete ETL runs on sample Cardano data, asserting that data flows correctly from PostgreSQL to RDF to triplestore, with validations on triple counts, ontology compliance, and metadata accuracy. This module includes timeout handling and resource cleanup, confirming the platform's data pipeline feasibility as per the project's capability assessments.

"test_api.py" covers API endpoints, using httpx's AsyncClient for asynchronous requests. It tests health checks, NL query processing (including streaming responses), SPARQL execution, dashboard CRUD operations, user authentication (login, OAuth), metrics retrieval, and admin functions like ETL initiation and cache flushing. Assertions check status codes, response bodies, and error handling, ensuring secure and performant API interactions.

### SPARQL Queries

The "sparql_tests.py" and "sparql_generation_tests.py" modules target SPARQL query handling and generation. "sparql_tests.py" executes predefined SPARQL queries against a test graph populated with sample data, verifying results for scenarios like top ADA outputs, average fees, wallet counts, and governance votes, using assertions on binding counts, ordering, and aggregations. "sparql_generation_tests.py" focuses on LLM-generated SPARQL from natural language, testing accuracy in translating queries like "largest ADA transactions" into valid SPARQL, with validations for syntax, variable detection (e.g., ADA variables), and result formatting utilities.

### Natural Language Queries

"oc_tests.py" addresses specialized validations on how the LLM model answers general questions.
"nl_query_tests.py" is a standalone script for testing the natural language query pipeline as the full integration test for CAP, not using pytest but runnable manually. It includes a test harness for health checks and sample queries (e.g., current epoch, latest blocks, top stake pools), streaming responses via SSE, and summarizing results. This module aids in verifying all the modules of the system, including SPARQL generation and query engine management to contextualize LLM model processing and responses, and user-facing features during development.

## How to run
With CAP and its dependencies running (i.e., after docker compose up -d), you can now run its tests.
On project root folder:

```bash
# activate virtual environment
source venv/bin/activate

# install dev dependencies
poetry install --with dev
```

### Pytest

```bash
# Run all pytests
pytest -v

# Run specific pytest file
pytest -v src/tests/test_api.py

# Run specifit pytest function
pytest -s src/tests/test_etl.py::test_etl_service_status

# Run with coverage report
pytest --cov=src/cap
```

### SPARQL Tests

SPARQL tests will test each SPARQL query passed as input of the tests.

```bash
# run sparql tests with all sparql queries in the examples folder
pythron src/tests/sparql_tests.py

# run sparql tests with only the transaction sparql queries
pythron src/tests/sparql_tests.py --txt-folder documentation/examples/sparql/transactions.txt
```


SPARQL generation will use the LLM to automatically generate one SPARQL query to each corresponting natural language query in the input.

```bash
# run sparql generation tests with all natural language queries in the examples folder
pythron src/tests/sparql_generation_tests.py

# run sparql generation tests with the use cases queries
pythron src/tests/sparql_generation_tests.py --txt-folder documentation/examples/nl/use_cases.txt
```

### Natural Language Tests

Tests on natural language text generation according to specific prompts

```bash
# run tests on LLM model
pythron src/tests/oc_tests.py
```

Integration Tests: verifying all the modules of the system, including SPARQL generation and query engine management to contextualize LLM model processing and responses, and user-facing features during development

```bash
# run natural language query tests with all natural language queries in the examples folder
python nl_query_tests.py

# run natural language query tests with the use cases queries
python nl_query_tests.py --txt-folder documentation/examples/nl/use_cases.txt
```
