from dataclasses import dataclass
from typing import Optional
from SPARQLWrapper import SPARQLWrapper, JSON
from opentelemetry import trace
from fastapi import HTTPException

import httpx
import asyncio
import logging
import urllib

from cap.config import settings

DEFAULT_PREFIX = """
    PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
"""

tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)

@dataclass
class VirtuosoConfig:
    """Configuration settings for Virtuoso connection."""
    host: str = settings.VIRTUOSO_HOST
    port: int = settings.VIRTUOSO_PORT
    username: str = settings.VIRTUOSO_USER
    password: str = settings.VIRTUOSO_PASSWORD
    sparql_str_endpoint: str = settings.VIRTUOSO_ENDPOINT
    query_timeout: int = settings.VIRTUOSO_TIMEOUT

    @property
    def base_url(self) -> str:
        """Get the base URL for Virtuoso server."""
        return f"http://{self.host}:{self.port}"

    @property
    def sparql_endpoint(self) -> str:
        """Get the SPARQL endpoint URL."""
        return f"{self.base_url}{self.sparql_str_endpoint}"

    @property
    def crud_endpoint(self) -> str:
        """Get the SPARQL Graph CRUD endpoint URL."""
        return f"{self.base_url}/sparql-graph-crud"

class VirtuosoClient:
    def __init__(self, config: VirtuosoConfig | None = None):
        self.config = config or VirtuosoConfig()
        self._sparql_wrapper = None
        self._http_client = None
        self._initialize_sparql_wrapper()

    async def _get_http_client(self):
        """Get or create reusable HTTP client with optimized settings."""
        if not self._http_client:
            timeout = httpx.Timeout(120.0, connect=10.0)
            self._http_client = httpx.AsyncClient(
                timeout=timeout,
                limits=httpx.Limits(
                    max_keepalive_connections=20,
                    max_connections=50,
                    keepalive_expiry=120.0
                ),
                http2=True  # Enable HTTP/2 for better performance
            )
        return self._http_client

    async def close(self):
        """Close the HTTP client connection."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    def _initialize_sparql_wrapper(self):
        """Initialize the SPARQL wrapper with proper configuration."""
        try:
            self._sparql_wrapper = SPARQLWrapper(self.config.sparql_endpoint)
            self._sparql_wrapper.setCredentials(self.config.username, self.config.password)
            self._sparql_wrapper.setReturnFormat(JSON)
            self._sparql_wrapper.setTimeout(self.config.query_timeout)
        except Exception as e:
            logger.error(f"Failed to initialize SPARQL wrapper: {e}")
            raise RuntimeError(f"SPARQL wrapper initialization failed: {e}")

    def _build_prefixes(self, prefix_statement, default_prefixes, additional_prefixes: Optional[dict[str, str]] = None) -> str:
        """Build prefix declarations including any additional prefixes."""
        prefix_str = default_prefixes
        if additional_prefixes:
            for prefix, uri in additional_prefixes.items():
                prefix_str += f"\n    {prefix_statement} {prefix}: <{uri}>"
        return prefix_str

    def _build_turtle_prefixes(self, additional_prefixes: Optional[dict[str, str]] = None) -> str:
        return self._build_prefixes("@prefix", "", additional_prefixes)

    def _build_sparql_prefixes(self, additional_prefixes: Optional[dict[str, str]] = None) -> str:
        return self._build_prefixes("PREFIX", DEFAULT_PREFIX, additional_prefixes)

    async def _execute_sparql_query_async(self, query: str) -> dict:
        """Execute SPARQL query asynchronously."""

        # If endpoint use plain HTTP GET
        if not self.config.sparql_endpoint.endswith("/sparql"):
            client = await self._get_http_client()
            try:
                # URL-encode the query as curl --data-urlencode does
                encoded_query = urllib.parse.urlencode({"query": query})
                url = f"{self.config.sparql_endpoint}?{encoded_query}"

                response = await client.get(
                    url,
                    headers={"Accept": "application/sparql-results+json"}
                )
                response.raise_for_status()
                ret_ = response.json()
                logger.info(f"query response: {response} \n    response.json(): {ret_}")
                return ret_

            except Exception as e:
                logger.error(f"SPARQL query failed: {e}")
                logger.error(f"Query: {query}")
                raise HTTPException(status_code=500, detail=f"query get failed: {str(e)}")

        def _execute_sync():
            try:
                if not self._sparql_wrapper:
                    self._initialize_sparql_wrapper()

                self._sparql_wrapper.setQuery(query)
                result = self._sparql_wrapper.query()
                return result.convert()
            except Exception as e:
                logger.error(f"SPARQL query execution failed!")
                logger.error(f"     query: {query}")
                logger.error(f"     exception: {e}")
                raise

        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, _execute_sync)
        except Exception as e:
            logger.error(f"Async SPARQL execution error: {e}")
            raise HTTPException(status_code=500, detail=f"SPARQL query failed: {str(e)}")

    async def _make_crud_request(
        self,
        method: str,
        graph_uri: str,
        data: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        additional_prefixes: Optional[dict[str, str]] = None
    ) -> bool:
        """Make a CRUD request to the Virtuoso endpoint."""
        with tracer.start_as_current_span("virtuoso_crud_request") as span:
            span.set_attribute("method", method)
            span.set_attribute("graph_uri", graph_uri)

            default_headers = {
                "Accept": "text/html",
                "Content-Type": "text/turtle"
            }
            if headers:
                default_headers.update(headers)

            # Use SPARQL DELETE for DELETE operations
            if method == "DELETE":
                try:
                    query = f"CLEAR GRAPH <{graph_uri}>"
                    await self._execute_sparql_query_async(query)
                    return True
                except Exception as e:
                    span.set_attribute("error", str(e))
                    logger.error(f"Failed to delete graph {graph_uri}: {e}")
                    raise HTTPException(
                        status_code=400,
                        detail=f"Failed to delete graph: {str(e)}"
                    )

            client = await self._get_http_client()

            try:
                str_prefixes = self._build_turtle_prefixes(additional_prefixes)

                # Prepare content
                content = str_prefixes + data if data else str_prefixes

                # Make request with retry logic
                max_retries = 3
                retry_delay = 0.5

                for attempt in range(max_retries):
                    try:
                        response = await client.request(
                            method=method,
                            url=self.config.crud_endpoint,
                            params={"graph-uri": graph_uri},
                            headers=default_headers,
                            content=content,
                            auth=(self.config.username, self.config.password)
                        )

                        if response.status_code not in {200, 201, 204}:
                            if attempt < max_retries - 1 and response.status_code >= 500:
                                # Retry on server errors
                                await asyncio.sleep(retry_delay * (attempt + 1))
                                continue

                            error_msg = f"Virtuoso CRUD operation failed: HTTP {response.status_code} - {response.text}"
                            span.set_attribute("error", error_msg)
                            logger.error(error_msg)
                            raise HTTPException(
                                status_code=response.status_code,
                                detail=error_msg
                            )

                        logger.debug(f"Successfully executed {method} operation on graph {graph_uri}")
                        return True

                    except httpx.TimeoutException:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay * (attempt + 1))
                            continue

                        error_msg = f"Timeout during {method} operation on graph {graph_uri} after {max_retries} attempts"
                        span.set_attribute("error", error_msg)
                        logger.error(error_msg)
                        raise HTTPException(status_code=504, detail=error_msg)

                    except httpx.RequestError as e:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay * (attempt + 1))
                            continue

                        error_msg = f"Request error during {method} operation: {str(e)}"
                        span.set_attribute("error", error_msg)
                        logger.error(error_msg)
                        raise HTTPException(status_code=503, detail=error_msg)

            except HTTPException:
                raise
            except Exception as e:
                error_msg = f"Unexpected error during {method} operation: {str(e)}"
                span.set_attribute("error", error_msg)
                logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)

    async def create_graph(
            self,
            graph_uri: str,
            turtle_data: str,
            additional_prefixes: Optional[dict[str, str]] = None
    ) -> bool:
        """Create a new graph with the provided Turtle data."""
        with tracer.start_as_current_span("create_graph") as span:
            span.set_attribute("graph_uri", graph_uri)
            span.set_attribute("data_size", len(turtle_data))

            try:
                exists = await self.check_graph_exists(graph_uri)
                if exists:
                    logger.warning(f"Graph {graph_uri} already exists, skipping creation")
                    return True  # Consider this a success case

                return await self._make_crud_request(
                    method="POST",
                    graph_uri=graph_uri,
                    data=turtle_data,
                    headers={"Content-Type": "application/x-turtle"},
                    additional_prefixes=additional_prefixes
                )
            except HTTPException:
                logger.error(f"HTTP Exception creating graph {graph_uri}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error creating graph {graph_uri}: {e}")
                raise

    async def read_graph(self, graph_uri: str) -> dict:
        """Read all triples from a graph."""
        with tracer.start_as_current_span("read_graph") as span:
            span.set_attribute("graph_uri", graph_uri)

            try:
                query = f"""
                CONSTRUCT {{ ?s ?p ?o }}
                WHERE {{
                    GRAPH <{graph_uri}> {{
                        ?s ?p ?o
                    }}
                }}
                """
                return await self._execute_sparql_query_async(query)
            except HTTPException:
                logger.error(f"HTTP Exception reading graph {graph_uri}")
                raise
            except Exception as e:
                logger.error(f"Error reading graph {graph_uri}: {e}")
                raise

    async def update_graph(
        self,
        graph_uri: str,
        insert_data: Optional[str] = None,
        delete_data: Optional[str] = None,
        additional_prefixes: Optional[dict[str, str]] = None
    ) -> bool:
        """Update a graph with INSERT and DELETE operations."""
        with tracer.start_as_current_span("update_graph") as span:
            span.set_attribute("graph_uri", graph_uri)
            span.set_attribute("has_insert_data", bool(insert_data))
            span.set_attribute("has_delete_data", bool(delete_data))

            if not insert_data and not delete_data:
                raise ValueError("Either insert_data or delete_data must be provided")

            try:
                self._sparql_wrapper.setMethod('POST')
                prefixes = self._build_sparql_prefixes(additional_prefixes)

                # Handle DELETE operation
                if delete_data:
                    # Check if delete_data contains variables
                    if '?' in delete_data:
                        # Use DELETE WHERE for patterns with variables
                        delete_query = f"""
                        {prefixes}
                        DELETE WHERE {{
                            GRAPH <{graph_uri}> {{
                                {delete_data}
                            }}
                        }}
                        """
                    else:
                        # Use DELETE DATA for specific triples
                        delete_query = f"""
                        {prefixes}
                        DELETE DATA {{
                            GRAPH <{graph_uri}> {{
                                {delete_data}
                            }}
                        }}
                        """

                    await self._execute_sparql_query_async(delete_query)

                # Handle INSERT operation
                if insert_data:
                    insert_query = f"""
                    {prefixes}
                    INSERT DATA {{
                        GRAPH <{graph_uri}> {{
                            {insert_data}
                        }}
                    }}
                    """

                    await self._execute_sparql_query_async(insert_query)

                return True

            except HTTPException:
                logger.error(f"HTTP Exception updating graph {graph_uri}")
                raise
            except Exception as e:
                logger.error(f"Error updating graph {graph_uri}: {e}")
                raise

    async def delete_graph(self, graph_uri: str) -> bool:
        """Delete an entire graph."""
        with tracer.start_as_current_span("delete_graph") as span:
            span.set_attribute("graph_uri", graph_uri)

            try:
                return await self._make_crud_request(
                    method="DELETE",
                    graph_uri=graph_uri
                )
            except HTTPException:
                logger.error(f"HTTP Exception deleting graph {graph_uri}")
                raise
            except Exception as e:
                logger.error(f"Error deleting graph {graph_uri}: {e}")
                raise

    async def check_graph_exists(self, graph_uri: str) -> bool:
        """Check if a graph exists."""
        with tracer.start_as_current_span("check_graph_exists") as span:
            span.set_attribute("graph_uri", graph_uri)

            try:
                query = f"""
                ASK WHERE {{
                    GRAPH <{graph_uri}> {{
                        ?s ?p ?o
                    }}
                }}
                """
                result = await self._execute_sparql_query_async(query)
                exists = bool(result.get('boolean', False))
                span.set_attribute("graph_exists", exists)
                return exists
            except HTTPException:
                # If we get an HTTP error, assume the graph doesn't exist
                logger.warning(f"Error checking if graph exists {graph_uri}, assuming it doesn't exist")
                return False
            except Exception as e:
                logger.error(f"Error checking graph existence {graph_uri}: {e}")
                # Don't raise HTTPException here, just return False
                return False

    async def execute_query(self, query: str) -> dict:
        """Execute a SPARQL query."""
        with tracer.start_as_current_span("execute_query") as span:
            span.set_attribute("query_type", "SELECT" if "SELECT" in query.upper() else "OTHER")

            try:
                return await self._execute_sparql_query_async(query)
            except HTTPException:
                logger.error(f"Error HTTPException")
                raise
            except Exception as e:
                logger.error(f"Error executing SPARQL query: {e}")
                raise

    async def get_graph_count(self, graph_uri: str) -> int:
        """Get the number of triples in a graph."""
        with tracer.start_as_current_span("get_graph_count") as span:
            span.set_attribute("graph_uri", graph_uri)

            try:
                query = f"""
                SELECT (COUNT(*) AS ?count)
                WHERE {{
                    GRAPH <{graph_uri}> {{
                        ?s ?p ?o
                    }}
                }}
                """
                result = await self._execute_sparql_query_async(query)

                if result.get('results', {}).get('bindings'):
                    count = int(result['results']['bindings'][0]['count']['value'])
                    span.set_attribute("triple_count", count)
                    return count
                return 0
            except Exception as e:
                logger.error(f"Error getting graph count for {graph_uri}: {e}")
                return 0

    async def test_connection(self) -> bool:
        """Test the connection to Virtuoso."""
        with tracer.start_as_current_span("test_connection") as span:
            try:
                query = "SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 1"
                await self._execute_sparql_query_async(query)
                span.set_attribute("connection_success", True)
                logger.info("Virtuoso connection test successful")
                return True
            except Exception as e:
                span.set_attribute("connection_success", False)
                span.set_attribute("error", str(e))
                logger.error(f"Virtuoso connection test failed: {e}")
                return False
