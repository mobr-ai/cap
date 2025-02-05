from dataclasses import dataclass
from typing import Optional
from SPARQLWrapper import SPARQLWrapper, JSON, GET, POST
import httpx
from opentelemetry import trace
from fastapi import HTTPException

from cap.config import settings

DEFAULT_PREFIX = """
    PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
"""

tracer = trace.get_tracer(__name__)

@dataclass
class VirtuosoConfig:
    """Configuration settings for Virtuoso connection."""
    host: str = settings.VIRTUOSO_HOST
    port: int = settings.VIRTUOSO_PORT
    username: str = settings.VIRTUOSO_USER
    password: str = settings.VIRTUOSO_PASSWORD

    @property
    def base_url(self) -> str:
        """Get the base URL for Virtuoso server."""
        return f"http://{self.host}:{self.port}"

    @property
    def sparql_endpoint(self) -> str:
        """Get the SPARQL endpoint URL."""
        return f"{self.base_url}/sparql"

    @property
    def crud_endpoint(self) -> str:
        """Get the SPARQL Graph CRUD endpoint URL."""
        return f"{self.base_url}/sparql-graph-crud"

class VirtuosoClient:
    def __init__(self, config: VirtuosoConfig | None = None):
        self.config = config or VirtuosoConfig()

        self.sparql:SPARQLWrapper = SPARQLWrapper(self.config.sparql_endpoint)
        self.sparql.setCredentials(self.config.username, self.config.password)
        self.sparql.setReturnFormat(JSON)

    def _build_prefixes(self, additional_prefixes: Optional[dict[str, str]] = None) -> str:
        """Build prefix declarations including any additional prefixes."""
        prefix_str = DEFAULT_PREFIX
        if additional_prefixes:
            for prefix, uri in additional_prefixes.items():
                prefix_str += f"\n    PREFIX {prefix}: <{uri}>"
        return prefix_str

    async def _make_crud_request(
        self, 
        method: str, 
        graph_uri: str, 
        data: Optional[str] = None,
        headers: Optional[dict[str, str]] = None
    ) -> bool:
        """Make a CRUD request to the Virtuoso endpoint."""
        with tracer.start_as_current_span("virtuoso_crud_request") as span:
            span.set_attribute("method", method)
            span.set_attribute("graph_uri", graph_uri)

            default_headers = {
                "Accept": "application/json",
                "Content-Type": "text/turtle"
            }
            if headers:
                default_headers.update(headers)

            # Use SPARQL DELETE for DELETE operations
            if method == "DELETE":
                query = f"CLEAR GRAPH <{graph_uri}>"
                try:
                    self.sparql.setMethod(POST)  # Set method to POST for update operation
                    await self.execute_query(query)
                    self.sparql.setMethod(GET)
                    return True
                except Exception as e:
                    span.set_attribute("error", str(e))
                    raise HTTPException(
                        status_code=400,
                        detail=f"Failed to delete graph: {str(e)}"
                    )

            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=method,
                    url=self.config.crud_endpoint,
                    params={"graph-uri": graph_uri},
                    headers=default_headers,
                    content=data,
                    auth=(self.config.username, self.config.password)
                )

                if response.status_code not in {200, 201, 204}:
                    span.set_attribute("error", response.text)
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"Virtuoso CRUD operation failed: {response.text}"
                    )

                return True

    async def create_graph(self, graph_uri: str, turtle_data: str) -> bool:
        """Create a new graph with the provided Turtle data."""
        with tracer.start_as_current_span("create_graph") as span:
            span.set_attribute("graph_uri", graph_uri)
            
            exists = await self.check_graph_exists(graph_uri)
            if exists:
                raise HTTPException(
                    status_code=409,
                    detail=f"Graph {graph_uri} already exists"
                )
            
            return await self._make_crud_request(
                method="POST",
                graph_uri=graph_uri,
                data=turtle_data,
                headers={"Content-Type": "application/x-turtle"}
            )

    async def read_graph(self, graph_uri: str) -> dict:
        """Read all triples from a graph."""
        with tracer.start_as_current_span("read_graph") as span:
            span.set_attribute("graph_uri", graph_uri)
            
            query = f"""
            CONSTRUCT {{ ?s ?p ?o }}
            WHERE {{
                GRAPH <{graph_uri}> {{
                    ?s ?p ?o
                }}
            }}
            """
            return await self.execute_query(query)

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

            if not insert_data and not delete_data:
                raise ValueError("Either insert_data or delete_data must be provided")

            prefixes = self._build_prefixes(additional_prefixes)
            query_parts = [prefixes]

            if delete_data:
                query_parts.append(f"DELETE DATA {{ GRAPH <{graph_uri}> {{ {delete_data} }} }}")
            if insert_data:
                query_parts.append(f"INSERT DATA {{ GRAPH <{graph_uri}> {{ {insert_data} }} }}")

            query = "\n".join(query_parts)
            self.sparql.setMethod(POST)  # Set POST method only for update operations
            try:
                await self.execute_query(query)
            finally:
                self.sparql.setMethod(GET)

            return True

    async def delete_graph(self, graph_uri: str) -> bool:
        """Delete an entire graph."""
        with tracer.start_as_current_span("delete_graph") as span:
            span.set_attribute("graph_uri", graph_uri)
            
            return await self._make_crud_request(
                method="DELETE",
                graph_uri=graph_uri
            )

    async def check_graph_exists(self, graph_uri: str) -> bool:
        """Check if a graph exists."""
        with tracer.start_as_current_span("check_graph_exists") as span:
            span.set_attribute("graph_uri", graph_uri)
            
            query = f"""
            ASK WHERE {{
                GRAPH <{graph_uri}> {{
                    ?s ?p ?o
                }}
            }}
            """
            self.sparql.setQuery(query)
            results = self.sparql.query().convert()
            return bool(results.get('boolean', False))

    async def execute_query(self, query: str) -> dict:
        """Execute a SPARQL query."""
        with tracer.start_as_current_span("execute_query") as span:
            span.set_attribute("query", query)
            
            self.sparql.setQuery(query)
            return self.sparql.query().convert()