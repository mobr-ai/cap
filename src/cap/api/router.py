from fastapi import APIRouter, HTTPException
from opentelemetry import trace
from urllib.parse import unquote_plus
import logging

from cap.api.models import (
    QueryRequest,
    QueryResponse,
    GraphCreateRequest,
    GraphUpdateRequest,
    GraphResponse,
    SuccessResponse
)
from cap.services.redis_sparql_client import get_redis_sparql_client
from cap.data.virtuoso import VirtuosoClient

router = APIRouter(prefix="/api/v1")
tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)

@router.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """Execute a SPARQL query."""
    with tracer.start_as_current_span("execute_query_endpoint") as span:
        span.set_attribute("query_type", request.type)
        client = VirtuosoClient()
        try:
            user_query = request.query
            if user_query:
                low_uq = user_query.lower()
                if "update" in low_uq or "delete" in low_uq or "insert" in low_uq:
                    return QueryResponse(results=[])

            redis_client = get_redis_sparql_client()
            cached_data = await redis_client.get_cached_query(user_query)
            if cached_data:
                return QueryResponse(results=cached_data)

            results = await client.execute_query(user_query)
            redis_client.cache_query(sparql_query=user_query, results=results)
            return QueryResponse(results=results)

        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=400, detail=str(e))

@router.post("/graphs", response_model=SuccessResponse)
async def create_graph(request: GraphCreateRequest):
    """Create a new graph with the provided Turtle data."""
    with tracer.start_as_current_span("create_graph_endpoint") as span:
        span.set_attribute("graph_uri", request.graph_uri)
        client = VirtuosoClient()
        try:
            success = await client.create_graph(request.graph_uri, request.turtle_data)
            return SuccessResponse(success=success)
        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Graph creation error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=400, detail=str(e))

@router.get("/graphs/{graph_uri:path}")
async def read_graph(graph_uri: str):
    """Read all triples from a graph."""
    try:
        graph_uri = unquote_plus(graph_uri)
        logger.debug(f"[READ] Decoded graph_uri: {graph_uri}")

        client = VirtuosoClient()
        exists = await client.check_graph_exists(graph_uri)
        logger.debug(f"[READ] Graph exists check: {exists}")

        if not exists:
            logger.debug(f"[READ] Graph not found: {graph_uri}")
            raise HTTPException(status_code=404, detail=f"Graph {graph_uri} not found")

        data = await client.read_graph(graph_uri)
        return GraphResponse(data=data)
    except HTTPException as e:
        logger.error(f"[READ] HTTP error: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"[READ] Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))

@router.patch("/graphs/{graph_uri:path}")
async def update_graph(graph_uri: str, update_request: GraphUpdateRequest):
    """Update a graph with INSERT and/or DELETE operations."""
    try:
        graph_uri = unquote_plus(graph_uri)
        logger.debug(f"[UPDATE] Decoded graph_uri: {graph_uri}")

        client = VirtuosoClient()
        exists = await client.check_graph_exists(graph_uri)
        logger.debug(f"[UPDATE] Graph exists check: {exists}")

        if not exists:
            logger.debug(f"[UPDATE] Graph not found: {graph_uri}")
            raise HTTPException(status_code=404, detail=f"Graph {graph_uri} not found")

        if not update_request.insert_data and not update_request.delete_data:
            raise HTTPException(
                status_code=400,
                detail="Either insert_data or delete_data must be provided"
            )

        success = await client.update_graph(
            graph_uri,
            insert_data=update_request.insert_data,
            delete_data=update_request.delete_data,
            additional_prefixes=update_request.prefixes
        )
        return SuccessResponse(success=success)
    except HTTPException as e:
        logger.error(f"[UPDATE] HTTP error: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"[UPDATE] Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/graphs/{graph_uri:path}")
async def delete_graph(graph_uri: str):
    """Delete an entire graph."""
    try:
        graph_uri = unquote_plus(graph_uri)
        logger.debug(f"[DELETE] Decoded graph_uri: {graph_uri}")

        client = VirtuosoClient()
        exists = await client.check_graph_exists(graph_uri)
        logger.debug(f"[DELETE] Graph exists check: {exists}")

        if not exists:
            logger.debug(f"[DELETE] Graph not found: {graph_uri}")
            raise HTTPException(status_code=404, detail=f"Graph {graph_uri} not found")

        success = await client.delete_graph(graph_uri)
        return SuccessResponse(success=success)
    except HTTPException as e:
        logger.error(f"[DELETE] HTTP error: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"[DELETE] Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))
