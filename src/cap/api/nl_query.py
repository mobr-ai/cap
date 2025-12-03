"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from opentelemetry import trace
from typing import Optional

from cap.database.session import get_db
from cap.database.model import User
from cap.core.auth_dependencies import get_current_user_unconfirmed
from cap.services.nl_service import query_with_stream_response
from cap.services.ollama_client import get_ollama_client
from cap.services.redis_nl_client import get_redis_nl_client

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

router = APIRouter(prefix="/api/v1/nl", tags=["llm"])


class NLQueryRequest(BaseModel):
    """Natural language query request."""
    query: str = Field(..., description="Natural language query", min_length=1, max_length=1000)
    context: Optional[str] = Field(None, description="Additional context for the query")


@router.get("/queries/top")
async def get_top_queries(limit: int = 5):
    """
    Get top N most frequently asked queries.

    Args:
        limit: Number of top queries to return (default: 5)

    Returns:
        List of queries with their frequencies and normalized versions
    """
    with tracer.start_as_current_span("get_top_queries") as span:
        span.set_attribute("limit", limit)

        try:
            redis_client = get_redis_nl_client()
            popular_queries = await redis_client.get_popular_queries(limit=limit)

            return {
                "top_queries": [
                    {
                        "rank": idx + 1,
                        "query": query["original_query"],
                        "normalized_query": query["normalized_query"],
                        "frequency": query["count"]
                    }
                    for idx, query in enumerate(popular_queries)
                ]
            }
        except Exception as e:
            logger.error(f"Error fetching top queries: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/query")
async def natural_language_query(
    request: NLQueryRequest,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_unconfirmed)
):
    """Process natural language query with metrics collection."""

    with tracer.start_as_current_span("nl_query_pipeline") as span:
        span.set_attribute("query", request.query)

        return StreamingResponse(
            query_with_stream_response(
                request.query, request.context, db, current_user),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no"
            }
        )


@router.get("/health")
async def health_check():
    """Check if the Ollama service is available."""
    try:
        ollama = get_ollama_client()
        is_healthy = await ollama.health_check()

        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "service": "ollama",
            "models": {
                "llm_model": ollama.llm_model
            }
        }

    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {
            "status": "error",
            "service": "ollama",
            "error": str(e)
        }


@router.get("/cache/stats")
async def get_cache_stats():
    """Get cache statistics."""
    try:
        redis_client = get_redis_nl_client()
        popular_queries = await redis_client.get_popular_queries(limit=10)

        return {
            "popular_queries": [
                {
                    "query": query,
                    "count": count
                }
                for query, count in popular_queries
            ]
        }
    except Exception as e:
        logger.error(f"Cache stats error: {e}")
        return {"error": str(e)}