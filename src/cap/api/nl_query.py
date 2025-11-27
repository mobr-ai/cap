"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
import json
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from opentelemetry import trace
from typing import Optional
import time

from cap.database.session import get_db
from cap.database.model import User
from cap.services.metrics_service import MetricsService
from cap.core.auth_dependencies import get_current_user_unconfirmed
from cap.util.sparql_util import convert_sparql_to_kv, format_for_llm
from cap.services.ollama_client import get_ollama_client
from cap.services.redis_nl_client import get_redis_nl_client
from cap.services.nl_service import StatusMessage, execute_sequential_queries, stream_with_timeout_messages
from cap.rdf.triplestore import TriplestoreClient
from cap.rdf.cache.query_normalizer import QueryNormalizer

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

        # Timing
        start_time = time.time()
        llm_start = None
        sparql_start = None

        async def response_stream():
            nonlocal llm_start, sparql_start

            # Metrics collection variables
            sparql_query_str = ""
            is_sequential = False
            sparql_valid = False
            query_succeeded = False
            kv_results = None
            error_msg = None

            try:
                yield StatusMessage.processing_query()

                ollama = get_ollama_client()
                triplestore = TriplestoreClient()
                redis_client = get_redis_nl_client()

                user_query = request.query
                if request.context:
                    user_query = f"{request.context}\n\n{request.query}"

                normalized = QueryNormalizer.normalize(user_query)
                cached_data = await redis_client.get_cached_query_with_original(normalized, user_query)

                sparql_query = ""
                sparql_results = None

                # Stage 1: NL to SPARQL
                llm_start = time.time()
                logger.info("Stage 1: convert NL to SPARQL")

                if cached_data:
                    logger.info(f"Cache HIT for {user_query}")
                    cached_sparql = cached_data["sparql_query"]
                    is_sequential = cached_data.get("is_sequential", False)

                    try:
                        if is_sequential:
                            sparql_queries = json.loads(cached_sparql)
                        else:
                            sparql_query = cached_sparql
                        sparql_valid = True
                    except (json.JSONDecodeError, TypeError):
                        is_sequential = False
                        sparql_query = cached_sparql
                        sparql_valid = True
                else:
                    logger.info(f"Cache MISS for {user_query}")
                    yield StatusMessage.generating_sparql()

                    try:
                        raw_sparql_response = await ollama.nl_to_sparql(natural_query=user_query)
                        is_sequential, sparql_content = ollama.detect_and_parse_sparql(raw_sparql_response)

                        if is_sequential:
                            sparql_queries = sparql_content
                        else:
                            sparql_query = sparql_content

                        sparql_valid = bool(sparql_content)
                    except Exception as e:
                        logger.error(f"SPARQL generation error: {e}", exc_info=True)
                        error_msg = str(e)
                        sparql_valid = False

                llm_latency_ms = int((time.time() - llm_start) * 1000)

                # Stage 2: Execute SPARQL
                has_data = True
                sparql_start = time.time()

                if is_sequential and sparql_queries:
                    yield StatusMessage.executing_query()
                    try:
                        sparql_results = await execute_sequential_queries(triplestore, sparql_queries)
                        has_data = bool(sparql_results and sparql_results.get('results', {}).get('bindings'))
                        query_succeeded = True

                        if has_data:
                            await redis_client.cache_query(
                                nl_query=user_query,
                                sparql_query=json.dumps(sparql_queries)
                            )
                    except Exception as e:
                        logger.error(f"Sequential SPARQL execution error: {e}", exc_info=True)
                        error_msg = str(e)
                        has_data = False
                        query_succeeded = False

                elif sparql_query:
                    yield StatusMessage.executing_query()
                    try:
                        sparql_results = await triplestore.execute_query(sparql_query)

                        result_count = 0
                        if sparql_results.get('results', {}).get('bindings'):
                            result_count = len(sparql_results['results']['bindings'])
                        elif sparql_results.get('boolean') is not None:
                            result_count = 1

                        has_data = result_count > 0
                        query_succeeded = True

                        if has_data:
                            await redis_client.cache_query(
                                nl_query=user_query,
                                sparql_query=sparql_query
                            )
                    except Exception as e:
                        logger.error(f"SPARQL execution error: {e}", exc_info=True)
                        error_msg = str(e)
                        has_data = False
                        query_succeeded = False
                else:
                    has_data = False

                sparql_latency_ms = int((time.time() - sparql_start) * 1000)

                # Store SPARQL query string for metrics
                if is_sequential and sparql_queries:
                    sparql_query_str = json.dumps(sparql_queries)
                else:
                    sparql_query_str = sparql_query or ""

                # Stage 3: Contextualize
                if has_data:
                    yield StatusMessage.processing_results()

                try:
                    formatted_results = ""
                    if has_data:
                        kv_results = convert_sparql_to_kv(sparql_results, sparql_query=sparql_query_str)
                        formatted_results = format_for_llm(kv_results, max_items=10000)

                    context_stream = ollama.contextualize_answer(
                        user_query=user_query,
                        sparql_query=sparql_query_str,
                        sparql_results=formatted_results,
                        kv_results=kv_results,
                        system_prompt=""
                    )

                    async for chunk in stream_with_timeout_messages(context_stream, timeout_seconds=300.0):
                        yield f"{chunk}\n"

                except Exception as e:
                    logger.error(f"Contextualization error: {e}", exc_info=True)
                    error_msg = str(e)
                    yield StatusMessage.error(f"Error generating answer: {str(e)}")

                yield StatusMessage.data_done()

            except Exception as e:
                logger.error(f"Pipeline error: {e}", exc_info=True)
                error_msg = str(e)
                query_succeeded = False
                yield StatusMessage.error(f"Unexpected error: {str(e)}")
                yield StatusMessage.data_done()

            finally:
                # Record metrics
                total_latency_ms = int((time.time() - start_time) * 1000)

                try:
                    MetricsService.record_query_metrics(
                        db=db,
                        nl_query=request.query,
                        normalized_query=normalized,
                        sparql_query=sparql_query_str,
                        kv_results=kv_results,
                        is_sequential=is_sequential,
                        sparql_valid=sparql_valid,
                        query_succeeded=query_succeeded,
                        llm_latency_ms=llm_latency_ms if llm_start else 0,
                        sparql_latency_ms=sparql_latency_ms if sparql_start else 0,
                        total_latency_ms=total_latency_ms,
                        user_id=current_user.user_id if current_user else None,
                        error_message=error_msg
                    )
                except Exception as metrics_error:
                    logger.error(f"Failed to record metrics: {metrics_error}", exc_info=True)

        return StreamingResponse(
            response_stream(),
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