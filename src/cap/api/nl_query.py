"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
import json
import re
from typing import Optional
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from opentelemetry import trace
from typing import Optional, Any

from cap.services.ollama_client import get_ollama_client
from cap.services.redis_client import get_redis_client
from cap.data.virtuoso import VirtuosoClient
from cap.services.result_processor import process_sparql_results

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

router = APIRouter(prefix="/api/v1/nl", tags=["llm"])


class NLQueryRequest(BaseModel):
    """Natural language query request."""
    query: str = Field(..., description="Natural language query", min_length=1, max_length=1000)
    context: Optional[str] = Field(None, description="Additional context for the query")

import asyncio
from itertools import cycle

class StatusMessage:
    """Helper for creating consistent status messages with rotation support."""

    # Extended status messages for long-running queries
    THINKING_MESSAGES = [
        "status: Analyzing your query deeply\n",
        "status: Exploring the knowledge graph\n",
        "status: Finding relevant connections\n",
        "status: Processing complex relationships\n",
        "status: Gathering comprehensive data\n",
        "status: Cross-referencing information\n",
        "status: Validating query results\n",
        "status: Optimizing data retrieval\n",
    ]

    @staticmethod
    def processing_query() -> str:
        return "status: Processing your query\n"

    @staticmethod
    def generating_sparql() -> str:
        return "status: Analyzing contexts in the knowledge graph\n"

    @staticmethod
    def executing_query() -> str:
        return "status: Fetching contextual data from knowledge graph\n"

    @staticmethod
    def no_results() -> str:
        return "status: No context found, thinking more\n"

    @staticmethod
    def processing_results() -> str:
        return "status: Analyzing context and preparing answer\n"

    @staticmethod
    def get_thinking_message_cycle():
        """Get cycling iterator for thinking messages."""
        return cycle(StatusMessage.THINKING_MESSAGES)

    @staticmethod
    def data_done() -> str:
        return "data: [DONE]\n"

    @staticmethod
    def error(message: str) -> str:
        return f"Error: {message}\n"


async def _stream_with_timeout_messages(
    stream_generator,
    timeout_seconds: float = 5.0
):
    """
    Wrap a stream generator with timeout status messages.

    If no output for timeout_seconds, emit rotating status messages.
    Uses asyncio.wait_for to detect timeouts and inject status messages.
    """
    message_cycle = StatusMessage.get_thinking_message_cycle()

    # Convert generator to async iterator
    stream_iter = stream_generator.__aiter__()
    last_status_time = asyncio.get_event_loop().time()

    while True:
        try:
            # Wait for next chunk with timeout
            chunk = await asyncio.wait_for(
                stream_iter.__anext__(),
                timeout=timeout_seconds
            )
            # Got a chunk, yield it and reset timer
            last_status_time = asyncio.get_event_loop().time()
            yield chunk

        except asyncio.TimeoutError:
            # No output for timeout_seconds, emit a thinking message
            current_time = asyncio.get_event_loop().time()
            if current_time - last_status_time >= timeout_seconds:
                yield next(message_cycle)
                last_status_time = current_time
            # Continue waiting for next chunk
            continue

        except StopAsyncIteration:
            # Stream ended normally
            break
        except Exception as e:
            # Log unexpected errors but don't break the stream
            logger.error(f"Error in stream wrapper: {e}")
            break

def _parse_cached_sequential_sparql(sparql_text: str) -> list[dict[str, Any]]:
    """Parse sequential SPARQL from cache that uses old separator format."""
    queries = []

    # Split by query markers (support both old and new formats)
    parts = re.split(r'---query \d+[^-]*---', sparql_text)

    for part in parts[1:]:  # Skip first empty part
        part = part.strip()
        if not part or part.startswith('---'):
            continue

        # Extract injection parameters
        inject_pattern = r'INJECT\([^)]+\)'
        inject_matches = re.findall(inject_pattern, part)

        queries.append({
            'query': part,
            'inject_params': inject_matches
        })

    return queries

async def _execute_sequential_queries(
    virtuoso: VirtuosoClient,
    queries: list[dict[str, Any]]
) -> dict[str, Any]:
    """Execute sequential SPARQL queries with result injection."""
    previous_results = {}
    final_results = None

    for idx, query_info in enumerate(queries):
        query = query_info['query']
        inject_params = query_info.get('inject_params', [])

        logger.info(f"Executing query {idx + 1}/{len(queries)}")

        # Inject previous results BEFORE execution
        for param_expr in inject_params:
            injected_value = _evaluate_injection(param_expr, previous_results)
            # Replace all INJECT variants with consistent pattern
            query = re.sub(
                r'INJECT(?:_FROM_PREVIOUS)?\([^)]+\)',
                str(injected_value),
                query,
                count=1
            )

        # Execute query
        results = await virtuoso.execute_query(query)

        # **FIX: Extract ALL variables from bindings, not just from single row**
        if results.get('results', {}).get('bindings'):
            bindings = results['results']['bindings']
            logger.info(f"Query {idx + 1} returned {len(bindings)} rows")

            # For aggregates (like COUNT), extract from first row
            if bindings:
                first_row = bindings[0]
                for var, value_obj in first_row.items():
                    # Store both raw value and numeric conversion
                    raw_value = value_obj.get('value')
                    previous_results[var] = raw_value

                    # Try to convert to number for math operations
                    try:
                        numeric_value = float(raw_value)
                        previous_results[var] = numeric_value
                        logger.info(f"Stored {var}={numeric_value}")
                    except (ValueError, TypeError):
                        previous_results[var] = raw_value
                        logger.info(f"Stored {var}={raw_value} (non-numeric)")

        # Handle boolean results
        elif results.get('boolean') is not None:
            previous_results['boolean'] = results['boolean']

        # **FIX: Warn if no results and injection needed for next query**
        else:
            logger.warning(f"Query {idx + 1} returned no results")
            if idx < len(queries) - 1 and queries[idx + 1].get('inject_params'):
                logger.error(f"Query {idx + 2} needs injection but query {idx + 1} returned no data")

        final_results = results

    return final_results

def _evaluate_injection(expression: str, previous_results: dict) -> Any:
    """Evaluate injection expression with previous results."""
    # Extract the actual expression
    expr = expression
    if 'evaluate(' in expr:
        match = re.search(r'evaluate\(([^)]+)\)', expr)
        if match:
            expr = match.group(1)

    # Remove INJECT wrapper if present
    expr = re.sub(r'^INJECT(?:_FROM_PREVIOUS)?\((.+)\)$', r'\1', expr)
    expr = re.sub(r'^evaluate\((.+)\)$', r'\1', expr)

    logger.info(f"Evaluating injection expression: '{expr}'")
    logger.info(f"Available variables: {previous_results}")

    # Replace variable names with their values
    for var, value in previous_results.items():
        if var in expr:
            # Ensure numeric values are properly formatted
            if isinstance(value, (int, float)):
                expr = expr.replace(var, str(value))
                logger.info(f"Replaced {var} with {value}")
            else:
                expr = expr.replace(var, f"'{value}'")

    # Safely evaluate with math operations allowed
    try:
        # Allow basic math operations
        safe_dict = {
            "__builtins__": {},
            "int": int,
            "float": float,
            "round": round,
            "abs": abs,
            "min": min,
            "max": max,
        }
        result = eval(expr, safe_dict, {})
        logger.info(f"Injection evaluated to: {result}")

        # Return as int if it's a whole number
        if isinstance(result, float) and result.is_integer():
            return int(result)
        return result

    except NameError as e:
        logger.error(f"Variable not found in injection: {e}")
        logger.error(f"Expression: {expr}")
        logger.error(f"Available: {list(previous_results.keys())}")
        return 0  # Safe default
    except Exception as e:
        logger.error(f"Injection evaluation error: {e}")
        logger.error(f"Expression: {expr}")
        return 0

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
            redis_client = get_redis_client()
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
async def natural_language_query(request: NLQueryRequest):
    """
    Process a natural language query through the full pipeline:
    1. Check Redis cache for previous results
    2. If not cached: Convert NL to SPARQL
    3. Execute SPARQL against Virtuoso
    4. Cache successful results
    5. Contextualize results with LLM
    6. Stream the final answer

    Returns a Server-Sent Events stream with status updates and final answer.
    """
    with tracer.start_as_current_span("nl_query_pipeline") as span:
        span.set_attribute("query", request.query)
        span.set_attribute("has_context", bool(request.context))

        async def response_stream():
            try:
                # Status: Processing query
                yield f"{StatusMessage.processing_query()}"

                # Get clients
                ollama = get_ollama_client()
                virtuoso = VirtuosoClient()
                redis_client = get_redis_client()

                # Build the user query
                user_query = request.query
                if request.context:
                    user_query = f"{request.context}\n\n{request.query}"

                # Check cache first
                span.set_attribute("stage", "check_cache")
                low_query: str = user_query.lower().strip()
                cached_data = await redis_client.get_cached_query(low_query)

                sparql_query = ""
                sparql_results = None

                # Stage 1: Convert NL to SPARQL
                if cached_data:
                    logger.info(f"Cache hit has cached_data: {cached_data}")
                    cached_sparql = cached_data["sparql_query"]

                    # Detect if it's sequential by checking for the separator
                    if "---split in two queries---" in cached_sparql or "---query 1" in cached_sparql:
                        is_sequential = True
                        # Split the queries manually
                        sparql_queries = _parse_cached_sequential_sparql(cached_sparql)
                    else:
                        is_sequential = False
                        sparql_query = cached_sparql

                else:
                    yield f"{StatusMessage.generating_sparql()}"

                    try:
                        # Generate raw response
                        raw_sparql_response = await ollama.generate_complete(
                            prompt=user_query,
                            model=ollama.llm_model,
                            system_prompt=ollama.nl_to_sparql_prompt,
                            temperature=0.0
                        )
                        logger.info(f"Generated raw SPARQL response: {raw_sparql_response[:200]}...")

                        is_sequential = False
                        sparql_query = ""
                        if "SELECT" in raw_sparql_response:
                            # Detect and parse
                            is_sequential, sparql_content = ollama.detect_and_parse_sparql(raw_sparql_response)

                            if is_sequential:
                                sparql_queries = sparql_content  # list[dict]
                                logger.info(f"Detected sequential SPARQL with {len(sparql_queries)} queries")
                            else:
                                sparql_query = sparql_content  # str
                                logger.info(f"Generated single SPARQL: {sparql_query}")

                    except Exception as e:
                        logger.error(f"SPARQL generation error: {e}", exc_info=True)
                        sparql_query = ""
                        is_sequential = False
                        sparql_queries = []  # Initialize empty list for sequential case

                # Stage 2: Execute SPARQL query
                logger.info(f"Initiating stage 2 for {user_query}")
                if is_sequential:
                    logger.info("stage2: executing sparql list")
                    yield f"{StatusMessage.executing_query()}"
                    try:
                        sparql_results = await _execute_sequential_queries(virtuoso, sparql_queries)
                        sparql_results = process_sparql_results(sparql_results)

                        # Check result count from final results
                        result_count = 0
                        if sparql_results.get('results', {}).get('bindings'):
                            result_count = len(sparql_results['results']['bindings'])
                        elif sparql_results.get('boolean') is not None:
                            result_count = 1

                        span.set_attribute("result_count", result_count)
                        logger.info(f"Sequential SPARQL returned {result_count} final results")

                        if result_count == 0:
                            yield f"{StatusMessage.no_results()}"
                        else:
                            # Cache the entire sequence (serialize queries list)
                            await redis_client.cache_query(
                                nl_query=user_query,
                                sparql_query=json.dumps(sparql_queries)  # Store as JSON
                            )

                    except Exception as e:
                        logger.error(f"Sequential SPARQL execution error: {e}", exc_info=True)
                        is_sequential = False  # Fallback to no results
                        sparql_results = None

                else:  # Single query
                    if sparql_query != "":
                        logger.info("stage2: executing single sparql")
                        yield f"{StatusMessage.executing_query()}"

                        try:
                            sparql_results = await virtuoso.execute_query(sparql_query)
                            sparql_results = process_sparql_results(sparql_results)

                            # Check if we got results
                            result_count = 0
                            if sparql_results.get('results', {}).get('bindings'):
                                result_count = len(sparql_results['results']['bindings'])
                            elif sparql_results.get('boolean') is not None:
                                result_count = 1

                            span.set_attribute("result_count", result_count)
                            logger.info(f"SPARQL query returned {result_count} results")

                            if result_count == 0:
                                yield f"{StatusMessage.no_results()}"
                            else:
                                # Cache successful query
                                await redis_client.cache_query(
                                    nl_query=user_query,
                                    sparql_query=sparql_query
                                )

                        except Exception as e:
                            logger.error(f"SPARQL execution error: {e}", exc_info=True)

                    else:
                        logger.warning("stage2: executing single sparql with an empty sparql")

                # Ensure sparql_query is str for contextualize (use last query if sequential)
                if is_sequential:
                    sparql_query = sparql_queries[-1]['query'] if sparql_queries else ""
                    logger.error("Could not find a list of sparql queries in a sequential query")

                if not sparql_query:
                    sparql_query = ""  # Ensure always defined

                # Stage 3: Contextualize results with LLM
                logger.info(f"Initiating stage 3 with results {sparql_results}")
                yield f"{StatusMessage.processing_results()}"

                try:
                    # Get the context stream from Ollama
                    context_stream = ollama.contextualize_answer(
                        user_query=user_query,
                        sparql_query=sparql_query,
                        sparql_results=sparql_results,
                        system_prompt=""
                    )

                    # Stream with timeout messages
                    async for chunk in _stream_with_timeout_messages(context_stream, timeout_seconds=5.0):
                        yield f"{chunk}\n"

                except Exception as e:
                    logger.error(f"Contextualization error: {e}", exc_info=True)
                    error_msg = StatusMessage.error(f"Error generating answer: {str(e)}")
                    yield f"{error_msg}\n"

                # Completion signal
                logger.info(f"Pipeline was completed")
                yield f"{StatusMessage.data_done()}"

            except Exception as e:
                logger.error(f"Pipeline error: {e}", exc_info=True)
                error_msg = StatusMessage.error(f"Unexpected error: {str(e)}")
                yield f"{error_msg}\n"
                yield f"{StatusMessage.data_done()}"

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
        redis_client = get_redis_client()
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