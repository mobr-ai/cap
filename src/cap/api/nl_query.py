"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
import json
import re
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from opentelemetry import trace
from typing import Optional, Any

from cap.util.sparql_util import convert_sparql_to_kv, format_for_llm
from cap.services.ollama_client import get_ollama_client
from cap.services.redis_nl_client import get_redis_nl_client
from cap.data.virtuoso import VirtuosoClient

from cap.data.cache.query_normalizer import QueryNormalizer

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
        return "status: Analyzing how to consume the knowledge graph\n"

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
    def no_data() -> str:
        return "I do not have this information yet.\n"

    @staticmethod
    def data_done() -> str:
        return "data: [DONE]\n"

    @staticmethod
    def error(message: str) -> str:
        return f"Error: {message}\n"


async def _stream_with_timeout_messages(
    stream_generator,
    timeout_seconds: float = 300.0
):
    """
    Wrap a stream generator with timeout status messages.
    """
    message_cycle = StatusMessage.get_thinking_message_cycle()
    last_status_time = asyncio.get_event_loop().time()

    try:
        # Convert generator to async iterator once
        stream_iter = stream_generator.__aiter__()

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
                logger.info("LLM stream completed successfully")
                break

    except asyncio.CancelledError:
        # Client disconnected - log it but don't raise
        logger.warning("Client cancelled the stream connection")
        raise  # Re-raise to properly cleanup

    except Exception as e:
        # Log unexpected errors
        logger.error(f"Error in stream wrapper: {e}", exc_info=True)
        # Yield error message to client if still connected
        try:
            yield f"error: Stream error: {str(e)}\n"
        except:
            pass

async def _execute_sequential_queries(
    virtuoso: VirtuosoClient,
    queries: list[dict[str, Any]]
) -> dict[str, Any]:
    """Execute sequential SPARQL queries with result injection."""
    previous_results = {}
    final_results = None

    for idx, query_info in enumerate(queries):
        query = query_info['query']

        logger.info(f"Executing sequential query {idx + 1}/{len(queries)}")

        inject_matches = []
        pos = 0
        while True:
            match = re.search(r'INJECT(?:_FROM_PREVIOUS)?\(', query[pos:], re.IGNORECASE)
            if not match:
                break

            start = pos + match.start()
            paren_count = 1
            i = start + len(match.group(0))

            while i < len(query) and paren_count > 0:
                if query[i] == '(':
                    paren_count += 1
                elif query[i] == ')':
                    paren_count -= 1
                i += 1

            if paren_count == 0:
                inject_matches.append(query[start:i])
                pos = i
            else:
                break

        # Process each INJECT statement found
        for param_expr in inject_matches:
            # Extract the expression to evaluate
            expr_match = re.search(r'evaluate\(([^)]+(?:\([^)]*\))*[^)]*)\)', param_expr)
            if expr_match:
                original_expr = expr_match.group(1)
            else:
                original_expr = re.sub(r'^INJECT(?:_FROM_PREVIOUS)?\((.+)\)$', r'\1', param_expr)

            logger.info(f"Extracted expression to evaluate: '{original_expr}'")
            injected_value = _evaluate_injection(param_expr, previous_results)

            # Replace the INJECT statement with the computed value
            if isinstance(injected_value, (int, float)):
                injected_int = int(round(injected_value))
                if injected_int < 1:
                    logger.warning(f"LIMIT value {injected_int} < 1, setting to 1")
                    injected_int = 1
                replacement = str(injected_int)
            else:
                replacement = str(injected_value)

            logger.info(f"Replacing '{param_expr}' with '{replacement}'")
            query = query.replace(param_expr, replacement, 1)

        # Execute as plain SPARQL string
        results = await virtuoso.execute_query(query)

        if results.get('results', {}).get('bindings'):
            bindings = results['results']['bindings']
            logger.info(f"Query {idx + 1} returned {len(bindings)} rows")

            if bindings:
                # Extract ALL variables from first binding
                first_row = bindings[0]
                for var, value_obj in first_row.items():
                    raw_value = value_obj.get('value')

                    # Try numeric conversion
                    try:
                        numeric_value = float(raw_value)
                        # Store as int if whole number
                        if numeric_value.is_integer():
                            previous_results[var] = int(numeric_value)
                        else:
                            previous_results[var] = numeric_value
                        logger.info(f"Stored {var}={previous_results[var]} (numeric)")
                    except (ValueError, TypeError):
                        previous_results[var] = raw_value
                        logger.info(f"Stored {var}={raw_value} (string)")

        elif results.get('boolean') is not None:
            previous_results['boolean'] = results['boolean']
            logger.info(f"Stored boolean={results['boolean']}")
        else:
            logger.warning(f"Query {idx + 1} returned no results")

        final_results = results

    return final_results if final_results else {}

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

    # **ENHANCED: Check for missing variables before evaluation**
    required_vars = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expr)
    missing_vars = [v for v in required_vars if v not in previous_results and v not in ['int', 'float', 'round', 'abs', 'min', 'max']]

    if missing_vars:
        logger.error(f"Missing variables in injection: {missing_vars}")
        logger.error(f"Expression: {expr}")
        logger.error(f"Available: {list(previous_results.keys())}")
        # Return safe default instead of 0
        return 1  # Prevents LIMIT 0 issues

    # Replace variable names with their values
    for var, value in previous_results.items():
        if var in expr:
            if isinstance(value, (int, float)):
                expr = expr.replace(var, str(value))
                logger.info(f"Replaced {var} with {value}")
            else:
                expr = expr.replace(var, f"'{value}'")

    # Safely evaluate with math operations allowed
    try:
        import math
        safe_dict = {
            "__builtins__": {},
            "int": int,
            "float": float,
            "round": round,
            "abs": abs,
            "min": min,
            "max": max,
            "ceil": math.ceil,
            "floor": math.floor,
        }
        result = eval(expr, safe_dict, {})
        logger.info(f"Injection evaluated to: {result}")

        # Always return integer for LIMIT/OFFSET clauses**
        # Round to nearest integer if it's a float
        if isinstance(result, float):
            result = int(round(result))  # e.g., 5440.07 -> 5440

        return result

    except NameError as e:
        logger.error(f"Variable not found in injection: {e}")
        return 1  # Safe default prevents LIMIT 0
    except Exception as e:
        logger.error(f"Injection evaluation error: {e}")
        return 1  # Safe default prevents LIMIT 0

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
                redis_client = get_redis_nl_client()

                # Build the user query
                user_query = request.query
                if request.context:
                    user_query = f"{request.context}\n\n{request.query}"

                # Check cache first
                normalized = QueryNormalizer.normalize(user_query)
                cached_data = await redis_client.get_cached_query_with_original(normalized, user_query)

                sparql_query = ""
                sparql_results = None

                # Stage 1: Convert NL to SPARQL
                logger.info(f"Stage 1: convert NL to sparql")
                if cached_data:
                    logger.info(f"Cache **HIT** for \n   {user_query}\n   normalized query: {normalized}")
                    cached_sparql = cached_data["sparql_query"]

                    # Parse cached SPARQL (unified handling)
                    try:
                        is_sequential = cached_data.get("is_sequential")
                        if is_sequential:
                            parsed = json.loads(cached_sparql)
                            sparql_queries = parsed

                            for idx, query_info in enumerate(sparql_queries):
                                query_text = query_info['query']
                                # Check if placeholders still exist
                                if re.search(r'<<(?:PCT|STR|LIM|CUR|URI)_\d+>>', query_text):
                                    logger.error(f"Query {idx+1} still contains unreplaced placeholders: {query_text}")
                                    # Log the issue but continue - the query will fail and we'll know why
                                else:
                                    logger.info(f"Query {idx+1} placeholders successfully restored")

                            logger.info(f"Cached sequential SPARQL with {len(parsed)} queries")

                        else:
                            sparql_query = cached_sparql

                            # Check single query for placeholders
                            if re.search(r'<<(?:PCT|STR|LIM|CUR|URI)_\d+>>', sparql_query):
                                logger.error(f"Single query still contains unreplaced placeholders: {sparql_query}")

                    except (json.JSONDecodeError, TypeError):
                        is_sequential = False
                        sparql_query = cached_sparql
                else:
                    logger.info(f"Cache **MISS** for \n   {user_query}\n   normalized query: {normalized}")
                    yield f"{StatusMessage.generating_sparql()}"

                    try:
                        raw_sparql_response = await ollama.nl_to_sparql(natural_query=user_query)
                        is_sequential, sparql_content = ollama.detect_and_parse_sparql(raw_sparql_response)
                        if is_sequential:
                            sparql_queries = sparql_content
                            logger.info(f"Generated sequential SPARQL with {len(sparql_queries)} queries")
                        else:
                            sparql_query = sparql_content
                            logger.info(f"Generated single SPARQL")
                        logger.info(f"SPARQL created by LLM:\n {raw_sparql_response}")

                    except Exception as e:
                        logger.error(f"SPARQL generation error: {e}", exc_info=True)
                        is_sequential = False
                        sparql_query = ""
                        sparql_queries = []

                # Stage 2: Execute SPARQL query
                has_data = True
                logger.info(f"Initiating stage 2 for {user_query}")
                if is_sequential:
                    logger.info("stage2: executing sparql list")
                    yield f"{StatusMessage.executing_query()}"
                    try:
                        sparql_results = await _execute_sequential_queries(virtuoso, sparql_queries)
                        if sparql_results:
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
                                has_data = False
                            else:
                                # Cache the entire sequence (serialize queries list)
                                await redis_client.cache_query(
                                    nl_query=user_query,
                                    sparql_query=json.dumps(sparql_queries)  # Store as JSON
                                )
                        else:
                            has_data = False

                    except Exception as e:
                        logger.error(f"Sequential SPARQL execution error: {e}", exc_info=True)
                        is_sequential = False  # Fallback to no results
                        sparql_results = None
                        has_data = False

                else:  # Single query
                    if sparql_query != "":
                        logger.info("stage2: executing single sparql")
                        yield f"{StatusMessage.executing_query()}"

                        try:
                            sparql_results = await virtuoso.execute_query(sparql_query)

                            # Check if we got results
                            result_count = 0
                            if sparql_results.get('results', {}).get('bindings'):
                                result_count = len(sparql_results['results']['bindings'])
                            elif sparql_results.get('boolean') is not None:
                                result_count = 1

                            span.set_attribute("result_count", result_count)
                            logger.info(f"SPARQL query returned {result_count} results")
                            logger.info(f"    SPARQL query: {sparql_query}")

                            if result_count == 0:
                                yield f"{StatusMessage.no_results()}"
                                has_data = False
                            else:
                                # Cache successful query
                                await redis_client.cache_query(
                                    nl_query=user_query,
                                    sparql_query=sparql_query
                                )

                        except Exception as e:
                            logger.error(f"SPARQL execution error: {e}", exc_info=True)
                            has_data = False

                    else:
                        logger.warning("stage2: executing single sparql with an empty sparql")
                        has_data = False

                if is_sequential and sparql_queries:
                    sparql_query = json.dumps(sparql_queries)

                if not sparql_query:
                    sparql_query = ""  # Ensure always defined

                # Stage 3: Contextualize results with LLM
                logger.info(f"Initiating stage 3")
                if has_data:
                    yield f"{StatusMessage.processing_results()}"

                try:
                    kv_results = ""
                    formatted_results = ""
                    if has_data:
                        kv_results = convert_sparql_to_kv(sparql_results, sparql_query=sparql_query)
                        formatted_results = format_for_llm(kv_results, max_items=10000)

                    # Get the context stream from Ollama
                    context_stream = ollama.contextualize_answer(
                        user_query=user_query,
                        sparql_query=sparql_query,
                        sparql_results=formatted_results,
                        kv_results=kv_results,
                        system_prompt=""
                    )

                    # Stream with timeout messages
                    async for chunk in _stream_with_timeout_messages(context_stream, timeout_seconds=300.0):
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