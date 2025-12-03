"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
import re
import time
import json
import asyncio
from opentelemetry import trace
from typing import Any
from itertools import cycle

from cap.rdf.triplestore import TriplestoreClient
from cap.services.metrics_service import MetricsService
from cap.rdf.triplestore import TriplestoreClient
from cap.rdf.cache.query_normalizer import QueryNormalizer
from cap.util.sparql_util import convert_sparql_to_kv, format_for_llm
from cap.services.ollama_client import get_ollama_client
from cap.services.redis_nl_client import get_redis_nl_client

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

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

async def query_with_stream_response(
        query, context, db=None, user=None):

    # Metrics collection variables
    start_time = time.time()
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

        user_query = query
        if context:
            user_query = f"{context}\n\n{query}"

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
            user_id = None
            if user:
                user_id = user.user_id

            MetricsService.record_query_metrics(
                db=db,
                nl_query=query,
                normalized_query=normalized,
                sparql_query=sparql_query_str,
                kv_results=kv_results,
                is_sequential=is_sequential,
                sparql_valid=sparql_valid,
                query_succeeded=query_succeeded,
                llm_latency_ms=llm_latency_ms if llm_start else 0,
                sparql_latency_ms=sparql_latency_ms if sparql_start else 0,
                total_latency_ms=total_latency_ms,
                user_id=user_id,
                error_message=error_msg
            )
        except Exception as metrics_error:
            logger.error(f"Failed to record metrics: {metrics_error}", exc_info=True)

async def stream_with_timeout_messages(
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

async def execute_sequential_queries(
    triplestore: TriplestoreClient,
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
        results = await triplestore.execute_query(query)

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
