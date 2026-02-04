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

from cap.util.status_message import StatusMessage
from cap.services.metrics_service import MetricsService
from cap.services.sparql_service import execute_sparql
from cap.rdf.cache.query_normalizer import QueryNormalizer
from cap.util.sparql_util import detect_and_parse_sparql
from cap.util.sparql_result_processor import convert_sparql_to_kv, format_for_llm
from cap.services.ollama_client import get_ollama_client, OllamaClient
from cap.services.redis_nl_client import get_redis_nl_client, RedisNLClient

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


async def nlq_to_sparql(
        user_query: str,
        redis_client: RedisNLClient,
        ollama: OllamaClient,
        conversation_history: list[dict]
    ):

    normalized = QueryNormalizer.normalize(user_query)
    cached_data = await redis_client.get_cached_query_with_original(normalized, user_query)

    sparql_query = ""
    sparql_queries = None

    if cached_data:
        logger.info(f"Cache HIT for {user_query} -> {normalized}")
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
        logger.info(f"Cache MISS for {user_query} -> {normalized}")

        try:
            raw_sparql_response = await ollama.nl_to_sparql(
                natural_query=user_query,
                conversation_history=conversation_history
            )
            is_sequential, sparql_content = detect_and_parse_sparql(raw_sparql_response, user_query)

            if is_sequential:
                sparql_queries = sparql_content
            else:
                sparql_query = sparql_content

            sparql_valid = bool(sparql_content)
        except Exception as e:
            logger.error(f"SPARQL generation error: {e}")
            sparql_valid = False

    return normalized, sparql_query, sparql_queries, is_sequential, sparql_valid

async def query_with_stream_response(
    query, context, db=None, user=None, conversation_history=None):

    # Metrics collection variables
    start_time = time.time()
    sparql_query_str = ""
    is_sequential = False
    sparql_valid = False
    kv_results = None
    error_msg = None
    normalized = ""
    has_data = False
    llm_start = None
    sparql_start = None

    try:
        yield StatusMessage.processing_query()

        ollama = get_ollama_client()
        redis_client = get_redis_nl_client()

        user_query = query
        if context:
            logger.info("Querying with context.")
            logger.info(f"User query: {user_query}")
            logger.info(f"Context: {context}")
            user_query = f"{context}\n\n{query}"

        # Retry configuration
        max_retries = 2
        retry_count = 0
        was_from_cache = False
        sparql_query = ""
        sparql_queries = None

        # Stage 1 & 2: NL to SPARQL with retry on execution error
        while retry_count <= max_retries:
            try:
                logger.info(f"Stage 1: convert NL to SPARQL (attempt {retry_count + 1}/{max_retries + 1})")

                # Check cache status before generating
                normalized_check = QueryNormalizer.normalize(user_query)
                cached_data = await redis_client.get_cached_query_with_original(normalized_check, user_query)
                was_from_cache = cached_data is not None

                # Generate or retrieve SPARQL
                normalized, sparql_query, sparql_queries, is_sequential, sparql_valid = await nlq_to_sparql(
                    user_query=user_query,
                    redis_client=redis_client,
                    ollama=ollama,
                    conversation_history=conversation_history
                )

                # Stage 2: Execute SPARQL
                yield StatusMessage.executing_query()
                sparql_start = time.time()
                sparql_dict = await execute_sparql(sparql_query, is_sequential, sparql_queries)
                has_data = sparql_dict["has_data"]
                sparql_results = sparql_dict["sparql_results"]
                error_msg = sparql_dict["error_msg"]

                # Success - cache if needed and break
                if has_data and not was_from_cache:
                    if is_sequential and sparql_queries:
                        await redis_client.cache_query(
                            nl_query=user_query,
                            sparql_query=json.dumps(sparql_queries)
                        )
                    elif sparql_query:
                        await redis_client.cache_query(
                            nl_query=user_query,
                            sparql_query=sparql_query
                        )

                # Success, exit retry loop
                break

            except Exception as exec_error:
                error_msg = str(exec_error)
                logger.error(f"SPARQL execution error (attempt {retry_count + 1}/{max_retries + 1}): {error_msg}")

                # If from cache or max retries reached, re-raise
                if was_from_cache or retry_count >= max_retries:
                    logger.error("Cannot retry: query was from cache or max retries reached")
                    sparql_results = {}
                    has_data = False
                    break

                # Retry with error feedback
                retry_count += 1
                logger.warning(f"Retrying with error feedback (attempt {retry_count + 1}/{max_retries + 1})")
                yield StatusMessage.processing_query()

                # Create new conversation history with error feedback (don't mutate original)
                conversation_history = list(conversation_history) if conversation_history else []
                conversation_history.append({
                    "role": "user",
                    "content": f"The SPARQL query you generated failed with this error:\n\n{str(error_msg)}\n\nPlease analyze the error and generate a corrected SPARQL query. Original question: {query}"
                })

        sparql_latency_ms = int((time.time() - sparql_start) * 1000) if sparql_start else 0

        # Store SPARQL query string for metrics
        if is_sequential and sparql_queries:
            sparql_query_str = json.dumps(sparql_queries)
        else:
            sparql_query_str = sparql_query or ""

        # Stage 3: Contextualize
        if has_data:
            yield StatusMessage.processing_results()

        try:
            llm_start = time.time()
            formatted_results = ""
            if has_data:
                kv_results = convert_sparql_to_kv(sparql_results, sparql_query=sparql_query_str)
                formatted_results = format_for_llm(kv_results, max_items=10000)

            context_stream = ollama.generate_answer_with_context(
                user_query=user_query,
                sparql_query=sparql_query_str,
                sparql_results=formatted_results,
                kv_results=kv_results,
                system_prompt="",
                conversation_history=conversation_history
            )
            llm_latency_ms = int((time.time() - llm_start) * 1000)

            async for chunk in stream_with_timeout_messages(context_stream, timeout_seconds=300.0):
                yield chunk

        except Exception as e:
            logger.error(f"Contextualization error: {e}")
            error_msg = str(e)
            yield StatusMessage.error(f"Error generating answer: {str(e)}")

        yield StatusMessage.data_done()

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        error_msg = str(e)
        has_data = False
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
                query_succeeded=has_data,
                llm_latency_ms=llm_latency_ms if llm_start else 0,
                sparql_latency_ms=sparql_latency_ms if sparql_start else 0,
                total_latency_ms=total_latency_ms,
                user_id=user_id,
                error_message=error_msg
            )
        except Exception as metrics_error:
            logger.error(f"Failed to record metrics: {metrics_error}")

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
        logger.error(f"Error in stream wrapper: {e}")
        # Yield error message to client if still connected
        try:
            yield f"error: Stream error: {str(e)}\n"
        except:
            pass
