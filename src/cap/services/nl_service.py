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
from cap.util.sparql_util import convert_sparql_to_kv, format_for_llm, detect_and_parse_sparql
from cap.services.ollama_client import get_ollama_client
from cap.services.redis_nl_client import get_redis_nl_client

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

async def query_with_stream_response(
    query, context, db=None, user=None, conversation_history=None):

    # Metrics collection variables
    start_time = time.time()
    sparql_query_str = ""
    is_sequential = False
    sparql_valid = False
    kv_results = None
    error_msg = None

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

        normalized = QueryNormalizer.normalize(user_query)
        cached_data = await redis_client.get_cached_query_with_original(normalized, user_query)

        sparql_query = ""
        sparql_queries = None

        # Stage 1: NL to SPARQL
        llm_start = time.time()
        logger.info("Stage 1: convert NL to SPARQL")

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
            yield StatusMessage.generating_sparql()

            try:
                raw_sparql_response = await ollama.nl_to_sparql(natural_query=user_query)
                is_sequential, sparql_content = detect_and_parse_sparql(raw_sparql_response, user_query)

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
        yield StatusMessage.executing_query()
        sparql_start = time.time()
        sparql_dict = await execute_sparql(sparql_query, is_sequential, sparql_queries)
        has_data = sparql_dict["has_data"]
        sparql_results = sparql_dict["sparql_results"]
        error_msg = sparql_dict["error_msg"]

        if has_data:
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
                system_prompt="",
                conversation_history=conversation_history
            )

            async for chunk in stream_with_timeout_messages(context_stream, timeout_seconds=300.0):
                yield chunk

        except Exception as e:
            logger.error(f"Contextualization error: {e}", exc_info=True)
            error_msg = str(e)
            yield StatusMessage.error(f"Error generating answer: {str(e)}")

        yield StatusMessage.data_done()

    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
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
