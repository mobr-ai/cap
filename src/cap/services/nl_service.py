"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
import re
from opentelemetry import trace
from typing import Any

from cap.rdf.triplestore import TriplestoreClient
from cap.rdf.cache.query_normalizer import QueryNormalizer

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

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
