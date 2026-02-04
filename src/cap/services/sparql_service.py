"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
import re
from opentelemetry import trace
from typing import Any
from fastapi.exceptions import HTTPException

from cap.rdf.triplestore import TriplestoreClient

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

async def execute_sparql(sparql_query: str, is_sequential: bool, sparql_queries: list) -> dict:
    has_data = True
    error_msg = ""
    sparql_results = {}
    triplestore = TriplestoreClient()

    if is_sequential and sparql_queries:
        try:
            sparql_results = await _execute_sequential_queries(triplestore, sparql_queries)
            has_data = bool(sparql_results and sparql_results.get('results', {}).get('bindings'))

        except Exception as e:
            logger.error(f"Sequential SPARQL execution error: {e}")
            error_msg = str(e)
            has_data = False

    elif sparql_query:
        try:
            sparql_results = await triplestore.execute_query(sparql_query)

            result_count = 0
            if sparql_results.get('results', {}).get('bindings'):
                result_count = len(sparql_results['results']['bindings'])
            elif sparql_results.get('boolean') is not None:
                result_count = 1

            has_data = result_count > 0

        except HTTPException as http_err:
            # Convert HTTPException to regular exception to prevent propagation
            logger.error(f"SPARQL execution error (HTTP {http_err.status_code}): {http_err.detail}")
            logger.error(f"    SPARQL: {sparql_query}")
            error_msg = f"{http_err.status_code}: {http_err.detail}"
            has_data = False
        except Exception as e:
            logger.error(f"SPARQL execution error: {e}")
            logger.error(f"    SPARQL: {sparql_query}")
            error_msg = str(e)
            has_data = False
    else:
        has_data = False

    return {
        "has_data": has_data,
        "sparql_results": sparql_results,
        "error_msg": error_msg
    }

async def _execute_sequential_queries(
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
