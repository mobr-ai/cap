"""
SPARQL Results to Key-Value Converter for Blockchain Data
Handles large integers (ADA amounts in lovelace) and nested structures
"""
import logging
import copy
from typing import Any
from decimal import Decimal, InvalidOperation
import re

from cap.util.str_util import is_hex_string, hex_to_string

logger = logging.getLogger(__name__)

ADA_CURRENCY_URI = "https://mobr.ai/ont/cardano#cnt/ada"
LOVELACE_TO_ADA = 1_000_000


def _detect_ada_variables(sparql_query: str) -> set[str]:
    """
    Detect which variables in a SPARQL query represent ADA amounts.
    Handles multi-level aggregations (e.g., SUM(SUM(?value))).
    """
    if not sparql_query:
        return set()

    ada_vars = set()

    # Extract the query text
    query_text = sparql_query
    if isinstance(sparql_query, list):
        query_text = " ".join([q.get('query', '') if isinstance(q, dict) else str(q) for q in sparql_query])
    elif isinstance(sparql_query, dict):
        query_text = sparql_query.get('query', str(sparql_query))

    # Step 1: Find base ADA value variables (from hasCurrency)
    lines = query_text.split('\n')
    for i, line in enumerate(lines):
        if ADA_CURRENCY_URI in line:
            context = '\n'.join(lines[max(0, i-3):min(len(lines), i+4)])
            # Checking for the properties that can hold ADA values
            value_vars = re.findall(
                r'(?:hasValue|hasTotalSupply|hasMaxSupply)\s+\?(\w+)',
                context
            )
            ada_vars.update(value_vars)
        else:
            # Checking for the properties that always hold ADA values
            context = '\n'.join(lines[max(0, i-3):min(len(lines), i+4)])
            # Checking for the properties that can hold ADA values
            value_vars = re.findall(
                r'(?:hasFee|hasTxOutputValue)\s+\?(\w+)',
                context
            )
            ada_vars.update(value_vars)

    # Step 2: Propagate through aliases and aggregations (iteratively)
    # This handles cases like: (?fee AS ?avgFee), (SUM(?fee) AS ?total), etc.
    max_iterations = 10  # Prevent infinite loops
    iteration = 0

    while iteration < max_iterations:
        iteration += 1
        previous_count = len(ada_vars)

        # Pattern 1: Simple aliases (?sourceVar AS ?aliasVar)
        alias_matches = re.findall(
            r'\(\s*\?(\w+)\s+AS\s+\?(\w+)\s*\)',
            query_text,
            re.IGNORECASE
        )
        for source_var, alias_var in alias_matches:
            if source_var in ada_vars and alias_var not in ada_vars:
                ada_vars.add(alias_var)
                logger.info(f"Added ADA alias variable: {alias_var} (from {source_var})")

        # Pattern 2: Aggregations with ADA variables
        # Handles: (AGG(?adaVar) AS ?result), (AGG(COALESCE(?adaVar, 0)) AS ?result)
        agg_patterns = [
            # Simple aggregation: SUM(?fee) AS ?total
            r'(?:SUM|AVG|MIN|MAX)\s*\(\s*\?(\w+)\s*\)\s+AS\s+\?(\w+)',
            # With COALESCE: SUM(COALESCE(?fee, 0)) AS ?total
            r'(?:SUM|AVG|MIN|MAX)\s*\(\s*COALESCE\s*\(\s*\?(\w+)\s*,',
            # Division/arithmetic with aggregation: (SUM(?fee) / COUNT(?tx)) AS ?avg
            r'(?:SUM|AVG|MIN|MAX)\s*\(\s*(?:COALESCE\s*\(\s*)?\?(\w+)',
        ]

        for pattern in agg_patterns:
            matches = re.findall(pattern, query_text, re.IGNORECASE)
            for match in matches:
                source_var = match[0] if isinstance(match, tuple) else match
                if source_var in ada_vars:
                    # Find the result variable (AS ?resultVar)
                    # Look for the AS clause after this aggregation
                    context_start = query_text.find(f'?{source_var}')
                    if context_start != -1:
                        context = query_text[context_start:context_start+200]
                        as_match = re.search(r'\)\s+AS\s+\?(\w+)', context, re.IGNORECASE)
                        if as_match:
                            result_var = as_match.group(1)
                            if result_var not in ada_vars:
                                ada_vars.add(result_var)
                                logger.info(f"Added ADA aggregate variable: {result_var} (from {source_var})")

        # Pattern 3: Direct variable binding in aggregations
        # Handles: ((SUM(?fee) / COUNT(?tx)) AS ?avgFee)
        complex_agg_pattern = r'\(\s*\(\s*(?:SUM|AVG|MIN|MAX)\s*\([^)]*\?(\w+)[^)]*\)[^)]*\)\s+AS\s+\?(\w+)\s*\)'
        complex_matches = re.findall(complex_agg_pattern, query_text, re.IGNORECASE)
        for source_var, result_var in complex_matches:
            if source_var in ada_vars and result_var not in ada_vars:
                ada_vars.add(result_var)
                logger.info(f"Added ADA complex aggregate variable: {result_var} (from {source_var})")

        # Stop if no new variables were added
        if len(ada_vars) == previous_count:
            break

    logger.info(f"Final detected ADA variables after propagation: {ada_vars}")
    return ada_vars


def _detect_token_name_variables(sparql_query: str) -> set[str]:
    """
    Detect which variables in a SPARQL query represent token names.
    These should be converted from hex to string if applicable.

    Args:
        sparql_query: SPARQL query string or structure

    Returns:
        Set of variable names that represent token names
    """
    if not sparql_query:
        return set()

    token_name_vars = set()

    # Extract the query text
    query_text = sparql_query
    if isinstance(sparql_query, list):
        query_text = " ".join([q.get('query', '') if isinstance(q, dict) else str(q) for q in sparql_query])
    elif isinstance(sparql_query, dict):
        query_text = sparql_query.get('query', str(sparql_query))

    # Look for hasTokenName property patterns
    # Pattern: ?something b:hasTokenName ?tokenName
    token_name_patterns = [
        r'hasTokenName\s+\?(\w+)',
        r'b:hasTokenName\s+\?(\w+)',
    ]

    for pattern in token_name_patterns:
        matches = re.findall(pattern, query_text, re.IGNORECASE)
        token_name_vars.update(matches)

    # Also propagate through aliases
    alias_matches = re.findall(r'\(\s*\?(\w+)\s+AS\s+\?(\w+)\s*\)', query_text, re.IGNORECASE)
    max_iterations = 5
    iteration = 0

    while iteration < max_iterations:
        iteration += 1
        previous_count = len(token_name_vars)

        for source_var, alias_var in alias_matches:
            if source_var in token_name_vars and alias_var not in token_name_vars:
                token_name_vars.add(alias_var)
                logger.info(f"Added token name alias: {alias_var} (from {source_var})")

        if len(token_name_vars) == previous_count:
            break

    if token_name_vars:
        logger.info(f"Detected token name variables: {token_name_vars}")

    return token_name_vars


def _convert_lovelace_to_ada(lovelace_value: str) -> dict[str, Any]:
    """
    Convert a lovelace amount to ADA and return formatted information
    with proper decimal representation.

    Args:
        lovelace_value: String representation of lovelace amount

    Returns:
        Dictionary with lovelace and ADA representations
    """
    try:
        # Convert to Decimal safely
        lovelace_num = Decimal(lovelace_value)
        ada_value = lovelace_num / LOVELACE_TO_ADA

        result = {
            'lovelace': lovelace_value,
            'ada': str(ada_value),
        }

        return result

    except (ValueError, TypeError, InvalidOperation, Exception) as e:
        logger.warning(f"Could not convert lovelace value '{lovelace_value}': {e}")
        # Also ensure no decimal part is shown in fallback
        clean_value = lovelace_value.split('.')[0] if isinstance(lovelace_value, str) else str(lovelace_value)
        return {
            'lovelace': clean_value
        }


def convert_sparql_to_kv(sparql_results: dict, sparql_query: str = "") -> dict[str, Any]:
    """
    Convert SPARQL results to simplified key-value pairs for LLM consumption.

    Optimized for blockchain data:
    - Preserves large integers (amounts in lovelace)
    - Flattens nested structures
    - Removes SPARQL metadata noise
    - Groups related data logically
    - Detects and converts ADA amounts from lovelace
    - Converts hex token names to readable strings

    Args:
        sparql_results: Raw SPARQL query results from Virtuoso
        sparql_query: Original SPARQL query (used to detect ADA variables and token names)

    Returns:
        Simplified dictionary with key-value pairs
    """
    if not sparql_results:
        return {}

    # Detect which variables represent ADA amounts
    ada_variables = _detect_ada_variables(sparql_query)

    # Detect which variables represent token names (should be hex-decoded)
    token_name_variables = _detect_token_name_variables(sparql_query)

    # Handle ASK queries (boolean results)
    if 'boolean' in sparql_results:
        return {
            'result_type': 'boolean',
            'value': sparql_results['boolean']
        }

    # Handle SELECT/CONSTRUCT queries
    if 'results' not in sparql_results or 'bindings' not in sparql_results['results']:
        logger.warning("Unexpected SPARQL result structure")
        return {'raw_results': sparql_results}

    bindings = copy.deepcopy(sparql_results['results']['bindings'])

    if not bindings:
        return {
            'result_type': 'empty',
            'message': 'No results found'
        }

    # Single row result - convert to flat key-value
    if len(bindings) == 1:
        return {
            'result_type': 'single',
            'count': 1,
            'data': _flatten_binding(bindings[0], ada_variables, token_name_variables)
        }

    # Multiple rows - create structured result
    return {
        'result_type': 'multiple',
        'count': len(bindings),
        'data': [_flatten_binding(binding, ada_variables, token_name_variables) for binding in bindings]
    }


def _flatten_binding(binding: dict[str, Any], ada_variables: set[str] = None,
                     token_name_variables: set[str] = None) -> dict[str, Any]:
    """
    Flatten a single SPARQL binding to simple key-value pairs.

    Handles blockchain-specific data types:
    - Large integers (lovelace amounts)
    - Timestamps
    - Hashes and addresses
    - ADA/lovelace conversions
    - Hex token name conversions

    Args:
        binding: SPARQL binding dictionary
        ada_variables: Set of variable names that represent ADA amounts
        token_name_variables: Set of variable names that represent token names (hex-encoded)
    """
    if ada_variables is None:
        ada_variables = set()
    if token_name_variables is None:
        token_name_variables = set()

    result = {}

    if not binding:
        return result

    for var_name, value_obj in binding.items():
        if not isinstance(value_obj, dict):
            result[var_name] = value_obj
            continue

        value = value_obj.get('value', '')
        datatype = value_obj.get('datatype', '')
        value_type = value_obj.get('type', 'literal')

        # Convert based on datatype
        converted_value = _convert_value(value, datatype, value_type)

        # Handle ADA conversion
        if var_name in ada_variables and isinstance(converted_value, str):
            try:
                # Check if it's a numeric value
                float(converted_value)
                converted_value = _convert_lovelace_to_ada(converted_value)
            except (ValueError, TypeError):
                pass  # Keep original value if not numeric

        # Handle token name hex conversion
        if var_name in token_name_variables and isinstance(converted_value, str):
            if is_hex_string(converted_value):
                decoded_name = hex_to_string(converted_value)
                # Store both hex and decoded versions
                converted_value = {
                    'hex': converted_value,
                    'decoded': decoded_name,
                    'type': 'token_name'
                }
                logger.info(f"Converted token name from hex: {converted_value['hex']} -> {decoded_name}")

        result[var_name] = converted_value

    if not result:
        logger.debug(f"Could not flatten biding: \n   {binding}")

    return result


def _convert_value(value: str, datatype: str, value_type: str) -> Any:
    """
    Convert SPARQL value to appropriate Python type.

    Critical for blockchain data:
    - Use strings for large integers to prevent overflow
    - Preserve precision for amounts
    - Handle various numeric types
    """
    # Handle URIs
    if value_type == 'uri':
        return {'type': 'uri', 'value': value}

    # Handle blank nodes
    if value_type == 'bnode':
        return {'type': 'bnode', 'id': value}

    # Handle typed literals
    if datatype:
        # Integer types - CRITICAL for blockchain amounts
        if ('integer' in datatype.lower() or 'int' in datatype.lower() or
                'decimal' in datatype.lower() or
                'double' in datatype.lower() or
                'float' in datatype.lower() or
                'str' in datatype.lower()):

            return value

        # Boolean
        elif 'boolean' in datatype.lower():
            return value.lower() in ('true', '1', 'yes')

        # DateTime types
        elif 'datetime' in datatype.lower() or 'date' in datatype.lower():
            return {'type': 'datetime', 'value': value}

        # Duration
        elif 'duration' in datatype.lower():
            return {'type': 'duration', 'value': value}

    # Default: return as string
    return value


def format_for_llm(kv_data: dict[str, Any], max_items: int = 10000) -> str:
    """
    Format key-value data into a concise, LLM-friendly string.

    Args:
        kv_data: Key-value data from convert_sparql_to_kv
        max_items: Maximum number of items to include (prevents token overflow)

    Returns:
        Formatted string suitable for LLM context
    """
    result_type = kv_data.get('result_type', 'unknown')

    if result_type == 'boolean':
        return f"Query Result: {kv_data.get('value')}"

    if result_type == 'empty':
        return "No results found for this query."

    if result_type == 'single':
        lines = []
        data = kv_data.get('data', {})
        for key, value in data.items():
            lines.append(f"  {key}: {_format_value(value)}")
        return "\n".join(lines)

    if result_type == 'multiple':
        count = kv_data.get('count', 0)
        data = kv_data.get('data', [])

        # Limit to max_items to prevent token overflow
        display_data = data
        truncated = False
        if (max_items and max_items > 0):
            display_data = data[:max_items]
            truncated = len(data) > max_items

        lines = [f"{count} records:"]

        for idx, item in enumerate(display_data, 1):
            lines.append(f"\{idx}:")
            for key, value in item.items():
                lines.append(f"  {key}: {_format_value(value)}")

        if truncated:
            lines.append(f"\n... and {count - max_items} more results")

        return "\n".join(lines)

    return str(kv_data)


def _format_value(value: Any) -> str:
    """Format a value for display to LLM."""
    if isinstance(value, dict):
        if value.get('type') == 'uri':
            return f"<{value.get('value', '')}>"
        elif value.get('type') == 'datetime':
            return value.get('value', '')
        elif value.get('type') == 'duration':
            return value.get('value', '')
        elif value.get('type') == 'token_name':
            # Format token name with both hex and decoded
            decoded = value.get('decoded', '')
            hex_val = value.get('hex', '')
            if decoded != hex_val:
                return f"{decoded} (hex: {hex_val})"
            return decoded
        elif 'lovelace' in value and 'ada' in value:
            # Format ADA amount
            if 'approximately' in value:
                return f"{value.get('ada', '')} ADA (approximately {value.get('approximately', '')})"

            return f"{value.get('ada', '')} ADA"

        return str(value)

    return str(value)
