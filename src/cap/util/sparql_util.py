"""
SPARQL Results to Key-Value Converter for Blockchain Data
Handles large integers (ADA amounts in lovelace) and nested structures
"""
import logging
from typing import Any, Union
from decimal import Decimal, InvalidOperation
import re
from rdflib.plugins.sparql.parser import parseQuery
from pyparsing import ParseException

logger = logging.getLogger(__name__)

ADA_CURRENCY_URI = "https://mobr.ai/ont/cardano#cnt/ada"
LOVELACE_TO_ADA = 1_000_000


def _clean_sparql(sparql_text: str) -> str:
    """
    Clean and extract SPARQL query from LLM response.

    Args:
        sparql_text: Raw text from LLM

    Returns:
        Cleaned SPARQL query
    """
    # Remove markdown code blocks
    sparql_text = re.sub(r'```sparql\s*', '', sparql_text)
    sparql_text = re.sub(r'```\s*', '', sparql_text)

    # Extract SPARQL query pattern
    # Look for PREFIX or SELECT/ASK/CONSTRUCT/DESCRIBE
    match = re.search(
        r'((?:PREFIX[^\n]+\n)*\s*(?:SELECT|ASK|CONSTRUCT|DESCRIBE).*)',
        sparql_text,
        re.IGNORECASE | re.DOTALL
    )

    if match:
        sparql_text = match.group(1)

    # Remove common explanatory text
    sparql_text = re.sub(r'(?i)here is the sparql query:?\s*', '', sparql_text)
    sparql_text = re.sub(r'(?i)the query is:?\s*', '', sparql_text)
    sparql_text = re.sub(r'(?i)this query will:?\s*.*$', '', sparql_text, flags=re.MULTILINE)

    # Remaining nl before PREFIX
    index = sparql_text.find("PREFIX")
    if index > -1:
        sparql_text = sparql_text[index:]

    # Clean up whitespace
    lines = [line.strip() for line in sparql_text.strip().split('\n') if line.strip()]

    # Filter out lines that are explanatory text and prefixes
    sparql_lines = []
    in_query = False
    for line in lines:
        upper_line = line.upper()
        # Start capturing when we hit query keywords
        if any(keyword in upper_line for keyword in ['SELECT', 'ASK', 'CONSTRUCT', 'DESCRIBE']):
            in_query = True

        # Once we're in the query, keep all lines
        if in_query:
            sparql_lines.append(line)

    cleaned = '\n'.join(sparql_lines).strip()

    logger.debug(f"Cleaned SPARQL query: {cleaned}")
    return cleaned

def _ensure_prefixes(query: str) -> str:
    """
    Ensure the four required PREFIX declarations are present in the SPARQL query.
    Prepends missing ones at the top if not found.
    """
    required_prefixes = {
        "rdf": "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
        "blockchain": "PREFIX b: <https://mobr.ai/ont/blockchain#>",
        "cardano": "PREFIX c: <https://mobr.ai/ont/cardano#>",
        "xsd": "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>",
    }

    stripped = query.strip()
    query_upper = query.upper()

    # Check which prefixes are already present
    missing_prefixes = []
    for prefix_name, prefix_declaration in required_prefixes.items():
        # Look for the prefix declaration pattern (case-insensitive)
        # Check for both "PREFIX rdf:" and "PREFIX rdf :" patterns
        pattern1 = f"PREFIX {prefix_name}:".upper()
        pattern2 = f"PREFIX {prefix_name} :".upper()

        if pattern1 not in query_upper and pattern2 not in query_upper:
            missing_prefixes.append(prefix_declaration)

    if missing_prefixes:
        # Prepend missing prefixes with newline separation
        prepend = "\n".join(missing_prefixes) + "\n\n"
        query = prepend + stripped
        logger.debug(f"Added {len(missing_prefixes)} missing prefixes to SPARQL query")
    else:
        logger.debug("All required prefixes already present in SPARQL query")

    return query

def validate_and_fix_sparql(query: str) -> tuple[bool, str, list[str]]:
    """
    Validate and attempt to fix SPARQL query issues.

    Process:
    1. Try to detect and fix common semantic issues FIRST
    2. Then validate syntax with RDFLib parser
    3. If validation fails, try additional fixes and re-validate

    Args:
        query: SPARQL query string to validate and fix

    Returns:
        Tuple of (is_valid: bool, fixed_query: str, issues: list[str])
    """
    issues = []
    fixed_query = query

    # Step 1: Pre-validation fixes for common GROUP BY issues
    fixed_query = _fix_group_by_aggregation(fixed_query, issues)

    # Step 2: Try syntax validation
    try:
        parseQuery(fixed_query)
        logger.info("SPARQL query validated successfully")
        return True, fixed_query, issues
    except ParseException as e:
        error_msg = f"Syntax error: {str(e)}"
        issues.append(error_msg)
        logger.warning(error_msg)

        # Step 3: Try additional fixes based on the error
        if "expected" in str(e).lower():
            # Try to fix missing dots, braces, etc.
            fixed_query = _fix_structural_issues(fixed_query, issues)

            # Re-validate after structural fixes
            try:
                parseQuery(fixed_query)
                logger.info("Query validated after structural fixes")
                return True, fixed_query, issues
            except Exception:
                pass

        return False, fixed_query, issues

    except Exception as e:
        error_msg = f"Validation error: {str(e)}"
        issues.append(error_msg)
        logger.error(error_msg)
        return False, fixed_query, issues


def _fix_group_by_aggregation(query: str, issues: list[str]) -> str:
    """
    Fix GROUP BY aggregation issues where expressions use variables
    not in the GROUP BY clause.

    Example: Changes "GROUP BY ?month" to "GROUP BY (SUBSTR(STR(?timestamp), 1, 7))"
    when ?month is defined as an expression involving ?timestamp.
    """
    if 'GROUP BY' not in query.upper():
        return query

    fixed_query = query

    # Find SELECT clause
    select_match = re.search(r'SELECT\s+(.*?)\s+WHERE', query, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return query

    select_clause = select_match.group(1)

    # Find GROUP BY clause
    group_by_match = re.search(
        r'GROUP\s+BY\s+(.*?)(?:\s+ORDER|\s+HAVING|\s+LIMIT|\s+OFFSET|\s*\}|\s*$)',
        query,
        re.IGNORECASE | re.DOTALL
    )
    if not group_by_match:
        return query

    group_by_clause = group_by_match.group(1).strip()
    group_by_vars = set(re.findall(r'\?(\w+)', group_by_clause))

    # Find all (expression AS ?var) patterns in SELECT
    expression_patterns = re.findall(
        r'\(([^)]+)\s+AS\s+\?(\w+)\)',
        select_clause,
        re.IGNORECASE
    )

    # Build a map of variable -> expression
    var_to_expression = {}
    for expr, var_name in expression_patterns:
        var_to_expression[var_name] = expr.strip()

    # Check if GROUP BY uses variables that are defined as expressions
    for group_var in group_by_vars:
        if group_var in var_to_expression:
            expression = var_to_expression[group_var]

            # Check if the expression uses variables not in GROUP BY
            expr_vars = set(re.findall(r'\?(\w+)', expression))
            missing_vars = expr_vars - group_by_vars

            if missing_vars:
                # Replace GROUP BY ?var with GROUP BY (expression)
                old_pattern = f'GROUP BY ?{group_var}'
                new_pattern = f'GROUP BY ({expression})'

                if old_pattern in fixed_query:
                    fixed_query = fixed_query.replace(old_pattern, new_pattern)
                    fix_msg = f"Fixed GROUP BY: replaced '?{group_var}' with expression '({expression})'"
                    issues.append(fix_msg)
                    logger.info(fix_msg)

    # Also check for non-aggregated variables in SELECT that aren't in GROUP BY
    all_select_vars = set(re.findall(r'\?(\w+)', select_clause))

    # Find variables that are in aggregate functions
    agg_pattern = r'(?:COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|SAMPLE)\s*\([^)]*\?(\w+)'
    aggregated_vars = set(re.findall(agg_pattern, select_clause, re.IGNORECASE))

    # Variables defined as expressions (AS clause)
    defined_vars = set(var_to_expression.keys())

    # Non-aggregated variables that should be in GROUP BY
    non_agg_vars = all_select_vars - aggregated_vars - defined_vars

    # Missing from GROUP BY
    missing_from_group = non_agg_vars - group_by_vars

    if missing_from_group:
        # Add missing variables to GROUP BY
        for var in missing_from_group:
            group_by_clause += f' ?{var}'

        # Replace the GROUP BY clause
        old_group_by = group_by_match.group(0)
        new_group_by = f'GROUP BY {group_by_clause}'
        fixed_query = fixed_query.replace(old_group_by, new_group_by)

        fix_msg = f"Added missing variables to GROUP BY: {missing_from_group}"
        issues.append(fix_msg)
        logger.info(fix_msg)

    return fixed_query


def _fix_structural_issues(query: str, issues: list[str]) -> str:
    """
    Fix basic structural issues like unbalanced braces or parentheses.
    """
    fixed_query = query

    # Check and fix unbalanced braces
    open_braces = fixed_query.count('{')
    close_braces = fixed_query.count('}')
    if open_braces > close_braces:
        fixed_query += ' }' * (open_braces - close_braces)
        issues.append(f"Added {open_braces - close_braces} missing closing braces")

    # Check and fix unbalanced parentheses
    open_parens = fixed_query.count('(')
    close_parens = fixed_query.count(')')
    if open_parens > close_parens:
        fixed_query += ')' * (open_parens - close_parens)
        issues.append(f"Added {open_parens - close_parens} missing closing parentheses")

    return fixed_query

def _parse_sequential_sparql(sparql_text: str) -> list[dict[str, Any]]:
    """
    Parse sequential SPARQL queries from LLM response with proper INJECT extraction.
    """
    queries = []

    # Split by query sequence markers
    parts = re.split(r'---query sequence \d+:.*?---', sparql_text)

    for part in parts[1:]:  # Skip first empty part
        cleaned = _clean_sparql(part)
        if not cleaned:
            continue
        cleaned = _ensure_prefixes(cleaned)

        # Extract INJECT patterns with nested parentheses
        inject_params = []
        pos = 0
        while True:
            match = re.search(r'INJECT(?:_FROM_PREVIOUS)?\(', cleaned[pos:])
            if not match:
                break

            start = pos + match.start()
            paren_count = 1
            i = start + len(match.group(0))
            while i < len(cleaned) and paren_count > 0:
                if cleaned[i] == '(':
                    paren_count += 1
                elif cleaned[i] == ')':
                    paren_count -= 1
                i += 1

            if paren_count == 0:
                inject_params.append(cleaned[start:i])
                pos = i
            else:
                break

        queries.append({
            'query': cleaned,
            'inject_params': inject_params
        })

    return queries

def detect_and_parse_sparql(sparql_text: str) -> tuple[bool, Union[str, list[dict[str, Any]]]]:
    """
    Detect if the SPARQL text contains sequential queries and parse accordingly.

    Returns:
        Tuple of (is_sequential: bool, content: str or list[dict])
    """
    # Check for sequential markers
    if re.search(r'---query sequence \d+:.*?---', sparql_text, re.IGNORECASE | re.DOTALL):
        queries = _parse_sequential_sparql(sparql_text)
        return len(queries) > 0, queries  # True if parsed successfully
    else:
        cleaned = _clean_sparql(sparql_text)
        cleaned = _ensure_prefixes(cleaned)

        # Validate and fix
        is_valid, fixed_query, issues = validate_and_fix_sparql(cleaned)
        if issues:
            logger.info(f"SPARQL validation results: {'; '.join(issues)}")

        return False, fixed_query

def _is_hex_string(value: str) -> bool:
    """
    Check if a string is a valid hexadecimal string.

    Args:
        value: String to check

    Returns:
        True if the string is valid hex, False otherwise
    """
    if not value or not isinstance(value, str):
        return False

    # Remove common hex prefixes
    clean_value = value.lower().strip()
    if clean_value.startswith('0x'):
        clean_value = clean_value[2:]

    # Check if it's all hex digits and has reasonable length
    if len(clean_value) < 2:
        return False

    return bool(re.match(r'^[0-9a-f]+$', clean_value))


def _hex_to_string(hex_value: str) -> str:
    """
    Convert a hexadecimal string to a readable string.

    Args:
        hex_value: Hexadecimal string (with or without '0x' prefix)

    Returns:
        Decoded string, or original value if conversion fails
    """
    try:
        # Remove 0x prefix if present
        clean_hex = hex_value.lower().strip()
        if clean_hex.startswith('0x'):
            clean_hex = clean_hex[2:]

        # Convert hex to bytes
        byte_data = bytes.fromhex(clean_hex)

        # Try UTF-8 decoding first
        try:
            decoded = byte_data.decode('utf-8')
            # Only return if it contains printable characters
            if decoded.isprintable() or any(c.isalnum() or c.isspace() for c in decoded):
                return decoded.strip()
        except UnicodeDecodeError:
            pass

        # Try ASCII decoding as fallback
        try:
            decoded = byte_data.decode('ascii', errors='ignore')
            if decoded.strip():
                return decoded.strip()
        except:
            pass

        # If all decoding fails, return original
        return hex_value

    except (ValueError, TypeError) as e:
        logger.debug(f"Could not decode hex string '{hex_value}': {e}")
        return hex_value


def _detect_ada_variables(sparql_query: str) -> set[str]:
    """
    Detect which variables in a SPARQL query represent ADA amounts.
    Handles multi-level aggregations (e.g., SUM(SUM(?value))).
    """
    if not sparql_query or ADA_CURRENCY_URI not in sparql_query:
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

    # Step 2: Propagate through aggregations (iteratively until no new vars found)
    # This handles multi-level aggregations like SUM(SUM(?value))
    max_iterations = 10  # Prevent infinite loops
    iteration = 0

    while iteration < max_iterations:
        iteration += 1
        previous_count = len(ada_vars)

        # Find all aggregate patterns: AGG(?source_var) AS ?result_var
        # Handles both simple and nested patterns
        agg_patterns = [
            # Pattern 1: SUM(xsd:decimal(?value)) AS ?balance
            r'(?:SUM|AVG|MIN|MAX|COUNT)\s*\(\s*(?:xsd:\w+\s*\(\s*)?\?(\w+)\s*\)?\s*\)\s+AS\s+\?(\w+)',
            # Pattern 2: (SUM(?balance) AS ?total)
            r'\(\s*(?:SUM|AVG|MIN|MAX|COUNT)\s*\(\s*\?(\w+)\s*\)\s+AS\s+\?(\w+)\s*\)',
        ]

        for pattern in agg_patterns:
            matches = re.findall(pattern, query_text, re.IGNORECASE)
            for source_var, result_var in matches:
                # If source is ADA variable, result is also ADA variable
                if source_var in ada_vars and result_var not in ada_vars:
                    ada_vars.add(result_var)
                    logger.info(f"Added aggregate result variable: {result_var} (from {source_var})")

        # Also handle simple aliases: (?var AS ?alias)
        alias_matches = re.findall(r'\(\s*\?(\w+)\s+AS\s+\?(\w+)\s*\)', query_text, re.IGNORECASE)
        for source_var, alias_var in alias_matches:
            if source_var in ada_vars and alias_var not in ada_vars:
                ada_vars.add(alias_var)
                logger.info(f"Added aliased variable: {alias_var} (from {source_var})")

        # Stop if no new variables were added
        if len(ada_vars) == previous_count:
            break

    logger.info(f"Detected ADA variables: {ada_vars}")
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
    without any decimal part in the string representation.

    Args:
        lovelace_value: String representation of lovelace amount

    Returns:
        Dictionary with lovelace and ADA representations
    """
    try:
        # Convert to Decimal safely
        lovelace_num = Decimal(lovelace_value)
        ada_num = lovelace_num / LOVELACE_TO_ADA

        # Remove decimal information by converting to int first
        lovelace_str = lovelace_value.split('.')[0] if isinstance(lovelace_value, str) else str(lovelace_value)
        ada_int = int(ada_num)
        ada_str = str(ada_int)

        # Human-readable format for large amounts
        str_large = ""
        if ada_int >= 1_000_000_000:
            # Billions
            billions = ada_int / 1_000_000_000
            str_large = f"{billions:.2f} billions ADA"
        elif ada_int >= 1_000_000:
            # Millions
            millions = ada_int / 1_000_000
            str_large = f"{millions:.2f} millions ADA"

        result = {
            'lovelace': lovelace_str,
            'ada': ada_str,
            'unit': 'lovelace'
        }

        if str_large != "":
            result['approximately'] = str_large

        return result

    except (ValueError, TypeError, InvalidOperation, Exception) as e:
        logger.warning(f"Could not convert lovelace value '{lovelace_value}': {e}")
        # Also ensure no decimal part is shown in fallback
        clean_value = lovelace_value.split('.')[0] if isinstance(lovelace_value, str) else str(lovelace_value)
        return {
            'lovelace': clean_value,
            'unit': 'lovelace'
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

    bindings = sparql_results['results']['bindings']

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
            if _is_hex_string(converted_value):
                decoded_name = _hex_to_string(converted_value)
                # Store both hex and decoded versions
                converted_value = {
                    'hex': converted_value,
                    'decoded': decoded_name,
                    'type': 'token_name'
                }
                logger.info(f"Converted token name from hex: {converted_value['hex']} -> {decoded_name}")

        result[var_name] = converted_value

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


# Example usage and tests
if __name__ == "__main__":
    # Test with blockchain data including hex token name
    sample_sparql_result = {
        "results": {
            "bindings": [
                {
                    "tn": {
                        "type": "literal",
                        "value": "48756e74696e67746f6e"  # "Huntington" in hex
                    },
                    "blockNumber": {
                        "type": "literal",
                        "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                        "value": "10123456"
                    },
                    "totalOutput": {
                        "type": "literal",
                        "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                        "value": "1500000000000"  # 1.5 million ADA in lovelace
                    }
                }
            ]
        }
    }

    # Sample SPARQL query with token name
    sample_query = """
    PREFIX b: <https://mobr.ai/ont/cardano#>
    SELECT ?tokenName ?totalOutput
    WHERE {
        ?token b:hasTokenName ?tn .
        ?token b:hasValue ?totalOutput .
        ?token b:hasCurrency <https://mobr.ai/ont/cardano#cnt/ada> .
    }
    """

    # Convert to K/V
    kv_result = convert_sparql_to_kv(sample_sparql_result, sample_query)
    print("K/V Result:")
    print(kv_result)
    print("\n" + "="*50 + "\n")

    # Format for LLM
    llm_format = format_for_llm(kv_result)
    print("LLM Format:")
    print(llm_format)