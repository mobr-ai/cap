"""
SPARQL Results to Key-Value Converter for Blockchain Data
Handles large integers (ADA amounts in lovelace) and nested structures
"""
import logging
from typing import Any, Union
import re
from rdflib.plugins.sparql.parser import parseQuery
from pyparsing import ParseException

logger = logging.getLogger(__name__)

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
        "rdfs": "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
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

def _validate_and_fix_sparql(query: str, nl_query: str) -> tuple[bool, str, list[str]]:
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
        return True, fixed_query, issues
    except ParseException as e:
        error_msg = f"Syntax error: {str(e)}"
        issues.append(error_msg)
        logger.warning(f"Error for query {nl_query}: {error_msg}")

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
    Fix or add GROUP BY clause for SPARQL queries with aggregations.

    This function ensures SPARQL queries with aggregate functions have correct GROUP BY:
    1. Adds GROUP BY if missing but needed (query has aggregates)
    2. Fixes GROUP BY that uses expression variables instead of base variables
    3. Adds missing non-aggregated variables to existing GROUP BY

    Args:
        query: SPARQL query string to fix
        issues: List to append fix descriptions to

    Returns:
        Fixed SPARQL query string with proper GROUP BY clause

    Examples:
        Missing GROUP BY:
            SELECT ?addr (COUNT(?tx) AS ?count) WHERE {...}
            -> SELECT ?addr (COUNT(?tx) AS ?count) WHERE {...} GROUP BY ?addr

        Wrong expression in GROUP BY:
            SELECT (SUBSTR(?date, 1, 7) AS ?month) (COUNT(?tx) AS ?count)
            WHERE {...} GROUP BY ?month
            -> ... GROUP BY (SUBSTR(?date, 1, 7))
    """
    # Parse query structure
    select_match = re.search(
        r'SELECT\s+(.*?)\s+WHERE',
        query,
        re.IGNORECASE | re.DOTALL
    )
    if not select_match:
        return query

    select_clause = select_match.group(1).strip()

    # Extract query components
    var_definitions = _extract_variable_definitions(select_clause)
    aggregate_result_vars = _extract_aggregate_result_variables(select_clause)
    aggregated_vars = _extract_aggregated_variables(select_clause)
    all_select_vars = set(re.findall(r'\?(\w+)', select_clause))

    # Determine if query has aggregations
    has_aggregates = bool(aggregate_result_vars)

    if not has_aggregates:
        # No aggregates, no GROUP BY needed
        return query

    # Calculate non-aggregated variables that need to be in GROUP BY
    non_aggregated_vars = (
        all_select_vars
        - aggregate_result_vars      # Exclude COUNT(...) AS ?var results
        - aggregated_vars            # Exclude ?var inside COUNT(?var)
        - set(var_definitions.keys())  # Exclude (expr AS ?var) results
    )

    # Check if GROUP BY exists
    group_by_match = re.search(
        r'GROUP\s+BY\s+(.*?)(?:\s+ORDER|\s+HAVING|\s+LIMIT|\s+OFFSET|\s*\}|\s*$)',
        query,
        re.IGNORECASE | re.DOTALL
    )

    if not group_by_match:
        # No GROUP BY clause exists, add it if needed
        if non_aggregated_vars:
            fixed_query = _add_group_by_clause(
                query,
                non_aggregated_vars,
                var_definitions,
                issues
            )
            return fixed_query
        else:
            # Aggregates only, no GROUP BY needed
            return query

    # GROUP BY exists, fix it
    group_by_clause = group_by_match.group(1).strip()
    group_by_full_match = group_by_match.group(0)
    group_by_vars = _extract_grouping_variables(group_by_clause)

    fixed_query = query

    # Fix 1: Replace expression variables with actual expressions
    for group_var in list(group_by_vars):
        if group_var in var_definitions:
            expression = var_definitions[group_var]
            expr_vars = set(re.findall(r'\?(\w+)', expression))

            # Check if expression uses variables not in GROUP BY
            if expr_vars - group_by_vars:
                pattern = rf'\b\?{group_var}\b'
                replacement = f'({expression})'

                new_group_by_clause = re.sub(pattern, replacement, group_by_clause)

                if new_group_by_clause != group_by_clause:
                    new_group_by_full = group_by_full_match.replace(
                        group_by_clause,
                        new_group_by_clause
                    )
                    fixed_query = fixed_query.replace(group_by_full_match, new_group_by_full)

                    group_by_clause = new_group_by_clause
                    group_by_full_match = new_group_by_full
                    group_by_vars.remove(group_var)
                    group_by_vars.update(expr_vars)

                    fix_msg = f"Replaced GROUP BY '?{group_var}' with expression '({expression})'"
                    issues.append(fix_msg)
                    logger.info(fix_msg)

    # Fix 2: Add missing non-aggregated variables
    missing_vars = non_aggregated_vars - group_by_vars

    if missing_vars:
        additional_vars = ' '.join(f'?{var}' for var in sorted(missing_vars))
        new_group_by_clause = f'{group_by_clause} {additional_vars}'.strip()

        new_group_by_full = group_by_full_match.replace(
            group_by_clause,
            new_group_by_clause
        )
        fixed_query = fixed_query.replace(group_by_full_match, new_group_by_full)

        fix_msg = f"Added missing variables to GROUP BY: {missing_vars}"
        issues.append(fix_msg)
        logger.info(fix_msg)

    # Fix 3: Remove invalid variables from GROUP BY (aggregated results)
    invalid_vars = group_by_vars & aggregate_result_vars

    if invalid_vars:
        new_group_by_clause = group_by_clause
        for invalid_var in invalid_vars:
            pattern = rf'\s*\?{invalid_var}\b'
            new_group_by_clause = re.sub(pattern, '', new_group_by_clause)

        new_group_by_clause = ' '.join(new_group_by_clause.split())  # Clean whitespace

        if new_group_by_clause != group_by_clause:
            new_group_by_full = group_by_full_match.replace(
                group_by_clause,
                new_group_by_clause
            )
            fixed_query = fixed_query.replace(group_by_full_match, new_group_by_full)

            fix_msg = f"Removed invalid aggregate result variables from GROUP BY: {invalid_vars}"
            issues.append(fix_msg)
            logger.info(fix_msg)

    return fixed_query


def _add_group_by_clause(
    query: str,
    group_vars: set[str],
    var_definitions: dict[str, str],
    issues: list[str]
) -> str:
    """
    Add GROUP BY clause to a query that needs it but doesn't have one.

    Args:
        query: Original SPARQL query
        group_vars: Variables that should be in GROUP BY
        var_definitions: Mapping of variables to their expressions
        issues: List to append fix messages to

    Returns:
        Query with GROUP BY clause added
    """
    # Build GROUP BY clause
    group_by_parts = []

    for var in sorted(group_vars):
        if var in var_definitions:
            # Use the expression, not the variable
            expression = var_definitions[var]
            group_by_parts.append(f'({expression})')
        else:
            group_by_parts.append(f'?{var}')

    group_by_clause = 'GROUP BY ' + ' '.join(group_by_parts)

    # Find insertion point (before ORDER BY, LIMIT, OFFSET, or final brace)
    insertion_match = re.search(
        r'(\s+)(ORDER\s+BY|LIMIT|OFFSET|\})',
        query,
        re.IGNORECASE
    )

    if insertion_match:
        # Insert before the matched keyword
        insert_pos = insertion_match.start(1)
        fixed_query = (
            query[:insert_pos] +
            '\n' + group_by_clause +
            query[insert_pos:]
        )
    else:
        # Add at end before final brace or end of query
        if query.rstrip().endswith('}'):
            insert_pos = query.rstrip().rfind('}')
            fixed_query = (
                query[:insert_pos] +
                group_by_clause + '\n' +
                query[insert_pos:]
            )
        else:
            fixed_query = query.rstrip() + '\n' + group_by_clause

    fix_msg = f"Added GROUP BY clause with variables: {group_vars}"
    issues.append(fix_msg)
    logger.info(fix_msg)

    return fixed_query


def _extract_variable_definitions(select_clause: str) -> dict[str, str]:
    """
    Extract variable definitions from SELECT clause.

    Finds patterns like: (EXPRESSION AS ?variable)

    Args:
        select_clause: SELECT clause content

    Returns:
        Dictionary mapping variable names to their expressions

    Example:
        "(SUBSTR(STR(?timestamp), 1, 7) AS ?month)" -> {"month": "SUBSTR(STR(?timestamp), 1, 7)"}
    """
    definitions = {}

    # Pattern for nested expressions: (expr AS ?var)
    # Handle balanced parentheses
    pattern = r'\(([^()]+(?:\([^()]*\)[^()]*)*)\s+AS\s+\?(\w+)\)'
    matches = re.findall(pattern, select_clause, re.IGNORECASE)

    for expr, var_name in matches:
        definitions[var_name] = expr.strip()

    return definitions


def _extract_grouping_variables(group_by_clause: str) -> set[str]:
    """
    Extract actual grouping variables from GROUP BY clause.

    Excludes variables inside function calls or expressions.
    Only returns simple variable references like ?var.

    Args:
        group_by_clause: GROUP BY clause content (without "GROUP BY" prefix)

    Returns:
        Set of variable names used for grouping

    Example:
        "?epochNumber (SUBSTR(?date, 1, 7))" -> {"epochNumber"}
    """
    # Remove all expressions in parentheses (including functions)
    temp_clause = group_by_clause

    # Iteratively remove parenthesized expressions
    max_iterations = 10
    for _ in range(max_iterations):
        before = temp_clause
        temp_clause = re.sub(r'\([^()]*\)', '', temp_clause)
        if temp_clause == before:
            break

    # Extract remaining simple variables
    variables = set(re.findall(r'\?(\w+)', temp_clause))

    return variables


def _extract_aggregate_result_variables(select_clause: str) -> set[str]:
    """
    Extract variables that are results of aggregate functions.

    Finds patterns like: COUNT(...) AS ?var, SUM(...) AS ?var

    Args:
        select_clause: SELECT clause content

    Returns:
        Set of variable names that hold aggregate results

    Example:
        "(COUNT(?tx) AS ?totalTxs)" -> {"totalTxs"}
    """
    pattern = r'(?:COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|SAMPLE)\s*\([^)]*\)\s+AS\s+\?(\w+)'
    matches = re.findall(pattern, select_clause, re.IGNORECASE)
    return set(matches)


def _extract_aggregated_variables(select_clause: str) -> set[str]:
    """
    Extract variables used inside aggregate functions.

    Finds variables that appear within COUNT(), SUM(), etc.

    Args:
        select_clause: SELECT clause content

    Returns:
        Set of variable names used inside aggregates

    Example:
        "COUNT(?tx) SUM(?value)" -> {"tx", "value"}
    """
    # Find all aggregate function calls and extract variables from inside them
    aggregate_pattern = r'(?:COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|SAMPLE)\s*\(([^)]*)\)'
    aggregate_contents = re.findall(aggregate_pattern, select_clause, re.IGNORECASE)

    variables = set()
    for content in aggregate_contents:
        # Extract variables from the aggregate content
        vars_in_agg = re.findall(r'\?(\w+)', content)
        variables.update(vars_in_agg)

    return variables


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

def detect_and_parse_sparql(sparql_text: str, nl_query) -> tuple[bool, Union[str, list[dict[str, Any]]]]:
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
        fixed_query = ensure_validity(sparql_text, nl_query)
        return False, fixed_query

def ensure_validity(sparql_query: str, nl_query: str) -> str:
    cleaned = _clean_sparql(sparql_query)
    cleaned = _ensure_prefixes(cleaned)

    # Validate and fix
    _, fixed_query, issues = _validate_and_fix_sparql(cleaned, nl_query)
    if issues:
        logger.info(f"SPARQL validation results: {'; '.join(issues)}")

    return fixed_query

TAIL_RE = re.compile(
    r"""
    \s*
    (?:
        LIMIT\s+\S+
        (?:\s+OFFSET\s+\S+)?
    )?
    \s*$
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

def force_limit_cap(query: str, limit_cap: int = 3500) -> str:
    """
    Ensures the query has an appropriate LIMIT based on limit_cap:
    - If limit_cap is 0: removes existing LIMIT and sets LIMIT 0
    - If limit_cap > 0: only updates LIMIT if existing LIMIT is larger than limit_cap
    - Only modifies the outermost query LIMIT
    """
    q = query.rstrip().rstrip(";")

    if limit_cap == 0:
        # Remove existing trailing LIMIT/OFFSET and set LIMIT 0
        q = TAIL_RE.sub("", q).rstrip()
        return f"{q}\nLIMIT 0"

    # Extract existing LIMIT value from outermost query
    limit_match = re.search(
        r'LIMIT\s+(\d+)\s*(?:OFFSET\s+\d+)?\s*$',
        q,
        re.IGNORECASE
    )

    if limit_match:
        existing_limit = int(limit_match.group(1))
        # Only update if existing limit is larger than limit_cap
        if existing_limit > limit_cap:
            q = TAIL_RE.sub("", q).rstrip()
            return f"{q}\nLIMIT {limit_cap}"
        else:
            # Keep existing limit as is
            return query.rstrip().rstrip(";")
    else:
        # No existing LIMIT, add limit_cap
        q = TAIL_RE.sub("", q).rstrip()
        return f"{q}\nLIMIT {limit_cap}"