import logging
import re
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)

ADA_CURRENCY_URI = "https://mobr.ai/ont/cardano#cnt/ada"
LOVELACE_TO_ADA = Decimal("1000000")


def _contains_lovelace_to_ada_division(expr: str) -> bool:
    return bool(re.search(r"/\s*(?:1000000|1000000\.0|1_000_000)\b", expr))


def _iter_projection_expressions(query_text: str):
    """
    Yields balanced parenthesized projection expressions, e.g.

      (SUM(xsd:decimal(?lovelaceValue)) AS ?controlledLovelace)
      (COUNT(DISTINCT ?unspentOutput) AS ?utxoCount)

    This avoids the previous greedy aggregate regex accidentally spanning
    across multiple SELECT expressions.
    """
    depth = 0
    start = None

    for i, ch in enumerate(query_text):
        if ch == "(":
            if depth == 0:
                start = i + 1
            depth += 1
        elif ch == ")":
            if depth == 0:
                continue

            depth -= 1
            if depth == 0 and start is not None:
                yield query_text[start:i]
                start = None


def _query_text(sparql_query: str | list[Any] | dict[str, Any]) -> str:
    if isinstance(sparql_query, list):
        return " ".join(
            q.get("query", "") if isinstance(q, dict) else str(q)
            for q in sparql_query
        )
    if isinstance(sparql_query, dict):
        query = sparql_query.get("query")
        return query if isinstance(query, str) else str(sparql_query)
    return sparql_query or ""


def detect_ada_variables(sparql_query: str | list[Any] | dict[str, Any]) -> set[str]:
    query_text = _query_text(sparql_query)
    if not query_text:
        return set()

    ada_vars: set[str] = set()

    direct_amount_predicates = (
        "hasFee",
        "hasTxOutputValue",
        "hasValue",
        "hasTotalSupply",
        "hasMaxSupply",
    )
    predicate_pattern = "|".join(re.escape(p) for p in direct_amount_predicates)

    for match in re.finditer(
        rf"(?:{predicate_pattern})\s+\?(\w+)",
        query_text,
        re.IGNORECASE,
    ):
        ada_vars.add(match.group(1))

    lines = query_text.splitlines()
    for i, line in enumerate(lines):
        if ADA_CURRENCY_URI not in line:
            continue

        context = "\n".join(lines[max(0, i - 5): min(len(lines), i + 6)])
        for match in re.finditer(
            r"(?:hasValue|hasTotalSupply|hasMaxSupply)\s+\?(\w+)",
            context,
            re.IGNORECASE,
        ):
            ada_vars.add(match.group(1))

    changed = True
    while changed:
        before = len(ada_vars)

        # Projection alias:
        # (?source AS ?alias)
        for source_var, alias_var in re.findall(
            r"\(\s*\?(\w+)\s+AS\s+\?(\w+)\s*\)",
            query_text,
            re.IGNORECASE,
        ):
            if source_var in ada_vars:
                ada_vars.add(alias_var)

        # BIND expressions:
        # BIND(?value AS ?x)
        # BIND(COALESCE(?value, 0) AS ?x)
        # BIND(xsd:decimal(?value) AS ?x)
        # BIND((?value / 1000000) AS ?x)
        for expr, result_var in re.findall(
            r"BIND\s*\(\s*(.*?)\s+AS\s+\?(\w+)\s*\)",
            query_text,
            re.IGNORECASE | re.DOTALL,
        ):
            if _contains_lovelace_to_ada_division(expr):
                continue

            source_vars = re.findall(r"\?(\w+)", expr)
            if any(source_var in ada_vars for source_var in source_vars):
                ada_vars.add(result_var)

        # Aggregate / projection expressions.
        #
        # Do NOT use a greedy regex across the whole query here. It can cross
        # expression boundaries and incorrectly classify COUNT aliases such as
        # ?utxoCount as ADA when another expression in the same SELECT contains
        # ?lovelaceValue.
        for projection_expr in _iter_projection_expressions(query_text):
            alias_match = re.search(
                r"\bAS\s+\?(\w+)\s*$",
                projection_expr,
                re.IGNORECASE | re.DOTALL,
            )
            if not alias_match:
                continue

            result_var = alias_match.group(1)
            expr = projection_expr[:alias_match.start()]

            # COUNT results are cardinalities, never ADA amounts.
            if re.search(r"\bCOUNT\s*\(", expr, re.IGNORECASE):
                continue

            # If the query already divides lovelace by 1,000,000, the projected
            # value is already ADA. Do not convert it again.
            if _contains_lovelace_to_ada_division(expr):
                continue

            # Only propagate ADA-ness through real numeric expressions derived
            # from already-known lovelace variables.
            source_vars = re.findall(r"\?(\w+)", expr)
            if any(source_var in ada_vars for source_var in source_vars):
                ada_vars.add(result_var)

        changed = len(ada_vars) != before

    return ada_vars


def convert_lovelace_to_ada(value: str) -> dict[str, Any]:
    try:
        lovelace = Decimal(str(value))
        ada = lovelace / LOVELACE_TO_ADA

        return {
            "value": str(ada),
            "type": "literal",
            "datatype": "decimal",
            "unit": "ADA",
            "lovelace": str(value),
            "ada": str(ada),
        }

    except (ValueError, TypeError, InvalidOperation):
        return {
            "value": str(value),
            "lovelace": str(value).split(".")[0],
        }


def convert_cardano_result_value(
    var_name: str,
    value: Any,
    sparql_query: str = "",
) -> Any:
    if var_name not in detect_ada_variables(sparql_query):
        return value

    if not isinstance(value, (str, int, float, Decimal)):
        return value

    try:
        Decimal(str(value))
    except (InvalidOperation, ValueError):
        return value

    return convert_lovelace_to_ada(str(value))


def format_cardano_result_value(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None

    if "lovelace" in value and "ada" in value:
        return f"{value.get('ada', '')} ADA ({value.get('lovelace', '')} lovelace)"

    return None
