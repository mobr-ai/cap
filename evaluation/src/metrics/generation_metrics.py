"""
Generation and execution metrics.

No LLM-as-judge. We rely on:
- SPARQL parseable: non-empty string.
- SPARQL exact match (whitespace-normalized) against expected SPARQL.
- Execution success + non-empty results (from execute_sparql()).
- Final answer non-empty.
"""
from dataclasses import dataclass
import re


@dataclass(frozen=True)
class GenerationScores:
    sparql_parseable: bool
    sparql_exact_match: bool
    execution_success: bool
    result_non_empty: bool
    final_answer_non_empty: bool
    e2e_success: bool


def normalize_sparql(s: str) -> str:
    s = s.strip()
    # collapse whitespace but keep inside IRIs untouched (safe enough here)
    s = re.sub(r"\s+", " ", s)
    return s


def score_generation(
    generated_sparql: str,
    expected_sparql: str,
    execution_success: bool,
    result_non_empty: bool,
    final_answer: str,
) -> GenerationScores:
    gen = generated_sparql or ""
    exp = expected_sparql or ""

    parseable = bool(gen.strip())
    exact = normalize_sparql(gen) == normalize_sparql(exp) if parseable else False
    final_non_empty = bool((final_answer or "").strip())

    e2e = parseable and execution_success and final_non_empty

    return GenerationScores(
        sparql_parseable=parseable,
        sparql_exact_match=exact,
        execution_success=execution_success,
        result_non_empty=result_non_empty,
        final_answer_non_empty=final_non_empty,
        e2e_success=e2e,
    )
