"""
Run the NL→SPARQL pipeline with evaluation hooks.

This wrapper is intentionally small and mirrors the production flow:
- optional cache check (Redis)
- optional few-shot retrieval for NL→SPARQL (SimilarityService via LLMClient)
- SPARQL execution
- final answer generation (LLM)

We keep this independent from FastAPI endpoints to run as a CLI.
"""
import time
import json
from dataclasses import dataclass
from typing import Any

from cap.rdf.cache.query_normalizer import QueryNormalizer
from cap.services.redis_nl_client import get_redis_nl_client
from cap.services.llm_client import get_llm_client, cleanup_llm_client
from cap.services.similarity_service import SearchStrategy
from cap.services.sparql_service import execute_sparql
from cap.util.sparql_util import detect_and_parse_sparql
from cap.util.sparql_result_processor import convert_sparql_to_kv, format_for_llm


@dataclass
class PipelineRun:
    cache_hit: bool
    retrieved: list[dict[str, Any]]
    sparql: str
    execution_success: bool
    result_non_empty: bool
    final_answer: str
    latency_retrieval_ms: int
    latency_nl_to_sparql_ms: int
    latency_sparql_exec_ms: int
    latency_final_answer_ms: int
    latency_end_to_end_ms: int


async def run_pipeline(
    user_query: str,
    expected_base_nl_query: str,
    use_cache: bool,
    use_ontology: bool,
    use_fewshot: bool,
    fewshot_strategy: SearchStrategy,
) -> PipelineRun:
    t0 = time.time()

    llm = get_llm_client()
    redis = get_redis_nl_client()

    cache_hit = False
    retrieved: list[dict[str, Any]] = []
    sparql_query = ""
    sparql_queries = None

    # ---------- cache ----------
    if use_cache:
        normalized = QueryNormalizer.normalize(user_query)
        cached = await redis.get_cached_query_with_original(normalized, user_query)
        if cached:
            cache_hit = True
            cached_sparql = cached.get("sparql_query", "") or ""
            is_sequential = bool(cached.get("is_sequential", False))

            if is_sequential:
                sparql_queries = json.loads(cached_sparql)
            else:
                sparql_query = cached_sparql

    # ---------- retrieval + NL→SPARQL ----------
    t_retrieval_start = time.time()
    t_nl2sparql_start = time.time()

    if not cache_hit:
        retrieved = []
        raw_sparql_response = await llm.nl_to_sparql(
            natural_query=user_query,
            conversation_history=[],
            use_ontology=use_ontology,
            use_fewshot=use_fewshot,
            fewshot_strategy=fewshot_strategy,
            fewshot_top_n=3,
            _eval_retrieved_out=retrieved,
        )

        is_sequential, sparql_content = detect_and_parse_sparql(raw_sparql_response, user_query)
        if is_sequential:
            sparql_queries = sparql_content
            sparql_query = ""
        else:
            sparql_query = sparql_content
            sparql_queries = None

    t_nl2sparql_end = time.time()
    retrieval_ms = int((t_nl2sparql_end - t_retrieval_start) * 1000)

    # Note: we cannot cleanly separate retrieval vs nl2sparql timings without
    # patching internals; we still expose total for this stage.
    nl_to_sparql_ms = int((t_nl2sparql_end - t_nl2sparql_start) * 1000)

    # ---------- SPARQL execution ----------
    t_exec_start = time.time()
    sparql_dict = await execute_sparql(sparql_query, is_sequential, sparql_queries)
    t_exec_end = time.time()

    error_msg = sparql_dict.get("error_msg")
    execution_success = error_msg in (None, "", "null")
    sparql_results = sparql_dict.get("sparql_results") or []
    result_non_empty = bool(sparql_results)

    sparql_exec_ms = int((t_exec_end - t_exec_start) * 1000)

    # ---------- final answer generation (required) ----------
    t_final_start = time.time()
    kv = convert_sparql_to_kv(sparql_results)
    kv_for_llm = format_for_llm(kv)

    prompt = f"""
User Question: {user_query}

Current utc date and time: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}.

This is the data you MUST consider in your answer:
{kv_for_llm}
"""
    if len(prompt) > 18000:
        prompt = prompt[:18000]

    chunks = []
    async for chunk in llm.generate_stream(
        prompt=prompt,
        model=llm.llm_model,
        system_prompt="",
        temperature=0.1,
    ):
        chunks.append(chunk)

    final_answer = "".join(chunks)

    t_final_end = time.time()

    final_answer_ms = int((t_final_end - t_final_start) * 1000)
    t1 = time.time()

    return PipelineRun(
        cache_hit=cache_hit,
        retrieved=retrieved,
        sparql=sparql_query,
        execution_success=execution_success,
        result_non_empty=result_non_empty,
        final_answer=final_answer,
        latency_retrieval_ms=retrieval_ms,
        latency_nl_to_sparql_ms=nl_to_sparql_ms,
        latency_sparql_exec_ms=sparql_exec_ms,
        latency_final_answer_ms=final_answer_ms,
        latency_end_to_end_ms=int((t1 - t0) * 1000),
    )
