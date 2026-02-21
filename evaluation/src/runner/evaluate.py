"""
Evaluation runner: loads dataset JSONL, runs ablations, computes metrics, writes artifacts.
"""
import json
from pathlib import Path
from typing import Any

from evaluation.src.runner.ablations import EvalConfig
from evaluation.src.runner.pipeline_wrapper import run_pipeline
from evaluation.src.metrics.retrieval_metrics import score_retrieval
from evaluation.src.metrics.generation_metrics import score_generation


def load_jsonl(path: str) -> list[dict[str, Any]]:
    rows = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


async def evaluate_dataset(
    dataset_path: str,
    configs: list[EvalConfig],
    outdir: str,
    k: int = 5,
) -> list[dict[str, Any]]:
    rows = load_jsonl(dataset_path)
    per_example_results: list[dict[str, Any]] = []

    # Build gold id lookup for retrieval: base_id by normalized base query
    base_id_by_base_nl: dict[str, str] = {r["base_nl_query"]: r["base_id"] for r in rows}

    for cfg in configs:
        for ex in rows:
            run = await run_pipeline(
                user_query=ex["nl_query"],
                expected_base_nl_query=ex["base_nl_query"],
                use_cache=cfg.use_cache,
                use_ontology=cfg.use_ontology,
                use_fewshot=cfg.use_fewshot,
                fewshot_strategy=cfg.fewshot_strategy,
            )

            # retrieval base ids (best-effort: map retrieved original_query back to base_id)
            retrieved_base_ids: list[str] = []
            for item in run.retrieved:
                oq = item.get("original_query") or ""
                bid = base_id_by_base_nl.get(oq)
                if bid:
                    retrieved_base_ids.append(bid)

            r_scores = score_retrieval(
                retrieved_base_ids=retrieved_base_ids,
                gold_base_id=ex["base_id"],
                k=k,
            ) if run.retrieved else score_retrieval([], ex["base_id"], k)

            g_scores = score_generation(
                generated_sparql=run.sparql,
                expected_sparql=ex["expected_sparql"],
                execution_success=run.execution_success,
                result_non_empty=run.result_non_empty,
                final_answer=run.final_answer,
            )

            per_example_results.append({
                "config_name": cfg.name,
                "example_id": ex["id"],
                "split": ex["split"],
                "base_id": ex["base_id"],
                "variant_type": ex["variant_type"],
                "tags": ex["tags"],

                "cache_expected_hit": ex["cache_expected_hit"],
                "cache_actual_hit": run.cache_hit,

                "retrieval_precision@5": r_scores.precision_at_k,
                "retrieval_recall@5": r_scores.recall_at_k,
                "retrieval_mrr@5": r_scores.mrr_at_k,
                "retrieval_ndcg@5": r_scores.ndcg_at_k,

                "sparql_parseable": g_scores.sparql_parseable,
                "sparql_exact_match": g_scores.sparql_exact_match,
                "execution_success": g_scores.execution_success,
                "result_non_empty": g_scores.result_non_empty,
                "final_answer_non_empty": g_scores.final_answer_non_empty,
                "e2e_success": g_scores.e2e_success,

                "latency_retrieval_ms": run.latency_retrieval_ms,
                "latency_nl_to_sparql_ms": run.latency_nl_to_sparql_ms,
                "latency_sparql_exec_ms": run.latency_sparql_exec_ms,
                "latency_final_answer_ms": run.latency_final_answer_ms,
                "latency_end_to_end_ms": run.latency_end_to_end_ms,
            })

    return per_example_results
