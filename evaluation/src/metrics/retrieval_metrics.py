"""
Information retrieval metrics for top-k retrieval.

We treat the "correct" retrieval as: the cached entry whose base_id matches
the dataset example's base_id.

The evaluator captures retrieved examples via LLMClient.nl_to_sparql(..., _eval_retrieved_out=[...])
which contains Redis cache objects with at least "original_query" and "normalized_query".
"""
from dataclasses import dataclass
import math


@dataclass(frozen=True)
class RetrievalScores:
    precision_at_k: float
    recall_at_k: float
    mrr_at_k: float
    ndcg_at_k: float


def score_retrieval(
    retrieved_base_ids: list[str],
    gold_base_id: str,
    k: int,
) -> RetrievalScores:
    topk = retrieved_base_ids[:k]
    rel = [1 if bid == gold_base_id else 0 for bid in topk]

    precision = sum(rel) / k if k > 0 else 0.0
    recall = 1.0 if any(rel) else 0.0
    mrr = 0.0
    for idx, r in enumerate(rel, start=1):
        if r:
            mrr = 1.0 / idx
            break

    # nDCG with binary relevance
    dcg = 0.0
    for i, r in enumerate(rel, start=1):
        if r:
            dcg += 1.0 / math.log2(i + 1)

    idcg = 1.0  # best possible is having the single relevant item at rank 1
    ndcg = dcg / idcg if idcg > 0 else 0.0

    return RetrievalScores(
        precision_at_k=precision,
        recall_at_k=recall,
        mrr_at_k=mrr,
        ndcg_at_k=ndcg,
    )
