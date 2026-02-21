"""
Aggregate metrics and write a readable summary.
"""
from collections import defaultdict
import json
from pathlib import Path


def aggregate(results: list[dict]) -> dict:
    """
    Results: list of per-example dicts. Returns a nested summary per config.
    """
    by_cfg: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        by_cfg[r["config_name"]].append(r)

    summary = {}
    for cfg, rows in by_cfg.items():
        n = len(rows)
        if n == 0:
            continue

        def avg(field: str) -> float:
            return sum(float(x.get(field, 0.0)) for x in rows) / n

        def rate(field: str) -> float:
            return sum(1.0 for x in rows if x.get(field)) / n

        summary[cfg] = {
            "n": n,
            "retrieval": {
                "precision@5": avg("retrieval_precision@5"),
                "recall@5": avg("retrieval_recall@5"),
                "mrr@5": avg("retrieval_mrr@5"),
                "ndcg@5": avg("retrieval_ndcg@5"),
            },
            "generation": {
                "sparql_parseable_rate": rate("sparql_parseable"),
                "sparql_exact_match_rate": rate("sparql_exact_match"),
                "execution_success_rate": rate("execution_success"),
                "result_non_empty_rate": rate("result_non_empty"),
                "final_answer_non_empty_rate": rate("final_answer_non_empty"),
                "e2e_success_rate": rate("e2e_success"),
            },
            "latency_ms": {
                "retrieval_avg": avg("latency_retrieval_ms"),
                "nl_to_sparql_avg": avg("latency_nl_to_sparql_ms"),
                "sparql_exec_avg": avg("latency_sparql_exec_ms"),
                "final_answer_avg": avg("latency_final_answer_ms"),
                "end_to_end_avg": avg("latency_end_to_end_ms"),
            }
        }

    return summary


def write_run_artifacts(outdir: str, per_example: list[dict]) -> dict:
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    (out / "per_example.jsonl").write_text(
        "\n".join(json.dumps(x, ensure_ascii=False) for x in per_example) + "\n",
        encoding="utf-8",
    )

    summary = aggregate(per_example)
    (out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    md = _to_markdown(summary)
    (out / "summary.md").write_text(md, encoding="utf-8")

    return summary


def _to_markdown(summary: dict) -> str:
    lines = ["# Evaluation summary\n"]
    for cfg, s in summary.items():
        lines.append(f"## {cfg}\n")
        lines.append(f"- N: {s['n']}\n")
        r = s["retrieval"]
        g = s["generation"]
        l = s["latency_ms"]
        lines.append(f"**Retrieval**: P@5={r['precision@5']:.3f}, R@5={r['recall@5']:.3f}, MRR@5={r['mrr@5']:.3f}, nDCG@5={r['ndcg@5']:.3f}\n")
        lines.append(f"**Gen/Exec**: parseable={g['sparql_parseable_rate']:.3f}, exact={g['sparql_exact_match_rate']:.3f}, exec_ok={g['execution_success_rate']:.3f}, non_empty={g['result_non_empty_rate']:.3f}, final={g['final_answer_non_empty_rate']:.3f}, e2e={g['e2e_success_rate']:.3f}\n")
        lines.append(f"**Latency (ms avg)**: retrieval={l['retrieval_avg']:.1f}, nl2sparql={l['nl_to_sparql_avg']:.1f}, exec={l['sparql_exec_avg']:.1f}, final={l['final_answer_avg']:.1f}, e2e={l['end_to_end_avg']:.1f}\n")
        lines.append("\n---\n")
    return "\n".join(lines)
