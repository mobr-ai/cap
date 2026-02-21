"""
CLI: run evaluation with ablations, write a run folder.

Example:
python -m evaluation.scripts.run_evaluation --dataset evaluation/datasets/generated/nlq_eval_dataset.jsonl --outdir evaluation/runs/run_001
"""
from __future__ import annotations

import argparse
import asyncio

from evaluation.src.runner.ablations import default_configs
from evaluation.src.runner.evaluate import evaluate_dataset
from evaluation.src.metrics.report import write_run_artifacts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="evaluation/datasets/generated/nlq_eval_dataset.jsonl")
    ap.add_argument("--outdir", default="evaluation/runs/run_001")
    args = ap.parse_args()

    async def _run():
        per_ex = await evaluate_dataset(
            dataset_path=args.dataset,
            configs=default_configs(),
            outdir=args.outdir,
            k=5,
        )
        summary = write_run_artifacts(args.outdir, per_ex)
        print("Wrote:", args.outdir)
        return summary

    asyncio.run(_run())


if __name__ == "__main__":
    main()
