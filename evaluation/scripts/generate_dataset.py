"""
CLI: generate evaluation dataset JSONL.

Example:
python -m evaluation.scripts.generate_dataset --seed 7 --variants-per-base 6
"""
from __future__ import annotations

import argparse

from evaluation.src.dataset.dataset_builder import build_dataset


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="evaluation/datasets/raw/msgs.txt")
    ap.add_argument("--out", default="evaluation/datasets/generated/nlq_eval_dataset.jsonl")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--variants-per-base", type=int, default=6)
    args = ap.parse_args()

    counts = build_dataset(
        source_msgs_path=args.source,
        out_path=args.out,
        seed=args.seed,
        variants_per_base=args.variants_per_base,
    )
    print("Dataset written:", args.out)
    print("Counts:", counts)


if __name__ == "__main__":
    main()
