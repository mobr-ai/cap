"""
Build a JSONL evaluation dataset from cached NL/SPARQL mappings.

Output format: one JSON object per line (no CSV), fields:
- id
- split: train|dev|test (we default to test-heavy because evaluation-focused)
- base_id: stable id of the original cached mapping
- base_nl_query
- nl_query (variant)
- normalized_query
- expected_sparql
- tags
- variant_type
- cache_expected_hit (bool)  # mostly false by design
"""
from pathlib import Path
import json
import random

from cap.rdf.cache.query_normalizer import QueryNormalizer

from evaluation.src.dataset.query_mapping_parser import parse_msgs_file
from evaluation.src.dataset.variant_generators import generate_variants
from evaluation.src.dataset.tag_inferer import infer_tags


def build_dataset(
    source_msgs_path: str,
    out_path: str,
    seed: int = 7,
    variants_per_base: int = 6,
    split_ratios: tuple[float, float, float] = (0.1, 0.1, 0.8),
) -> dict[str, int]:
    rng = random.Random(seed)
    pairs = parse_msgs_file(source_msgs_path)

    # deterministic shuffling
    pairs = sorted(pairs, key=lambda p: p.base_id)
    rng.shuffle(pairs)

    n = len(pairs)
    n_train = int(n * split_ratios[0])
    n_dev = int(n * split_ratios[1])
    # remainder test
    split_map = {}
    for idx, p in enumerate(pairs):
        if idx < n_train:
            split_map[p.base_id] = "train"
        elif idx < n_train + n_dev:
            split_map[p.base_id] = "dev"
        else:
            split_map[p.base_id] = "test"

    out_lines: list[str] = []
    counts = {"train": 0, "dev": 0, "test": 0}

    example_id = 0
    for p in pairs:
        # include the base itself (useful for sanity checks / cache-hit baseline)
        base_norm = QueryNormalizer.normalize(p.nl_query)
        base_ex = {
            "id": f"ex_{example_id:06d}",
            "split": split_map[p.base_id],
            "base_id": p.base_id,
            "base_nl_query": p.nl_query,
            "nl_query": p.nl_query,
            "normalized_query": base_norm,
            "expected_sparql": p.sparql,
            "tags": infer_tags(p.nl_query).as_list(),
            "variant_type": "base",
            "cache_expected_hit": True,
        }
        out_lines.append(json.dumps(base_ex, ensure_ascii=False))
        counts[base_ex["split"]] += 1
        example_id += 1

        variants = generate_variants(p.nl_query, variants_per_base, rng=rng)
        for v in variants:
            norm = QueryNormalizer.normalize(v.text)
            ex = {
                "id": f"ex_{example_id:06d}",
                "split": split_map[p.base_id],
                "base_id": p.base_id,
                "base_nl_query": p.nl_query,
                "nl_query": v.text,
                "normalized_query": norm,
                "expected_sparql": p.sparql,
                "tags": infer_tags(v.text).as_list(),
                "variant_type": v.variant_type,
                "cache_expected_hit": (norm == base_norm),
            }
            # cache_expected_hit should be almost always False for variants (by design)
            out_lines.append(json.dumps(ex, ensure_ascii=False))
            counts[ex["split"]] += 1
            example_id += 1

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    return counts
