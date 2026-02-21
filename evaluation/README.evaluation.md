# Evaluation suite (NL → SPARQL → Execute → Final answer)

This folder adds:
- A **dataset generator** that creates realistic paraphrases + synonym variants from your cached NL/SPARQL pairs (no CSV, no LLM-judge).
- A **pipeline evaluator** that measures **retrieval quality**, **SPARQL generation quality**, **execution success**, **final answer generation**, and **latency**, including **ablation comparisons** (cache / few-shot / embeddings / jaccard / ontology).

## What gets evaluated

### Retrieval (few-shot selection)
When a cache miss happens, the system retrieves top-k similar cached queries (embeddings and/or Jaccard).
We evaluate retrieval against a known ground-truth (the originating cached query used to generate the variant).

Metrics:
- Recall@k, Precision@k
- MRR@k
- nDCG@k

### Generation + execution + final response
For each dataset example we run:
1) optional Redis cache lookup
2) optional similar-query retrieval for few-shot
3) NL→SPARQL generation (LLM)
4) SPARQL execution
5) final answer generation (LLM) — **not optional** in this evaluation

Metrics:
- SPARQL exact match rate (normalized whitespace)
- SPARQL parseable rate (non-empty + syntactically detected)
- SPARQL execution success rate
- Result non-empty rate
- End-to-end success rate (valid SPARQL + executes + final answer produced)
- Latency: retrieval / NL→SPARQL / execution / final answer / end-to-end

### Split

train and dev are used for sanity checks and choosing defatuls, whereas the test is used for the evaluation reporting

#### Train (10%): sanity checks

- regression tests
- optional prompt tuning experiments

#### Dev (10%): choosing ablation defaults

- threshold tuning
- prompt tweaks
- ontology usage decisions

#### Test (80%): final reporting

- ablation comparison
- benchmarks
- production regression tracking

## Quickstart

### 1) Generate dataset

```bash
python -m evaluation.scripts.generate_dataset   --source evaluation/datasets/raw/msgs.txt   --out evaluation/datasets/generated/nlq_eval_dataset.jsonl   --seed 7   --variants-per-base 6
```

### 2) Run evaluation (ablations)

```bash
python -m evaluation.scripts.run_evaluation   --dataset evaluation/datasets/generated/nlq_eval_dataset.jsonl   --outdir evaluation/runs/run_001
```

The run produces:
- `summary.json`
- `per_example.jsonl` (one record per example per ablation config)
- `summary.md`

## Notes

- No `try/except` around imports.
- No CSV.
- No LLM-as-judge.
- Tags are inferred using `PatternRegistry` terms + patterns.

