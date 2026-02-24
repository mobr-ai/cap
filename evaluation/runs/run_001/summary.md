# Evaluation summary

## cache+ontology+fewshot:auto

- N: 42

**Retrieval**: P@5=0.119, R@5=0.595, MRR@5=0.595, nDCG@5=0.595

**Gen/Exec**: parseable=1.000, exact=0.000, exec_ok=0.952, non_empty=0.952, final=1.000, e2e=0.952

**Latency (ms avg)**: retrieval=6580.7, nl2sparql=6580.7, exec=3857.0, final=9515.9, e2e=19957.5


---

## cache+ontology+fewshot:embeddings

- N: 42

**Retrieval**: P@5=0.119, R@5=0.595, MRR@5=0.595, nDCG@5=0.595

**Gen/Exec**: parseable=1.000, exact=0.000, exec_ok=0.976, non_empty=0.976, final=0.976, e2e=0.952

**Latency (ms avg)**: retrieval=6269.1, nl2sparql=6269.1, exec=3601.2, final=17801.4, e2e=27674.3


---

## cache+ontology+fewshot:jaccard

- N: 42

**Retrieval**: P@5=0.119, R@5=0.595, MRR@5=0.595, nDCG@5=0.595

**Gen/Exec**: parseable=1.000, exact=0.000, exec_ok=0.976, non_empty=0.976, final=1.000, e2e=0.976

**Latency (ms avg)**: retrieval=7465.8, nl2sparql=7465.8, exec=4799.8, final=11674.9, e2e=23942.9


---

## cache+ontology+no_fewshot

- N: 42

**Retrieval**: P@5=0.000, R@5=0.000, MRR@5=0.000, nDCG@5=0.000

**Gen/Exec**: parseable=1.000, exact=0.000, exec_ok=0.595, non_empty=0.595, final=1.000, e2e=0.595

**Latency (ms avg)**: retrieval=8389.1, nl2sparql=8389.1, exec=2804.8, final=7112.5, e2e=18308.6


---

## no_cache+ontology+fewshot:auto

- N: 42

**Retrieval**: P@5=0.190, R@5=0.952, MRR@5=0.952, nDCG@5=0.952

**Gen/Exec**: parseable=1.000, exact=0.000, exec_ok=0.976, non_empty=0.976, final=0.976, e2e=0.952

**Latency (ms avg)**: retrieval=10595.5, nl2sparql=10595.5, exec=3836.4, final=19057.7, e2e=33490.5


---

## no_cache+no_ontology+fewshot:auto

- N: 42

**Retrieval**: P@5=0.190, R@5=0.952, MRR@5=0.952, nDCG@5=0.952

**Gen/Exec**: parseable=1.000, exact=0.000, exec_ok=0.095, non_empty=0.095, final=1.000, e2e=0.095

**Latency (ms avg)**: retrieval=11057.5, nl2sparql=11057.5, exec=97.5, final=4023.7, e2e=15179.7


---

## no_cache+no_ontology+no_fewshot

- N: 42

**Retrieval**: P@5=0.000, R@5=0.000, MRR@5=0.000, nDCG@5=0.000

**Gen/Exec**: parseable=1.000, exact=0.000, exec_ok=0.000, non_empty=0.000, final=1.000, e2e=0.000

**Latency (ms avg)**: retrieval=11879.8, nl2sparql=11879.8, exec=12.6, final=4158.5, e2e=16051.8


---
