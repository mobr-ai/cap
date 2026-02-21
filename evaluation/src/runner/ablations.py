"""
Ablation configs for the evaluation.

We vary:
- use_cache: on/off
- use_ontology: on/off
- use_fewshot: on/off
- fewshot_strategy: embeddings/jaccard/none/auto

Note: `auto` will attempt embeddings and fall back to jaccard.
"""
from dataclasses import dataclass
from cap.services.similarity_service import SearchStrategy


@dataclass(frozen=True)
class EvalConfig:
    name: str
    use_cache: bool
    use_ontology: bool
    use_fewshot: bool
    fewshot_strategy: SearchStrategy


def default_configs() -> list[EvalConfig]:
    return [
        EvalConfig("cache+ontology+fewshot:auto", True, True, True, SearchStrategy.auto),
        EvalConfig("cache+ontology+fewshot:embeddings", True, True, True, SearchStrategy.embeddings),
        EvalConfig("cache+ontology+fewshot:jaccard", True, True, True, SearchStrategy.jaccard),
        EvalConfig("cache+ontology+no_fewshot", True, True, False, SearchStrategy.none),
        EvalConfig("no_cache+ontology+fewshot:auto", False, True, True, SearchStrategy.auto),
        EvalConfig("no_cache+no_ontology+fewshot:auto", False, False, True, SearchStrategy.auto),
        EvalConfig("no_cache+no_ontology+no_fewshot", False, False, False, SearchStrategy.none),
    ]
