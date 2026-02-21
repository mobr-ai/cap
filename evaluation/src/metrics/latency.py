"""
Latency capture helpers.
"""
from dataclasses import dataclass


@dataclass
class LatenciesMs:
    retrieval_ms: int = 0
    nl_to_sparql_ms: int = 0
    sparql_exec_ms: int = 0
    final_answer_ms: int = 0
    end_to_end_ms: int = 0
