"""
Parse the cached NL/SPARQL pairs from a chat-style export like `msgs.txt`.

This module is intentionally independent from Redis and from the runtime pipeline.
"""
from dataclasses import dataclass
from pathlib import Path
import re


@dataclass(frozen=True)
class NLSPARQLPair:
    base_id: str
    nl_query: str
    sparql: str


_USER_RE = re.compile(r"^MESSAGE user\s+(.*)\s*$")
_ASSISTANT_START_RE = re.compile(r"^MESSAGE assistant\s+\"\"\"\s*$")
_ASSISTANT_END_RE = re.compile(r"^\"\"\"\s*$")


def parse_msgs_file(path: str | Path) -> list[NLSPARQLPair]:
    p = Path(path)
    text = p.read_text(encoding="utf-8", errors="replace").splitlines()

    pairs: list[NLSPARQLPair] = []

    i = 0
    while i < len(text):
        m_user = _USER_RE.match(text[i])
        if not m_user:
            i += 1
            continue

        nlq = m_user.group(1).strip()
        i += 1

        # find assistant start
        while i < len(text) and not _ASSISTANT_START_RE.match(text[i]):
            i += 1
        if i >= len(text):
            break
        i += 1  # consume assistant start

        sparql_lines: list[str] = []
        while i < len(text) and not _ASSISTANT_END_RE.match(text[i]):
            sparql_lines.append(text[i])
            i += 1
        # consume assistant end if present
        if i < len(text) and _ASSISTANT_END_RE.match(text[i]):
            i += 1

        sparql = "\n".join(sparql_lines).strip()
        if not nlq or not sparql:
            continue

        base_id = _stable_id(nlq, sparql)
        pairs.append(NLSPARQLPair(base_id=base_id, nl_query=nlq, sparql=sparql))

    return pairs


def _stable_id(nlq: str, sparql: str) -> str:
    import hashlib
    h = hashlib.sha1()
    h.update(nlq.encode("utf-8"))
    h.update(b"\n---\n")
    h.update(sparql.encode("utf-8"))
    return h.hexdigest()[:12]
