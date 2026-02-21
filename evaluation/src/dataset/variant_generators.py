"""
Variant generation (paraphrases + synonym substitutions).

Design goals:
- Create *real* lexical + syntactic variety (not just punctuation/case).
- Avoid variants that collapse to the same cache key after QueryNormalizer.normalize().
- Preserve intent and entity constraints so the expected SPARQL stays the same.

We intentionally keep synonym selection deterministic under a random seed.
"""
from dataclasses import dataclass
import random
import re

from cap.rdf.cache.query_normalizer import QueryNormalizer
from cap.rdf.cache.pattern_registry import PatternRegistry


@dataclass(frozen=True)
class Variant:
    text: str
    variant_type: str  # "paraphrase" | "synonym" | "mixed"


# Domain-aware synonym lexicon (offline, deterministic, high-signal).
# We avoid brittle general-purpose thesauri because this is a semantic-search benchmark,
# not a general English paraphraser.
_SYNONYMS: dict[str, list[str]] = {
    "show": ["list", "display", "give me", "return"],
    "plot": ["draw", "chart", "graph"],
    "bar chart": ["histogram", "column chart"],
    "line chart": ["time series chart", "trend line"],
    "transactions": ["txs", "transfers"],
    "transaction": ["tx", "transfer"],
    "fee": ["cost", "transaction fee"],
    "average": ["mean", "avg"],
    "smart contract": ["plutus script", "on-chain script"],
    "smart contracts": ["plutus scripts", "on-chain scripts"],
    "deployed": ["created", "published", "uploaded"],
    "minted": ["issued", "created"],
    "nfts": ["non-fungible tokens", "NFTs"],
    "token": ["asset", "native token"],
    "tokens": ["assets", "native tokens"],
    "accounts": ["wallets", "stake accounts"],
    "created": ["opened", "registered"],
    "daily": ["each day", "per day"],
    "weekly": ["each week", "per week"],
    "monthly": ["each month", "per month"],
    "last week": ["past week", "previous 7 days"],
    "last month": ["past month", "previous 30 days"],
    "today": ["in the last 24 hours", "for the current day"],
    "most frequently": ["most common", "most often"],
    "largest": ["biggest", "highest-value"],
}

# phrase-level replacements first (longer to shorter)
_REPLACEMENT_ORDER = sorted(_SYNONYMS.keys(), key=len, reverse=True)


def generate_variants(
    base_query: str,
    variants_per_base: int,
    rng: random.Random,
) -> list[Variant]:
    base_norm = QueryNormalizer.normalize(base_query)
    seen_norm: set[str] = {base_norm}

    variants: list[Variant] = []

    # Start with template paraphrases driven by patterns
    templates = _build_paraphrase_templates(base_query)

    for t in templates:
        if len(variants) >= variants_per_base:
            break
        cand = t
        cn = QueryNormalizer.normalize(cand)
        if cn in seen_norm:
            continue
        seen_norm.add(cn)
        variants.append(Variant(text=cand, variant_type="paraphrase"))

    # Then add synonym and mixed variants
    attempts = 0
    while len(variants) < variants_per_base and attempts < variants_per_base * 30:
        attempts += 1
        mode = rng.choice(["synonym", "mixed"])
        cand = base_query

        if mode in ("synonym", "mixed"):
            cand = apply_synonyms(cand, rng, max_replacements=3)

        if mode == "mixed":
            cand = apply_light_rewrites(cand, rng)

        cn = QueryNormalizer.normalize(cand)
        if cn in seen_norm:
            continue
        seen_norm.add(cn)
        variants.append(Variant(text=cand, variant_type=mode))

    return variants


def apply_synonyms(text: str, rng: random.Random, max_replacements: int = 2) -> str:
    out = text
    replacements_done = 0

    low = out.lower()
    for key in _REPLACEMENT_ORDER:
        if replacements_done >= max_replacements:
            break
        if key not in low:
            continue
        choices = _SYNONYMS[key]
        repl = rng.choice(choices)

        # preserve capitalization if the original appears capitalized
        pat = re.compile(re.escape(key), re.IGNORECASE)
        out2 = pat.sub(repl, out, count=1)
        if out2 != out:
            out = out2
            low = out.lower()
            replacements_done += 1

    return out


def apply_light_rewrites(text: str, rng: random.Random) -> str:
    """
    Small syntactic rewrites that change surface form but keep meaning.
    (We keep them conservative to avoid changing the expected SPARQL.)
    """
    s = text.strip()

    # Convert imperative -> question form
    if s.lower().startswith(("plot", "draw", "chart", "graph", "show", "list")):
        if rng.random() < 0.5:
            s = "Can you " + s[0].lower() + s[1:]
        else:
            s = "Please " + s[0].lower() + s[1:]

    # Swap "show X in Y" -> "in Y, show X"
    m = re.search(r"^(.*)\s+(in|on|over|during)\s+(.+)$", s, flags=re.IGNORECASE)
    if m and rng.random() < 0.4:
        s = f"In {m.group(3)}, {m.group(1)}"

    # Replace "how many" -> "what number of"
    if re.search(r"\bhow many\b", s, flags=re.IGNORECASE) and rng.random() < 0.6:
        s = re.sub(r"\bhow many\b", "what number of", s, flags=re.IGNORECASE)

    # Replace "what is the average" -> "compute the mean"
    if re.search(r"\bwhat is the average\b", s, flags=re.IGNORECASE) and rng.random() < 0.7:
        s = re.sub(r"\bwhat is the average\b", "compute the mean", s, flags=re.IGNORECASE)

    return s


def _build_paraphrase_templates(base_query: str) -> list[str]:
    """
    Generate deterministic paraphrase templates based on PatternRegistry cues.
    """
    q = base_query.strip()
    low = q.lower()

    out: list[str] = []

    # chart-oriented templates
    if any(t in low for t in PatternRegistry.BAR_CHART_TERMS):
        out.append(re.sub(r"(?i)\bplot\b", "Create", q))
        out.append(re.sub(r"(?i)\bplot a bar chart\b", "Show a histogram", q))
    if any(t in low for t in PatternRegistry.LINE_CHART_TERMS):
        out.append(re.sub(r"(?i)\bplot\b", "Draw", q))
        out.append(re.sub(r"(?i)\bline chart\b", "time series chart", q))
    if any(t in low for t in PatternRegistry.TABLE_TERMS):
        out.append("Give me a table for: " + q)

    # generic question reshapes
    if low.startswith("what is"):
        out.append(re.sub(r"(?i)^what is\b", "Tell me", q))
    if low.startswith("how many"):
        out.append(re.sub(r"(?i)^how many\b", "Count the number of", q))

    # trend shortcut expansions
    if "trends" in low or "trend" in low:
        out.append(q.replace("Trends", "Show me the trends").replace("trends", "show me the trends"))

    # de-duplicate while preserving order
    dedup: list[str] = []
    seen: set[str] = set()
    for s in out:
        s2 = " ".join(s.split())
        if s2 not in seen and s2 != q:
            seen.add(s2)
            dedup.append(s2)

    return dedup
