"""
Tag inference for dataset examples.

Requirement: use PatternRegistry to infer tags.

We infer tags based on:
- chart intent (bar/line/pie/table/etc.)
- temporal granularity (daily/weekly/monthly/yearly/epoch)
- entities (nft, token, tx, block, epoch, account, pool, ...)
- ordering/limit terms (top/latest/earliest/etc.)

Tags are used to slice evaluation results and to verify the generator
creates meaningful lexical variation (synonyms/paraphrases) rather than
trivial normalization hits.
"""

import re
from dataclasses import dataclass

from cap.rdf.cache.pattern_registry import PatternRegistry
from cap.rdf.cache.query_normalizer import QueryNormalizer


@dataclass(frozen=True)
class TagSet:
    tags: list[str]

    def as_list(self) -> list[str]:
        return list(self.tags)


def infer_tags(nl_query: str) -> TagSet:
    low = nl_query.lower()
    tags: set[str] = set()

    # ----- chart type intent -----
    if _matches_any(low, PatternRegistry.BAR_CHART_TERMS):
        tags.add("intent:chart:bar")
    if _matches_any(low, PatternRegistry.LINE_CHART_TERMS):
        tags.add("intent:chart:line")
    if _matches_any(low, PatternRegistry.PIE_CHART_TERMS):
        tags.add("intent:chart:pie")
    if _matches_any(low, PatternRegistry.SCATTER_CHART_TERMS):
        tags.add("intent:chart:scatter")
    if _matches_any(low, PatternRegistry.BUBBLE_CHART_TERMS):
        tags.add("intent:chart:bubble")
    if _matches_any(low, PatternRegistry.TREEMAP_TERMS):
        tags.add("intent:chart:treemap")
    if _matches_any(low, PatternRegistry.HEATMAP_TERMS):
        tags.add("intent:chart:heatmap")
    if _matches_any(low, PatternRegistry.TABLE_TERMS):
        tags.add("intent:table")

    # ----- temporal granularity -----
    if re.search(PatternRegistry.build_pattern(PatternRegistry.DAILY_TERMS), low):
        tags.add("time:daily")
    if re.search(PatternRegistry.build_pattern(PatternRegistry.WEEKLY_TERMS), low):
        tags.add("time:weekly")
    if re.search(PatternRegistry.build_pattern(PatternRegistry.MONTHLY_TERMS), low):
        tags.add("time:monthly")
    if re.search(PatternRegistry.build_pattern(PatternRegistry.YEARLY_TERMS), low):
        tags.add("time:yearly")
    if re.search(PatternRegistry.build_pattern(PatternRegistry.EPOCH_PERIOD_TERMS), low):
        tags.add("time:epoch")

    # explicit months
    if _matches_any(low, PatternRegistry.MONTH_NAMES) or _matches_any(low, PatternRegistry.MONTH_ABBREV):
        tags.add("time:month_name")

    # relative windows
    if _matches_any(low, ["today"]):
        tags.add("time:today")
    if _matches_any(low, ["yesterday"]):
        tags.add("time:yesterday")
    if _matches_any(low, ["last week", "past week"]):
        tags.add("time:last_week")
    if _matches_any(low, ["last month", "past month"]):
        tags.add("time:last_month")
    if _matches_any(low, ["last 24 hours", "past 24 hours"]):
        tags.add("time:last_24h")

    # ----- ordering/limit -----
    if re.search(PatternRegistry.build_pattern(PatternRegistry.TOP_TERMS), low):
        tags.add("order:top")
    if re.search(PatternRegistry.build_pattern(PatternRegistry.LATEST_TERMS), low):
        tags.add("order:latest")
    if re.search(PatternRegistry.build_pattern(PatternRegistry.EARLIEST_TERMS), low):
        tags.add("order:earliest")

    # ----- entities via QueryNormalizer's entity patterns -----
    entity_patterns = QueryNormalizer.get_entity_patterns()
    for pat, ent in entity_patterns.items():
        if re.search(pat, low):
            tags.add(f"entity:{ent.lower()}")

    if not tags:
        tags.add("uncategorized")

    return TagSet(sorted(tags))


def _matches_any(text: str, phrases: list[str]) -> bool:
    return any(p in text for p in phrases)
