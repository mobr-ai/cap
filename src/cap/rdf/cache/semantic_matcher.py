import re
import logging

from cap.rdf.cache.pattern_registry import PatternRegistry

logger = logging.getLogger(__name__)

class SemanticMatcher:
    """Match queries based on semantic similarity, not just exact normalization."""

    SEMANTIC_GROUPS = {
        'latest': PatternRegistry.LATEST_TERMS,
        'oldest': PatternRegistry.EARLIEST_TERMS,
        'count': PatternRegistry.COUNT_TERMS,
        'sum': PatternRegistry.SUM_TERMS,
        'aggregate_time': PatternRegistry.AGGREGATE_TIME_TERMS,
        'top_ranked': PatternRegistry.TOP_TERMS,
        'bottom_ranked': PatternRegistry.BOTTOM_TERMS,
    }

    CHART_GROUPS = {
        'bar': PatternRegistry.BAR_CHART_TERMS,
        'line': PatternRegistry.LINE_CHART_TERMS,
        'pie': PatternRegistry.PIE_CHART_TERMS,
        'table': PatternRegistry.TABLE_TERMS,
    }

    # Equivalent comparison terms (normalized forms)
    COMPARISON_EQUIVALENTS = {
        'above': PatternRegistry.ABOVE_TERMS,
        'below': PatternRegistry.BELOW_TERMS,
        'equals': PatternRegistry.EQUALS_TERMS,
    }

    # Equivalent possession/relationship terms
    POSSESSION_EQUIVALENTS = {
        'hold': PatternRegistry.POSSESSION_TERMS
    }

    # Semantic sugar terms
    SEMANTIC_SUGAR = PatternRegistry.SEMANTIC_SUGAR

    @staticmethod
    def get_semantic_dicts() -> list:
        return [
            SemanticMatcher.COMPARISON_EQUIVALENTS,
            SemanticMatcher.POSSESSION_EQUIVALENTS,
            SemanticMatcher.SEMANTIC_GROUPS,
            SemanticMatcher.CHART_GROUPS
        ]

    @staticmethod
    def normalize_for_matching(normalized_query: str) -> str:
        """
        Further normalize query for semantic matching by:
        1. Replacing equivalent terms with canonical forms
        2. Handling word variations (has/have/hold)
        """
        result = normalized_query
        words = normalized_query.split()

        dicts = SemanticMatcher.get_semantic_dicts()

        # Normalize to semantic canonical form
        for d in dicts:
            for canonical, variants in d.items():
                for variant in variants:
                    # Use word boundaries to avoid partial matches
                    result = re.sub(rf'\b({re.escape(variant)})\b', canonical, result)

        # Remove redundant words that don't change nl meaning after normalization
        reduntant_words = SemanticMatcher.SEMANTIC_SUGAR + PatternRegistry.FILLER_WORDS
        if len(words) > 1:
            reduntant_words.append("VIZ")

        pattern = '|'.join(reduntant_words)
        result = re.sub(rf'\b({pattern})\b', '', result, flags=re.IGNORECASE)

        # Normalize whitespace
        result = re.sub(r'\s+', ' ', result).strip()
        return result
