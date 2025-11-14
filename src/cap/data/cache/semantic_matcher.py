import re

from cap.data.cache.pattern_registry import PatternRegistry

class SemanticMatcher:
    """Match queries based on semantic similarity, not just exact normalization."""

    SEMANTIC_GROUPS = {
        'latest': PatternRegistry.LAST_TERMS,
        'oldest': PatternRegistry.FIRST_TERMS,
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
        Normalize query for semantic matching by:
        1. Replacing equivalent terms with canonical forms.
        2. Handling word variations (e.g., plural forms).
        3. Removing filler/redundant words.
        4. Normalizing whitespace.
        """
        result = normalized_query.lower()  # case-insensitive matching

        # Normalize to canonical forms
        for d in SemanticMatcher.get_semantic_dicts():
            for canonical, variants in d.items():
                for variant in variants:
                    words = variant.split()
                    if len(words) > 1:
                        # Multi-word variant: only pluralize last word
                        last_word = re.escape(words[-1])
                        prefix = r"\s+".join(re.escape(w) for w in words[:-1])
                        pattern = rf'\b{prefix}\s+{last_word}s?\b'
                    else:
                        # Single-word variant
                        pattern = rf'\b{re.escape(variant)}s?\b'

                    result = re.sub(pattern, canonical, result, flags=re.IGNORECASE)

        # Remove redundant/filler words
        redundant_words = SemanticMatcher.SEMANTIC_SUGAR + PatternRegistry.FILLER_WORDS
        if redundant_words:
            filler_pattern = r'\b(?:' + '|'.join(map(re.escape, redundant_words)) + r')\b'
            result = re.sub(filler_pattern, '', result, flags=re.IGNORECASE)

        # Normalize whitespace
        result = re.sub(r'\s+', ' ', result).strip()

        return result
