import re

class SemanticMatcher:
    """Match queries based on semantic similarity, not just exact normalization."""

    SEMANTIC_GROUPS = {
        'list_latest': ['latest', 'most recent', 'newest', 'last', 'current'],
        'count': ['how many', 'number of', 'count', 'total'],
        'aggregate_time': ['over time', 'trend', 'historical', 'progression'],
        'top_ranked': ['top', 'largest', 'biggest', 'highest', 'most', 'by'],
    }

    CHART_GROUPS = {
        'bar': ['bar'],
        'pie': ['pie', 'pizza'],
        'line': ["line", "timeseries", "trend"],
        'table': ["list", "table", "display", "show", "get"],
    }

    # Equivalent comparison terms (normalized forms)
    COMPARISON_EQUIVALENTS = {
        'above': ['above', 'over', 'more than', 'greater than', 'exceeding', 'beyond'],
        'below': ['below', 'under', 'less than', 'fewer than'],
        'equals': ['equals', 'equal to', 'exactly', 'has the same'],
    }

    # Equivalent possession/relationship terms
    POSSESSION_EQUIVALENTS = {
        'hold': ['hold', 'has', 'have', 'owns', 'own', 'possess', 'possesses', 'contains', 'contain', 'holds'],
    }

    @staticmethod
    def normalize_for_matching(normalized_query: str) -> str:
        """
        Further normalize query for semantic matching by:
        1. Replacing equivalent terms with canonical forms
        2. Handling word variations (has/have/hold)
        """
        result = normalized_query

        # Normalize comparison terms to canonical form
        for canonical, variants in SemanticMatcher.COMPARISON_EQUIVALENTS.items():
            for variant in variants:
                if variant in result:
                    result = result.replace(variant, canonical)

        # Normalize possession/relationship terms
        for canonical, variants in SemanticMatcher.POSSESSION_EQUIVALENTS.items():
            for variant in variants:
                # Use word boundaries to avoid partial matches
                result = re.sub(rf'\b{re.escape(variant)}\b', canonical, result)

        # Remove redundant words that don't change meaning after normalization
        result = re.sub(r'\b(more|much)\b', '', result)

        # Normalize whitespace
        result = re.sub(r'\s+', ' ', result).strip()

        return result

    @staticmethod
    def get_semantic_variant(normalized_query: str) -> str:
        """Generate semantic variant key for better matching."""
        variant = normalized_query

        for group_name, terms in SemanticMatcher.SEMANTIC_GROUPS.items():
            pattern = '|'.join(re.escape(term) for term in terms)
            if re.search(pattern, variant):
                variant = re.sub(pattern, f'<<{group_name}>>', variant)

        return variant