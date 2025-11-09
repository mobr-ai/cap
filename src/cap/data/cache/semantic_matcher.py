import re

class SemanticMatcher:
    """Match queries based on semantic similarity, not just exact normalization."""

    SEMANTIC_GROUPS = {
        'list_latest': ['latest', 'most recent', 'newest', 'last', 'current'],
        'count': ['how many', 'number of', 'count', 'total'],
        'aggregate_time': ['over time', 'trend', 'historical', 'progression'],
        'top_ranked': ['top', 'largest', 'biggest', 'highest', 'most'],
    }

    @staticmethod
    def get_semantic_variant(normalized_query: str) -> str:
        """Generate semantic variant key for better matching."""
        variant = normalized_query

        for group_name, terms in SemanticMatcher.SEMANTIC_GROUPS.items():
            pattern = '|'.join(re.escape(term) for term in terms)
            if re.search(pattern, variant):
                variant = re.sub(pattern, f'<<{group_name}>>', variant)

        return variant