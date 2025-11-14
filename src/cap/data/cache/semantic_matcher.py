import re

class SemanticMatcher:
    """Match queries based on semantic similarity, not just exact normalization."""

    SEMANTIC_GROUPS = {
        'latest': [
            'latest', 'most recent', 'newest', 'last', 'current',
            'recent', 'recently', 'fresh', 'up to date', 'updated'
        ],
        'oldest': [
            'oldest', 'older', 'past', 'first', 'earliest',
            'long ago', 'initial', 'beginning', 'original'
        ],
        'count': [
            'how many', 'number of', 'count', 'amount of',
            'quantity', 'total number', 'how much'
        ],
        'sum': [
            'sum', 'total', 'add up', 'aggregate', 'combined',
            'accumulated', 'overall amount'
        ],
        'aggregate_time': [
            'over time', 'trend', 'historical', 'progression',
            'evolution', 'timeline', 'time series', 'history',
            'per year', 'per month', 'per day', 'by year', 'by month'
        ],
        'top_ranked': [
            'top', 'largest', 'biggest', 'highest', 'most',
            'best', 'leading', 'upper', 'ascending', 'asc',
            'top ranked', 'greatest', 'max', 'maximum'
        ],
        'bottom_ranked': [
            'bottom', 'lowest', 'smallest', 'least', 'worst',
            'lower', 'descending', 'desc', 'bottom ranked',
            'min', 'minimum'
        ],
        'display_action': [
            'show', 'display', 'list', 'get', 'create', 'give me',
            'fetch', 'return', 'pull', 'provide', 'output'
        ]
    }

    CHART_GROUPS = {
        'bar': [
            'bar', 'bar chart', 'bars', 'histogram', 'column chart'
        ],
        'pie': [
            'pie', 'pie chart', 'pizza', 'donut', 'doughnut', 'circle chart'
        ],
        'line': [
            'line', 'line chart', 'timeseries', 'time series', 'trend',
            'timeline', 'curve', 'line graph'
        ],
        'table': [
            'list', 'table', 'tabular', 'display', 'show', 'get', 'grid',
            'dataset', 'rows', 'columns'
        ],
    }

    # Equivalent comparison terms (normalized forms)
    COMPARISON_EQUIVALENTS = {
        'above': [
            'above', 'over', 'more than', 'greater than', 'exceeding',
            'beyond', 'higher than', 'greater', '>', 'at least'
        ],
        'below': [
            'below', 'under', 'less than', 'fewer than', 'lower than',
            'smaller than', '<', 'at most'
        ],
        'equals': [
            'equals', 'equal to', 'exactly', 'same as', 'is', 'match',
            'matches', 'identical to', '=', 'precisely'
        ],
    }

    # Equivalent possession/relationship terms
    POSSESSION_EQUIVALENTS = {
        'hold': [
            'hold', 'holds', 'has', 'have', 'owns', 'own', 'possess',
            'possesses', 'contains', 'contain', 'includes', 'include',
            'carrying', 'carry'
        ],
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


        # Normalize display/action verbs to canonical form
        display_terms = SemanticMatcher.SEMANTIC_GROUPS["display_action"]
        for term in display_terms:
            result = re.sub(rf'\b{re.escape(term)}\b', 'show', result)

        # Normalize table/visualization terms
        result = re.sub(r'\b(table|visualization|chart|graph)\b', 'table', result)

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