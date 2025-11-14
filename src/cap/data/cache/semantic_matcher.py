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
            'over time', 'historical', 'progression',
            'evolution', 'history',
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
            'line', 'line chart', 'timeseries', 'time serie', 'trend',
            'timeline', 'curve', 'line graph'
        ],
        'table': [
            'list', 'table', 'tabular', 'display', 'show', 'get', 'grid',
            'dataset', 'row', 'column'
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
            'hold', 'holds', 'has', 'have', 'own', 'possess', 'possesses',
            'contain', 'contain', 'include', 'carrying', 'carry'
        ],
    }

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

        dicts = SemanticMatcher.get_semantic_dicts()

        # Normalize to canonical form
        for d in dicts:
            for canonical, variants in d.items():
                for variant in variants:
                    # Use word boundaries to avoid partial matches
                    result = re.sub(rf'\b({re.escape(variant)})s?\b', canonical, result)

        # Normalize whitespace
        result = re.sub(r'\s+', ' ', result).strip()

        return result
