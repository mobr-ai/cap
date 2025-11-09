"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import logging
import re
import unicodedata
from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class QueryNormalizer:
    """Handle natural language query normalization."""

    TEMPORAL_TERMS = {
        r'\b(yearly|annually|per year|each year|every year)\b': 'per <<PERIOD>>',
        r'\b(monthly|per month|each month|every month)\b': 'per <<PERIOD>>',
        r'\b(weekly|per week|each week|every week)\b': 'per <<PERIOD>>',
        r'\b(daily|per day|each day|every day)\b': 'per <<PERIOD>>',
        r'\b(per epoch|each epoch|every epoch)\b': 'per <<PERIOD>>'
    }

    ORDERING_TERMS = {
        r'\b(first|earliest|oldest|initial)\s+(\d+)?\b': '<<ORDER_START>>',
        r'\b(last|latest|newest|most recent|recent)\s+(\d+)?\b': '<<ORDER_END>>',
        r'\b(top)\s+(\d+)?\b': '<<ORDER_TOP>>',
        r'\b(bottom|worst)\s+(\d+)?\b': '<<ORDER_BOTTOM>>',
        r'\b(largest|biggest|highest|maximum|max|greatest)\b': '<<ORDER_MAX>>',
        r'\b(smallest|lowest|minimum|min|least)\b': '<<ORDER_MIN>>'
    }

    CHART_TYPE_PATTERNS = {
        r'\b(bar|column|histogram)\s+(chart|graph|plot)\b': 'bar visualization',
        r'\b(line|linear|trend)\s+(chart|graph|plot)\b': 'line visualization',
        r'\b(pie|donut|doughnut)\s+(chart|graph|plot)\b': 'pie visualization',
    }

    ENTITY_MAPPINGS = {
        r'\b(wallet|account|address)s?\b': 'ENTITY_ACCOUNT',
        r'\b(transaction|tx)s?\b': 'ENTITY_TX',
        r'\b(block)s?\b': 'ENTITY_BLOCK',
        r'\b(epoch)s?\b': 'ENTITY_EPOCH',
        r'\b(native token|cnt|multi[- ]?asset)s?\b': 'ENTITY_TOKEN',
        r'\b(stake pool|pool)s?\b': 'ENTITY_POOL',
    }

    COMPARISON_PATTERNS = {
        r'\b(more than|over|above|greater than|exceeding|beyond)\b': 'above',
        r'\b(less than|under|below|fewer than)\b': 'below',
        r'\b(equal to|equals|exactly)\b': 'equals',
    }

    FILLER_WORDS = {
        'please', 'could', 'can', 'you', 'show', 'me', 'the', 'plot', 'have',
        'is', 'are', 'was', 'were', 'tell', 'define', 'your', 'my',
        'give', 'find', 'get', 'a', 'an', 'of', 'in', 'on', 'draw', 'yours',
        'do', 'does', 'showing', 'table', 'display',
        'bar', 'line', 'chart', 'graph', 'pie', 'list', 'create', 'delete'
    }

    QUESTION_WORDS = {'who', 'what', 'when', 'where', 'why', 'which', 'how many', 'how much', 'how long'}

    @staticmethod
    def _normalize_aggregation_terms(text: str) -> str:
        """Normalize various temporal aggregation phrasings."""
        # Normalize time period aggregations
        text = re.sub(
            r'\b(number|count|amount|total)\s+of\s+([a-z]+)\s+(per|by|each|every)\s+',
            r'\2 per ',
            text
        )

        # Normalize "over time" patterns
        text = re.sub(
            r'\b(over|across|through|throughout)\s+(time|period|duration)\b',
            'over time',
            text
        )

        return text

    @staticmethod
    def normalize(query: str) -> str:
        """Normalize natural language query for better cache hits."""
        normalized = query.lower()
        normalized = unicodedata.normalize('NFKD', normalized)
        normalized = normalized.encode('ascii', 'ignore').decode('ascii')

        normalized = QueryNormalizer._normalize_aggregation_terms(normalized)

        # temporal patterns
        normalized = re.sub(r'\b(in|of|for|during)?\s*\d{4}\b', '<<YEAR>>', normalized)
        normalized = re.sub(
            r'\b(january|february|march|april|may|june|july|august|september|october|november|december|'
            r'jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*\d{4}\b',
            '<<MONTH>>', normalized
        )
        normalized = re.sub(
            r'\b(first|last|second|third)\s+(week|day|month)\s+of\s+<<YEAR>>\b',
            '<<PERIOD_RANGE>>',
            normalized
        )
        normalized = re.sub(
            r'\b(on|in|during|for)\s+(<<MONTH>>|<<YEAR>>)\b',
            'in <<TIME>>',
            normalized
        )
        normalized = re.sub(r'\b\d{4}-\d{2}\b', '<<MONTH>>', normalized)
        normalized = re.sub(r'\bweek\s+of\s+<<YEAR>>\b', 'week of <<YEAR>>', normalized)
        normalized = re.sub(r'\bweek\s+\d+\b', 'week <<N>>', normalized)

        # temporal aggregation terms
        for pattern, replacement in QueryNormalizer.TEMPORAL_TERMS.items():
            normalized = re.sub(pattern, replacement, normalized)

        for pattern, replacement in QueryNormalizer.COMPARISON_PATTERNS.items():
            normalized = re.sub(pattern, replacement, normalized)

        # ordering terms
        for pattern, replacement in QueryNormalizer.ORDERING_TERMS.items():
            normalized = re.sub(pattern, replacement, normalized)

        # numeric patterns
        normalized = re.sub(r'\btop\s+\d+\b', 'top __N__', normalized)
        normalized = re.sub(
            r'\b\d+(?:\.\d+)?\s+(?:billion(?:s)?|million(?:s)?|thousand(?:s)?|hundred(?:s)?)\b',
            '<<N>>', normalized, flags=re.IGNORECASE
        )

        # token names
        normalized = re.sub(
            r'\b(ada|snek|hosky|[a-z]{3,10})\b(?=\s+(holder|token|account))',
            lambda m: '<<TOKEN>>' if m.group(1) not in ['many', 'much', 'what', 'which', 'have', 'define'] else m.group(1),
            normalized
        )

        # formatted and plain numbers
        normalized = re.sub(r'\b\d{1,3}(?:[,._]\d{3})+(?:\.\d+)?\b(?!\s*%)', '<<N>>', normalized)
        normalized = re.sub(r'\b\d+(?:\.\d+)?\b(?!\s*%)', '<<N>>', normalized)

        for pattern, replacement in QueryNormalizer.CHART_TYPE_PATTERNS.items():
            normalized = re.sub(pattern, replacement, normalized)

        for pattern, replacement in QueryNormalizer.ENTITY_MAPPINGS.items():
            normalized = re.sub(pattern, replacement, normalized)

        # Clean up
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized)

        # Remove filler words and sort
        words = normalized.split()
        filtered_words = [w for w in words if w not in QueryNormalizer.FILLER_WORDS]

        question_start = []
        remaining_words = []
        for word in filtered_words:
            if word in QueryNormalizer.QUESTION_WORDS and not question_start:
                question_start.append(word)
            else:
                remaining_words.append(word)

        remaining_words.sort()
        return ' '.join(question_start + remaining_words).strip()
