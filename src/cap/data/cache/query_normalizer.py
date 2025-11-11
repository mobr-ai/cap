"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import logging
import re
import unicodedata
from pathlib import Path
from opentelemetry import trace

from cap.data.cache.semantic_matcher import SemanticMatcher

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

ontology_path: str = "src/ontologies/cardano.ttl"

# Static global for preserved expressions
_PRESERVED_EXPRESSIONS = []

def _load_ontology_labels(onto_path: str = "src/ontologies/cardano.ttl") -> list:
    """Load rdfs:label values from the Turtle ontology file."""
    labels = []
    try:
        path = Path(onto_path)
        if not path.exists():
            logger.warning(f"Ontology file not found at {onto_path}")
            return labels

        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Match rdfs:label patterns in Turtle format
        # Handles both single-line and multi-line string literals
        label_pattern = r'rdfs:label\s+"([^"]+)"'
        matches = re.findall(label_pattern, content)

        for match in matches:
            label_lower = match.lower().strip()
            if label_lower and len(label_lower) > 1:  # Skip empty or single-char labels
                labels.append(label_lower)

        logger.info(f"Loaded {len(labels)} labels from ontology: {onto_path}")

    except Exception as e:
        logger.error(f"Error loading ontology labels from {onto_path}: {e}")

    return labels

class QueryNormalizer:
    """Handle natural language query normalization."""

    PRESERVED_EXPRESSIONS = [
        'asset policy', 'proof of work', 'proof of stake', 'stake pool',
        'native token', 'smart contract', 'ada pots', 'pot transfer',
        'collateral input', 'collateral output', 'reference input', 'fungible token'
        'chain selection rule'
    ]

    TEMPORAL_TERMS = {
        r'\b(yearly|annually|per year|each year|every year)\b': 'per <<PERIOD>>',
        r'\b(monthly|per month|each month|every month)\b': 'per <<PERIOD>>',
        r'\b(weekly|per week|each week|every week)\b': 'per <<PERIOD>>',
        r'\b(daily|per day|each day|every day)\b': 'per <<PERIOD>>',
        r'\b(per epoch|each epoch|every epoch)\b': 'per <<PERIOD>>'
    }

    ORDERING_TERMS = {
        r'\b(first|earliest|oldest|initial)\s+\d+\b': '<<ORDER_START>> <<N>>',
        r'\b(last|latest|newest|most recent|recent)\s+\d+\b': '<<ORDER_END>> <<N>>',
        r'\b(top)\s+\d+\b': '<<ORDER_TOP>> <<N>>',
        r'\b(bottom|worst)\s+\d+\b': '<<ORDER_BOTTOM>> <<N>>',
        r'\b(largest|biggest|highest|greatest)\b(?=\s+(number|count|amount|value))': '<<ORDER_MAX>>',
        r'\b(smallest|lowest|least)\b(?=\s+(number|count|amount|value))': '<<ORDER_MIN>>'
    }

    CHART_TYPE_PATTERNS = {
        r'\b(bar|column|histogram)\s+(chart|graph|plot)\b': 'bar visualization',
        r'\b(line|linear|trend)\s+(chart|graph|plot)\b': 'line visualization',
        r'\b(pie|donut|doughnut)\s+(chart|graph|plot)\b': 'pie visualization',
    }

    ENTITY_MAPPINGS = {
        # Governance and Certificates (more specific first)
        r'\b(drep (registration|update|retirement))s?\b': 'ENTITY_DREP_CERT',
        r'\b(stake pool retirement)s?\b': 'ENTITY_POOL_RETIREMENT',
        r'\b(governance (proposal|action))s?\b': 'ENTITY_PROPOSAL',
        r'\b(voting (anchor|procedure))s?\b': 'ENTITY_VOTING_ANCHOR',
        r'\b(constitutional committee)s?\b': 'ENTITY_COMMITTEE',
        r'\b(committee (member|credential))s?\b': 'ENTITY_COMMITTEE_MEMBER',
        r'\b((cold|hot) credential)s?\b': 'ENTITY_CREDENTIAL',
        r'\b(delegated representative|drep)s?\b': 'ENTITY_DREP',
        r'\b(delegation)s?\b': 'ENTITY_DELEGATION',
        r'\b(vote|casts vote)s?\b': 'ENTITY_VOTE',
        r'\b(certificate)s?\b': 'ENTITY_CERTIFICATE',
        r'\b(constitution)s?\b': 'ENTITY_CONSTITUTION',

        # Scripts and Smart Contracts
        r'\b(plutus script|native script|script|smart contract)s?\b': 'ENTITY_SCRIPT',
        r'\b((key|transaction) witness)s?\b': 'ENTITY_WITNESS',
        r'\b(datum)s?\b': 'ENTITY_DATUM',
        r'\b((cost model|execution units)s?)\b': 'ENTITY_COST_MODEL',

        # Tokens and Assets
        r'\b((multi[- ]?asset cardano native token|multi asset cnt|cnt|native token|cardano native token|token state|non[- ]?fungible token|nft|fungible token)s?)s?\b': 'ENTITY_TOKEN',
        r'\b(ada pot)s?\b': 'ENTITY_ADA_POTS',

        r'\b((protocol)s? parameter)s?\b': 'ENTITY_PROTOCOL_PARAMS',

        # System and Status
        r'\b((etl progress|what is happening|sync status|current (status|tip|height)s?)s?)s?\b': 'ENTITY_STATUS',

        r'\b(reward withdrawal)s?\b': 'ENTITY_REWARD_WITHDRAWAL',
        r'\b(input)s?\b': 'ENTITY_UTXO_INPUT',
        r'\b(output)s?\b': 'ENTITY_UTXO_INPUT',

        r'\b(stake pool)s?\b': 'ENTITY_POOL',
        r'\b(cnt)s?\b': 'ENTITY_TOKEN',
        r'\b(account|wallet)s?\b': 'ENTITY_ACCOUNT',
        r'\b(transaction|tx)s?\b': 'ENTITY_TX',
        r'\b(block)s?\b': 'ENTITY_BLOCK',
        r'\b(epoch)s?\b': 'ENTITY_EPOCH',
        r'\b(stake pool|pool)s?\b(?!\s+owner)': 'ENTITY_POOL',  # Only if not "pool owner"
    }


    COMPARISON_PATTERNS = {
        r'\b(more than|over|above|greater than|exceeding|beyond)\b': 'above',
        r'\b(less than|under|below|fewer than)\b': 'below',
        r'\b(equal to|equals|exactly)\b': 'equals',
    }

    FILLER_WORDS = {
        'please', 'could', 'can', 'you', 'me', 'the',
        'is', 'are', 'was', 'were', 'your', 'my',
        'a', 'an', 'of', 'in', 'on', 'yours',
        'do', 'does', 'ever'
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
    def ensure_expressions() -> None:
        global _PRESERVED_EXPRESSIONS

        if not _PRESERVED_EXPRESSIONS:
            # Load labels from ontology
            ontology_labels = _load_ontology_labels(ontology_path)

            # Add default expressions if ontology loading failed or returned nothing
            if not ontology_labels:
                logger.warning("No ontology labels loaded, using default preserved expressions")
                ontology_labels = [
                    'asset policy', 'proof of work', 'proof of stake', 'stake pool',
                    'native token', 'smart contract', 'ada pots', 'pot transfer',
                    'collateral input', 'collateral output', 'reference input', 'fungible token',
                    'chain selection rule'
                ]

            _PRESERVED_EXPRESSIONS = ontology_labels
            logger.info(f"Initialized PRESERVED_EXPRESSIONS with {len(_PRESERVED_EXPRESSIONS)} terms")

    @staticmethod
    def normalize(query: str) -> str:
        """Normalize natural language query for better cache hits."""
        QueryNormalizer.ensure_expressions()
        normalized = query.lower()
        normalized = unicodedata.normalize('NFKD', normalized)
        normalized = normalized.encode('ascii', 'ignore').decode('ascii')

        # Replace punctuation with spaces and normalize whitespace FIRST
        normalized = re.sub(r'[?.!,;:\-\(\)\[\]{}\'\"]+', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        # Remove possessive 's
        normalized = re.sub(r'\b(\w+)\'s\b', r'\1', normalized)

        # Replace multi-word expressions with single tokens temporarily
        expression_map = {}
        for i, expr in enumerate(QueryNormalizer.PRESERVED_EXPRESSIONS):
            if expr in normalized:
                placeholder = f'__EXPR{i}__'
                expression_map[placeholder] = expr.replace(' ', '_')
                normalized = normalized.replace(expr, placeholder)

        # Normalize definition requests to a standard form
        normalized = re.sub(
            r'\b(define|explain|describe|tell me about|whats?)\s+(an?|the)?\s*',
            'what ',
            normalized
        )
        # Also handle "what is/are" variations
        normalized = re.sub(
            r'\bwhat\s+(is|are|was|were)\s+(an?|the)?\s*',
            'what ',
            normalized
        )

        normalized = QueryNormalizer._normalize_aggregation_terms(normalized)

        # Handle ordinal dates (1st, 2nd, 3rd, 4th, etc.)
        normalized = re.sub(
            r'\b(\d{1,2})(st|nd|rd|th)?\s*,?\s*(\d{4})\b',
            r'<<DAY>> <<YEAR>>',
            normalized
        )
        normalized = re.sub(
            r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\s+(\d{1,2})(st|nd|rd|th)?\s*,?\s*(\d{4})\b',
            r'\1 <<DAY>> <<YEAR>>',
            normalized,
            flags=re.IGNORECASE
        )

        for pattern, replacement in QueryNormalizer.ENTITY_MAPPINGS.items():
            normalized = re.sub(pattern, replacement, normalized)

        if not re.search(r'\b(max|maximum|min|minimum)\s+(supply|value|amount|limit)', normalized):
            normalized = re.sub(r'\b(maximum|max)\b(?=\s+(number|count))', '<<ORDER_MAX>>', normalized)
            normalized = re.sub(r'\b(minimum|min)\b(?=\s+(number|count))', '<<ORDER_MIN>>', normalized)

        # temporal patterns
        normalized = re.sub(r'\b(in|of|for|during)?\s*\d{4}\b', ' <<YEAR>> ', normalized)
        normalized = re.sub(
            r'\b(january|february|march|april|may|june|july|august|september|october|november|december|'
            r'jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*\d{4}\b',
            ' <<MONTH>> ', normalized
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
        normalized = re.sub(r'\s+', ' ', normalized).strip()

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
        token_pattern = r'\b(ada|snek|hosky|[a-z]{3,10})\b(?=\s+(holder|token|account))'
        # Don't normalize token names that appear in "what is X" or "define X" contexts
        if not re.search(r'\b(what is|define|explain)\s+(a|an|the)?\s*\w+\s+(token|cnt)', normalized):
            normalized = re.sub(
                token_pattern,
                lambda m: '<<TOKEN>>' if m.group(1) not in ['many', 'much', 'what', 'which', 'have', 'define'] else m.group(1),
                normalized
            )

        # formatted and plain numbers
        normalized = re.sub(r'\b\d{1,3}(?:[,._]\d{3})+(?:\.\d+)?\b(?!\s*%)', '<<N>>', normalized)
        normalized = re.sub(r'\b\d+(?:\.\d+)?\b(?!\s*%)', '<<N>>', normalized)

        for pattern, replacement in QueryNormalizer.CHART_TYPE_PATTERNS.items():
            normalized = re.sub(pattern, replacement, normalized)

        # Clean up
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized)

        # Remove filler words and sort
        words = normalized.split()

        # Remove filler words but preserve question words at start
        question_words_found = []
        content_words = []

        for word in words:
            # Preserve placeholder patterns
            if word.startswith('ENTITY_') or word.startswith('<<'):
                content_words.append(word)
            elif word in QueryNormalizer.QUESTION_WORDS and not question_words_found:
                question_words_found.append(word)
            elif word not in QueryNormalizer.FILLER_WORDS:
                content_words.append(word)

        # Sort only the content words, keep question words at start
        content_words.sort()
        result = ' '.join(question_words_found + content_words).strip()

        # Apply semantic normalization BEFORE restoring expressions
        result = SemanticMatcher.normalize_for_matching(result)

        for placeholder, expr in expression_map.items():
            result = result.replace(placeholder, expr)

        # Validate minimum content - LOOSEN THIS CHECK
        if not result or len(result) < 3:  # Changed from len(result.split()) < 2
            logger.warning(f"Normalization produced too short result for: {query}")
            # Fallback: just lowercase and remove punctuation
            fallback = query.lower()
            fallback = re.sub(r'[?.!,;:\-\(\)\[\]{}\'\"]+', '', fallback)
            return ' '.join(fallback.split())  # normalize whitespace

        logger.debug(f"Normalized '{query}' -> '{result}'")
        return result
