"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import logging
import re
import unicodedata
from opentelemetry import trace

from cap.data.cache.semantic_matcher import SemanticMatcher
from cap.data.cache.pattern_registry import PatternRegistry

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class QueryNormalizer:
    """Handle natural language query normalization."""

    @staticmethod
    def get_temporal_patterns() -> dict[str, str]:
        """Generate temporal patterns from registry."""
        return {
            PatternRegistry.build_pattern(PatternRegistry.YEARLY_TERMS): 'per <<PERIOD>>',
            PatternRegistry.build_pattern(PatternRegistry.MONTHLY_TERMS): 'per <<PERIOD>>',
            PatternRegistry.build_pattern(PatternRegistry.WEEKLY_TERMS): 'per <<PERIOD>>',
            PatternRegistry.build_pattern(PatternRegistry.DAILY_TERMS): 'per <<PERIOD>>',
            PatternRegistry.build_pattern(PatternRegistry.EPOCH_PERIOD_TERMS): 'per <<PERIOD>>'
        }

    @staticmethod
    def get_ordering_patterns() -> dict[str, str]:
        """Generate ordering patterns from registry."""
        return {
            PatternRegistry.build_pattern(PatternRegistry.FIRST_TERMS) + r'\s+\d+\b': '<<ORDER_START>> <<N>>',
            PatternRegistry.build_pattern(PatternRegistry.LAST_TERMS) + r'\s+\d+\b': '<<ORDER_END>> <<N>>',
            PatternRegistry.build_pattern(PatternRegistry.TOP_TERMS) + r'\s+\d+\b': '<<ORDER_TOP>> <<N>>',
            PatternRegistry.build_pattern(PatternRegistry.BOTTOM_TERMS) + r'\s+\d+\b': '<<ORDER_BOTTOM>> <<N>>',
            PatternRegistry.build_pattern(PatternRegistry.MAX_TERMS) + r'(?=\s+(number|count|amount|value))': '<<ORDER_MAX>>',
            PatternRegistry.build_pattern(PatternRegistry.MIN_TERMS) + r'(?=\s+(number|count|amount|value))': '<<ORDER_MIN>>'
        }

    @staticmethod
    def get_chart_patterns() -> dict[str, str]:
        """Generate chart type patterns from registry."""
        patterns = {}
        for chart_type, terms in [
            ('bar', PatternRegistry.BAR_CHART_TERMS),
            ('line', PatternRegistry.LINE_CHART_TERMS),
            ('pie', PatternRegistry.PIE_CHART_TERMS)
        ]:
            pattern = PatternRegistry.build_pattern(terms) + r'\s+' + \
                     PatternRegistry.build_pattern(PatternRegistry.CHART_SUFFIXES)
            patterns[pattern] = f'{chart_type} visualization'
        return patterns

    @staticmethod
    def get_entity_patterns() -> dict[str, str]:
        """Generate entity patterns from registry."""
        return {
            # Transaction-related (more specific patterns first)
            PatternRegistry.build_entity_pattern(PatternRegistry.TRANSACTION_TERMS) +
                r'\s+' + PatternRegistry.build_pattern(PatternRegistry.TRANSACTION_DETAIL_TERMS): 'ENTITY_TX_DETAIL',
            r'\b(with|having)\s+' + PatternRegistry.build_pattern(PatternRegistry.TRANSACTION_DETAIL_TERMS): 'ENTITY_DETAIL',

            # Governance and Certificates (more specific first)
            r'\b(drep (registration|update|retirement))s?\b': 'ENTITY_DREP_CERT',
            r'\b(stake pool retirement)s?\b': 'ENTITY_POOL_RETIREMENT',
            PatternRegistry.build_entity_pattern(PatternRegistry.GOVERNANCE_PROPOSAL_TERMS): 'ENTITY_PROPOSAL',
            PatternRegistry.build_entity_pattern(PatternRegistry.VOTING_TERMS): 'ENTITY_VOTING_ANCHOR',
            PatternRegistry.build_entity_pattern(PatternRegistry.COMMITTEE_TERMS): 'ENTITY_COMMITTEE',
            r'\b(committee (member|credential))s?\b': 'ENTITY_COMMITTEE_MEMBER',
            r'\b((cold|hot) credential)s?\b': 'ENTITY_CREDENTIAL',
            PatternRegistry.build_entity_pattern(PatternRegistry.DREP_TERMS): 'ENTITY_DREP',
            PatternRegistry.build_entity_pattern(PatternRegistry.DELEGATION_TERMS): 'ENTITY_DELEGATION',
            PatternRegistry.build_entity_pattern(PatternRegistry.VOTE_TERMS): 'ENTITY_VOTE',
            PatternRegistry.build_entity_pattern(PatternRegistry.CERTIFICATE_TERMS): 'ENTITY_CERTIFICATE',
            PatternRegistry.build_entity_pattern(PatternRegistry.CONSTITUTION_TERMS): 'ENTITY_CONSTITUTION',

            # Scripts and Smart Contracts
            PatternRegistry.build_entity_pattern(PatternRegistry.SCRIPT_TERMS): 'ENTITY_SCRIPT',
            PatternRegistry.build_entity_pattern(PatternRegistry.WITNESS_TERMS): 'ENTITY_WITNESS',
            PatternRegistry.build_entity_pattern(PatternRegistry.DATUM_TERMS): 'ENTITY_DATUM',
            PatternRegistry.build_entity_pattern(PatternRegistry.COST_MODEL_TERMS): 'ENTITY_COST_MODEL',

            # Tokens and Assets
            PatternRegistry.build_entity_pattern(PatternRegistry.TOKEN_TERMS): 'ENTITY_TOKEN',
            PatternRegistry.build_entity_pattern(PatternRegistry.ADA_POT_TERMS): 'ENTITY_ADA_POTS',

            PatternRegistry.build_entity_pattern(PatternRegistry.PROTOCOL_PARAM_TERMS): 'ENTITY_PROTOCOL_PARAMS',

            # System and Status
            PatternRegistry.build_entity_pattern(PatternRegistry.STATUS_TERMS): 'ENTITY_STATUS',
            r'\b((what is happening|what up (cardano)s?)s?)s?\b': 'ENTITY_STATUS',

            PatternRegistry.build_entity_pattern(PatternRegistry.REWARD_TERMS): 'ENTITY_REWARD_WITHDRAWAL',
            PatternRegistry.build_entity_pattern(PatternRegistry.INPUT_TERMS): 'ENTITY_UTXO_INPUT',
            PatternRegistry.build_entity_pattern(PatternRegistry.OUTPUT_TERMS): 'ENTITY_UTXO_INPUT',

            PatternRegistry.build_entity_pattern(PatternRegistry.POOL_TERMS) + r'(?!\s+owner)': 'ENTITY_POOL',
            PatternRegistry.build_entity_pattern(PatternRegistry.ACCOUNT_TERMS): 'ENTITY_ACCOUNT',
            PatternRegistry.build_entity_pattern(PatternRegistry.TRANSACTION_TERMS): 'ENTITY_TX',
            PatternRegistry.build_entity_pattern(PatternRegistry.BLOCK_TERMS): 'ENTITY_BLOCK',
            PatternRegistry.build_entity_pattern(PatternRegistry.EPOCH_TERMS): 'ENTITY_EPOCH',
        }

    @staticmethod
    def get_comparison_patterns() -> dict[str, str]:
        """Generate comparison patterns from registry."""
        return {
            PatternRegistry.build_pattern(PatternRegistry.ABOVE_TERMS): 'above',
            PatternRegistry.build_pattern(PatternRegistry.BELOW_TERMS): 'below',
            PatternRegistry.build_pattern(PatternRegistry.EQUALS_TERMS): 'equals',
        }

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

        # Replace punctuation with spaces and normalize whitespace FIRST
        normalized = re.sub(r'[?.!,;:\-\(\)\[\]{}\'\"]+', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        # Remove possessive 's
        normalized = re.sub(r'\b(\w+)\'s\b', r'\1', normalized)

        # Normalize plurals to singular for better matching
        plural_pattern = PatternRegistry.build_pattern(PatternRegistry.DEFAULT_PRESERVED_EXPRESSIONS, word_boundary=False)
        normalized = re.sub(plural_pattern + r's\b', r'\1', normalized)

        # Replace multi-word expressions with single tokens temporarily
        expression_map = {}
        for i, expr in enumerate(PatternRegistry.get_preserved_expressions()):
            if expr in normalized:
                placeholder = f'__EXPR{i}__'
                expression_map[placeholder] = expr.replace(' ', '_')
                normalized = normalized.replace(expr, placeholder)

        # Normalize definition requests to a standard form
        definition_pattern = PatternRegistry.build_pattern(PatternRegistry.DEFINITION_TERMS)
        normalized = re.sub(
            definition_pattern + r's?\s+(an?|the)?\s*',
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
        ordinal_suffix = '|'.join(PatternRegistry.ORDINAL_SUFFIXES)
        normalized = re.sub(
            rf'\b(\d{{1,2}})({ordinal_suffix})?\s*,?\s*(\d{{4}})\b',
            r'<<DAY>> <<YEAR>>',
            normalized
        )
        normalized = re.sub(
            PatternRegistry.build_pattern(PatternRegistry.MONTH_NAMES) +
                r'\s+(\d{1,2})(st|nd|rd|th)?\s*,?\s*(\d{4})\b',
            r'\1 <<DAY>> <<YEAR>>',
            normalized,
            flags=re.IGNORECASE
        )

        # Normalize limit patterns - handle both explicit and implicit limits
        limit_entities = PatternRegistry.build_pattern(PatternRegistry.DEFAULT_PRESERVED_EXPRESSIONS, word_boundary=False)
        normalized = re.sub(
            PatternRegistry.build_pattern(PatternRegistry.LAST_TERMS) + r'\s+(\d+)\s+' + limit_entities,
            r'\1 <<N>> \2',
            normalized
        )
        # Handle queries without explicit number (implied limit of 1)
        normalized = re.sub(
            PatternRegistry.build_pattern(PatternRegistry.LAST_TERMS) + r'\s+' + limit_entities + r'(?!s)\b',
            r'\1 <<N>> \2',
            normalized
        )

        for pattern, replacement in QueryNormalizer.get_entity_patterns().items():
            normalized = re.sub(pattern, replacement, normalized)

        # Check for supply/value/amount/limit context before normalizing max/min
        max_min_words = PatternRegistry.MAX_TERMS + PatternRegistry.MIN_TERMS
        str_max_min_words = '|'.join(max_min_words)
        bound_words = '|'.join(PatternRegistry.BOUND_TERMS)
        if not re.search(rf'\b({str_max_min_words})\s+({bound_words})', normalized):
            normalized = re.sub(
                PatternRegistry.build_pattern(PatternRegistry.MAX_TERMS) + r'(?=\s+(number|count))',
                '<<ORDER_MAX>>',
                normalized
            )
            normalized = re.sub(
                PatternRegistry.build_pattern(PatternRegistry.MIN_TERMS) + r'(?=\s+(number|count))',
                '<<ORDER_MIN>>',
                normalized
            )

        # temporal patterns
        pattern = f"\\b({'|'.join(PatternRegistry.TEMPORAL_PREPOSITIONS)})?\\s*\\d{{4}}\\b"
        normalized = re.sub(rf'{pattern}', ' <<YEAR>> ', normalized)
        month_year_pattern = PatternRegistry.build_pattern(PatternRegistry.MONTH_NAMES + PatternRegistry.MONTH_ABBREV) + r'\s*\d{4}\b'
        normalized = re.sub(month_year_pattern, ' <<MONTH>> ', normalized)

        period_range = PatternRegistry.build_pattern(PatternRegistry.TIME_PERIOD_RANGE_TERMS)
        period_units = PatternRegistry.build_pattern(PatternRegistry.TIME_PERIOD_UNITS)
        normalized = re.sub(
            period_range + r'\s+' + period_units + r'\s+of\s+<<YEAR>>\b',
            '<<PERIOD_RANGE>>',
            normalized
        )
        time_context = PatternRegistry.build_pattern(PatternRegistry.TEMPORAL_PREPOSITIONS)
        normalized = re.sub(
            time_context + r'\s+(<<MONTH>>|<<YEAR>>)\b',
            'in <<TIME>>',
            normalized
        )
        normalized = re.sub(r'\b\d{4}-\d{2}\b', '<<MONTH>>', normalized)
        normalized = re.sub(r'\bweek\s+of\s+<<YEAR>>\b', 'week of <<YEAR>>', normalized)
        normalized = re.sub(r'\bweek\s+\d+\b', 'week <<N>>', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        # temporal aggregation terms
        for pattern, replacement in QueryNormalizer.get_temporal_patterns().items():
            normalized = re.sub(pattern, replacement, normalized)

        for pattern, replacement in QueryNormalizer.get_comparison_patterns().items():
            normalized = re.sub(pattern, replacement, normalized)

        # ordering terms
        for pattern, replacement in QueryNormalizer.get_ordering_patterns().items():
            normalized = re.sub(pattern, replacement, normalized)

        # numeric patterns
        normalized = re.sub(r'\btop\s+\d+\b', 'top __N__', normalized)
        normalized = re.sub(
            r'\b\d+(?:\.\d+)?\s+(?:billion(?:s)?|million(?:s)?|thousand(?:s)?|hundred(?:s)?)\b',
            '<<N>>', normalized, flags=re.IGNORECASE
        )

        # token names
        # Check for definition contexts more broadly
        def_terms = '|'.join(PatternRegistry.DEFINITION_TERMS)
        is_definition_query = bool(re.search(
            rf'\b({def_terms})\s+(is|are|was|were)?\s+(a|an|the)?\s*\w+',
            normalized
        ))

        token_pattern = r'\b(ada|snek|hosky|[a-z]{3,10})\b(?=\s+(holder|token|account))'

        # Don't normalize if it's a definition query OR if token name is the query subject
        if not is_definition_query:
            # Also preserve token if it appears early in query (likely the subject)
            words = normalized.split()
            subject_tokens = set(words[:5])  # First 5 words likely contain the subject

            normalized = re.sub(
                token_pattern,
                lambda m: ('<<TOKEN>>'
                        if m.group(1) not in PatternRegistry.QUESTION_WORDS
                        and m.group(1) not in subject_tokens
                        else m.group(1)),
                normalized
            )

        # formatted and plain numbers
        normalized = re.sub(r'\b\d{1,3}(?:[,._]\d{3})+(?:\.\d+)?\b(?!\s*%)', '<<N>>', normalized)
        normalized = re.sub(r'\b\d+(?:\.\d+)?\b(?!\s*%)', '<<N>>', normalized)

        for pattern, replacement in QueryNormalizer.get_chart_patterns().items():
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
            elif word in PatternRegistry.QUESTION_WORDS and not question_words_found:
                question_words_found.append(word)
            elif word not in PatternRegistry.FILLER_WORDS:
                content_words.append(word)


        # Sort only the content words, keep question words at start
        result = ' '.join(content_words).strip()
        for placeholder, expr in expression_map.items():
            result = result.replace(placeholder, expr)

        # Applying semantic normalization BEFORE sorting expressions, but need to be after having the placeholder patterns
        result = SemanticMatcher.normalize_for_matching(result)
        content_words = result.split()

        # Sort only the content words, keep question words at start
        content_words.sort()
        result = ' '.join(content_words).strip()

        if len(result) < 3:
            result = ' '.join(question_words_found + content_words).strip()

        # Validate minimum content
        if not result or len(result) < 3:
            logger.warning(f"Normalization produced too short result for: {query}")
            # Fallback: just lowercase and remove punctuation
            fallback = query.lower()
            fallback = re.sub(r'[?.!,;:\-\(\)\[\]{}\'\"]+', '', fallback)
            return ' '.join(fallback.split())  # normalize whitespace

        logger.debug(f"Normalized '{query}' -> '{result}'")
        return result