import re
import logging
from opentelemetry import trace
from pathlib import Path
from typing import Tuple

ontology_path: str = "src/ontologies/cardano.ttl"

# Static global for preserved expressions
_PRESERVED_EXPRESSIONS = []

_ENTITIES = []

def _load_ontology_labels(onto_path: str = "src/ontologies/cardano.ttl") -> Tuple[list, list]:
    """Load rdfs:label values from the Turtle ontology file."""
    entity_labels = []
    complex_labels = []
    try:
        path = Path(onto_path)
        if not path.exists():
            logger.warning(f"Ontology file not found at {onto_path}")
            return complex_labels, entity_labels

        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Match rdfs:label patterns in Turtle format
        # Handles both single-line and multi-line string literals
        label_pattern = r'rdfs:label\s+"([^"]+)"'
        matches = re.findall(label_pattern, content)

        for match in matches:
            label_lower = match.lower().strip()
            if label_lower:
                if len(label_lower.split()) > 1:
                    complex_labels.append(label_lower)

                entity_labels.append(label_lower)

        logger.info(f"    Loaded complex labels: \n {complex_labels}\n    Loaded entity labels:\n{entity_labels}\n    From ontology: {onto_path}")

    except Exception as e:
        logger.error(f"Error loading ontology labels from {onto_path}: {e}")

    return complex_labels, entity_labels

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class PatternRegistry:
    """Centralized registry for all patterns and word lists."""

    DEFAULT_PRESERVED_EXPRESSIONS = [
        'asset policy', 'proof of work', 'proof of stake', 'stake pool',
        'native token', 'smart contract', 'ada pots', 'pot transfer',
        'collateral input', 'collateral output', 'reference input', 'fungible token'
        'chain selection rule'
    ]

    DEFAULT_METADATA_PROPERTIES = [
        'hasCertificateMetadata', 'hasDelegateMetadata', 'hasStakePoolMetadata',
        'hasProposalMetadata', 'hasVoteMetadata', 'hasConstitutionMetadata',
        'hasMetadataDecodedCBOR', 'hasMetadataCBOR', 'hasMetadataJSON',
        'hasTxMetadata'
    ]

    # Temporal terms
    YEARLY_TERMS = ['yearly', 'annually', 'per year', 'each year', 'every year']
    MONTHLY_TERMS = ['monthly', 'per month', 'each month', 'every month']
    WEEKLY_TERMS = ['weekly', 'per week', 'each week', 'every week']
    DAILY_TERMS = ['daily', 'per day', 'each day', 'every day']
    EPOCH_PERIOD_TERMS = ['per epoch', 'each epoch', 'every epoch', 'by epoch']
    TEMPORAL_PREPOSITIONS = ['in', 'on', 'at', 'of', 'for', 'during']

    # Month names
    MONTH_NAMES = ['january', 'february', 'march', 'april', 'may', 'june',
                'july', 'august', 'september', 'october', 'november', 'december']
    MONTH_ABBREV = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                    'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    TIME_PERIOD_RANGE_TERMS = ['first', 'last', 'second', 'third']
    TIME_PERIOD_UNITS = ['week', 'day', 'month', 'hour', 'epoch']

    # Ordering terms
    MAX_TERMS = ['largest', 'biggest', 'highest', 'greatest', 'maximum', 'max']
    MIN_TERMS = ['smallest', 'lowest', 'least', 'minimum', 'min']
    TEMPORAL_STATE_TERMS = ['current', 'present', 'now', 'today']
    LAST_TERMS = ['latest', 'most recent', 'newest', 'last',
                'recent', 'recently', 'fresh', 'up to date',
                'updated'] + TEMPORAL_STATE_TERMS
    FIRST_TERMS = ['oldest', 'older', 'past', 'first', 'earliest',
                    'long ago', 'initial', 'beginning', 'original']
    COUNT_TERMS = ['how many', 'number of', 'count', 'amount of',
                    'quantity', 'how much']
    SUM_TERMS = ['sum', 'total', 'add up', 'aggregate', 'combined',
                    'accumulated', 'overall amount']
    AGGREGATE_TIME_TERMS = ['per year', 'per month', 'per day', 'by year', 'by month']
    TOP_TERMS = ['top', 'largest', 'biggest', 'highest', 'most',
                        'best', 'leading', 'upper', 'ascending', 'asc',
                        'top ranked', 'greatest', 'max', 'maximum']
    BOTTOM_TERMS = ['bottom', 'lowest', 'smallest', 'least', 'worst',
                            'lower', 'descending', 'desc', 'bottom ranked',
                            'min', 'minimum']
    ORDINAL_SUFFIXES = ['st', 'nd', 'rd', 'th']


    SEMANTIC_SUGAR = [
        'create', 'created', 'plot', 'draw', 'indeed', 'very', 'too', 'so', 'make', 'compose',
        'visualization', 'cardano', 'count', 'network', 'represent', 'table', 'versus',
        'against', 'pie', 'pizza', 'recorded', 'storage', 'storaged', "with", "all",
        'history', 'ever', 'over time', 'historical', 'progression', 'evolution',
    ]

    # Comparison terms
    ABOVE_TERMS = [
        'above', 'over', 'more than', 'greater than', 'exceeding',
        'beyond', 'higher than', 'greater', '>', 'at least'
    ]
    BELOW_TERMS = [
        'below', 'under', 'less than', 'fewer than', 'lower than',
        'smaller than', '<', 'at most'
    ]
    EQUALS_TERMS =  [
        'equals', 'equal to', 'exactly', 'same as', 'match',
        'matches', 'identical to', '=', 'precisely'
    ]

    BOUND_TERMS = [
        'supply', 'value', 'amount', 'limit'
    ]

    # Entities
    # Entity terms (words only, patterns generated dynamically)
    TRANSACTION_TERMS = ['transaction', 'tx']
    TRANSACTION_DETAIL_TERMS = ['script', 'json', 'metadata', 'datum', 'redeemer']
    POOL_TERMS = ['stake pool', 'pool', 'off chain stake pool data']
    BLOCK_TERMS = ['block']
    EPOCH_TERMS = ['epoch']
    TOKEN_TERMS = ['cnt', 'native token', 'cardano native token', 'token', 'nft', 'fungible token']
    GOVERNANCE_PROPOSAL_TERMS = ['governance', 'proposal', 'action']
    VOTING_TERMS = ['vote', 'voting', 'voting anchor']
    COMMITTEE_TERMS = ['committee']
    DREP_TERMS = ['drep', 'delegate representative']
    DELEGATION_TERMS = ['delegation', 'stake delegation']
    VOTE_TERMS = ['vote']
    CERTIFICATE_TERMS = ['certificate', 'cert']
    CONSTITUTION_TERMS = ['constitution']
    SCRIPT_TERMS = ['script', 'smart contract']
    WITNESS_TERMS = ['witness']
    DATUM_TERMS = ['datum', 'data']
    COST_MODEL_TERMS = ['cost model']
    ADA_POT_TERMS = ['ada pot', 'pot', 'treasury', 'reserves']
    PROTOCOL_PARAM_TERMS = ['protocol parameter', 'protocol params', 'parameters']
    STATUS_TERMS = ['status', 'state', 'health']
    REWARD_TERMS = ['reward', 'withdrawal', 'reward withdrawal']
    INPUT_TERMS = ['input', 'utxo input']
    OUTPUT_TERMS = ['output', 'utxo output']
    ACCOUNT_TERMS = ['account', 'stake account', 'wallet']

    # Chart types
    BAR_CHART_TERMS = [
        'bar', 'bar chart', 'bars', 'histogram', 'column chart'
    ]
    LINE_CHART_TERMS = [
        'line', 'line chart', 'timeseries', 'time serie', 'trend',
        'timeline', 'curve', 'line graph'
    ]
    PIE_CHART_TERMS = [
        'pie', 'pie chart', 'pizza', 'donut', 'doughnut', 'circle chart'
    ]
    TABLE_TERMS = [
        'list', 'table', 'tabular', 'display', 'show', 'get', 'grid',
        'dataset', 'row', 'column'
    ]
    CHART_SUFFIXES = [
        'chart', 'graph', 'plot', 'draw', 'display', 'paint', 'compose', 'trace'
    ]

    DEFINITION_TERMS = [
        'define', 'explain', 'describe', 'tell me about', 'whats', 'what'
    ]

    POSSESSION_TERMS = [
        'hold', 'holds', 'has', 'have', 'own', 'possess', 'possesses',
        'contain', 'contains', 'include', 'includes', 'carrying', 'carry'
    ]

    # Filler words (shared across normalizers)
    FILLER_WORDS = [
        'please', 'could', 'can', 'you', 'me', 'the', 'i', 'be',
        'is', 'are', 'was', 'were', 'your', 'my', 'exist', 'at',
        'a', 'an', 'of', 'in', 'on', 'yours', 'to', 'cardano',
        'do', 'does', 'ever', 'with', 'having', 'from', 'there'
    ]

    QUESTION_WORDS = ['who', 'what', 'when', 'where', 'why', 'which', 'how many', 'how much', 'how long']

    @staticmethod
    def ensure_expressions() -> None:
        global _PRESERVED_EXPRESSIONS
        global _ENTITIES

        if not _PRESERVED_EXPRESSIONS:
            # Load labels from ontology
            complex_labels, entity_labels = _load_ontology_labels(ontology_path)

            # Add default expressions if ontology loading failed or returned nothing
            if not complex_labels:
                logger.warning("No ontology labels loaded, using default preserved expressions")
                complex_labels = PatternRegistry.DEFAULT_PRESERVED_EXPRESSIONS

            _PRESERVED_EXPRESSIONS = complex_labels
            _ENTITIES = entity_labels

    @staticmethod
    def get_preserved_expressions() -> list:
        global _PRESERVED_EXPRESSIONS
        PatternRegistry.ensure_expressions()
        return _PRESERVED_EXPRESSIONS

    @staticmethod
    def get_entities() -> list:
        global _ENTITIES
        PatternRegistry.ensure_expressions()
        return _ENTITIES

    @staticmethod
    def build_pattern(terms: list[str], word_boundary: bool = True) -> str:
        """Build regex pattern from list of terms."""
        escaped = [re.escape(term) for term in terms]
        pattern = '|'.join(escaped)
        if word_boundary:
            return rf'\b({pattern})\b'
        return f'({pattern})'

    @staticmethod
    def build_entity_pattern(base_terms: list[str], plural: bool = True) -> str:
        """Build entity pattern with optional plural."""
        suffix = 's?' if plural else ''
        return PatternRegistry.build_pattern(base_terms) + suffix