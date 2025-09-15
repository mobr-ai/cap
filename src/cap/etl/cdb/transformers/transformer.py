"""
Base transformer for cardano-db-sync ETL pipeline.
"""

from abc import ABC, abstractmethod
from typing import Any
from urllib.parse import quote
import logging
from functools import lru_cache

from cap.config import settings

logger = logging.getLogger(__name__)

class BaseTransformer(ABC):
    """Base class for all data transformers with ontology alignment."""

    def __init__(self):
        self.base_uri = settings.CARDANO_GRAPH

    @lru_cache(maxsize=10000)
    def create_uri(self, entity_type: str, identifier: Any) -> str:
        """Create a URI for an entity with proper encoding."""
        if identifier is None:
            raise ValueError(f"Cannot create URI for {entity_type} with None identifier")

        # Handle different identifier types
        if isinstance(identifier, (int, float)):
            safe_id = str(identifier)
        else:
            # URL encode the identifier to handle special characters
            safe_id = quote(str(identifier), safe='')

        return f"<{self.base_uri}/{entity_type}/{safe_id}>"

    def create_hash_literal(self, hash_value: str) -> str:
        """Create a properly formatted hash literal."""
        if not hash_value:
            return '""'
        return f'"{hash_value}"'

    def create_amount_literal(self, amount: Any) -> str:
        """Create a properly formatted amount literal with ADA token amount."""
        if amount is None:
            return '"0"^^xsd:decimal'

        # Convert to decimal and ensure proper formatting
        try:
            decimal_amount = str(amount)
            return f'"{decimal_amount}"^^xsd:decimal'
        except (ValueError, TypeError):
            logger.warning(f"Invalid amount value: {amount}, defaulting to 0")
            return '"0"^^xsd:decimal'

    def format_literal(self, value: Any, datatype: str = None) -> str:
        """Format a literal value with optional datatype."""
        if value is None:
            return '""'

        # Escape quotes and special characters in string values
        if isinstance(value, str):
            # More robust escaping needed
            escaped_value = value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
            if datatype:
                return f'"{escaped_value}"^^{datatype}'
            else:
                return f'"{escaped_value}"'

        # Handle other types
        if datatype:
            return f'"{value}"^^{datatype}'
        else:
            return f'"{value}"'

    # Ontology-aligned URI creation methods
    def create_epoch_uri(self, epoch_no: int) -> str:
        """Create URI for epoch using ontology pattern."""
        return self.create_uri('epoch', epoch_no)

    def create_stake_address_uri(self, stake_address: str) -> str:
        """Create URI for stake address using ontology pattern."""
        return self.create_uri('stake_address', stake_address)

    def create_pool_uri(self, pool_hash: str) -> str:
        """Create URI for stake pool using ontology pattern."""
        return self.create_uri('stake_pool', pool_hash)

    def create_transaction_uri(self, tx_hash: str) -> str:
        """Create URI for transaction using ontology pattern."""
        return self.create_uri('transaction', tx_hash)

    def create_block_uri(self, block_hash: str) -> str:
        """Create URI for block using ontology pattern."""
        return self.create_uri('block', block_hash)

    def add_common_block_properties(self, block_uri: str, block: dict[str, Any]) -> list[str]:
        """Add common block properties aligned with ontology."""
        lines = []

        if block.get('slot_no') is not None:
            lines.append(f"    cardano:hasSlotNumber {self.format_literal(block['slot_no'], 'xsd:decimal')} ;")

        if block.get('epoch_no') is not None:
            epoch_uri = self.create_epoch_uri(block['epoch_no'])
            lines.append(f"    cardano:belongsToEpoch {epoch_uri} ;")

        if block.get('time'):
            lines.append(f"    blockchain:hasTimestamp {self.format_literal(block['time'], 'xsd:dateTime')} ;")

        return lines

    @abstractmethod
    def transform(self, data: list[dict[str, Any]]) -> str:
        """Transform data to RDF Turtle format."""
        pass