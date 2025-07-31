import logging
from typing import Any

from cap.etl.cdb.transformers.transformer import BaseTransformer

logger = logging.getLogger(__name__)

class BlockTransformer(BaseTransformer):
    """Transformer for block data aligned with Cardano ontology."""

    def transform(self, blocks: list[dict[str, Any]]) -> str:
        """Transform blocks to RDF Turtle format with complete ontology coverage."""
        turtle_lines = []

        for block in blocks:
            block_uri = self.create_block_uri(block['hash'])

            turtle_lines.append(f"{block_uri} a blockchain:Block ;")

            if block['hash']:
                turtle_lines.append(f"    blockchain:hasHash {self.create_hash_literal(block['hash'])} ;")

            if block['time']:
                turtle_lines.append(f"    blockchain:hasTimestamp {self.format_literal(block['time'], 'xsd:dateTime')} ;")

            if block['slot_no'] is not None:
                turtle_lines.append(f"    cardano:hasSlotNumber {self.format_literal(block['slot_no'], 'xsd:decimal')} ;")

            if block['epoch_no'] is not None:
                epoch_uri = self.create_epoch_uri(block['epoch_no'])
                turtle_lines.append(f"    cardano:belongsToEpoch {epoch_uri} ;")

            if block['epoch_slot_no'] is not None:
                turtle_lines.append(f"    cardano:hasEpochSlot {self.format_literal(block['epoch_slot_no'], 'xsd:decimal')} ;")

            if block['block_no'] is not None:
                turtle_lines.append(f"    cardano:hasBlockNumber {self.format_literal(block['block_no'], 'xsd:decimal')} ;")

            if block['size']:
                turtle_lines.append(f"    cardano:hasBlockSize {self.format_literal(block['size'], 'xsd:decimal')} ;")

            if block['tx_count']:
                turtle_lines.append(f"    cardano:hasBlockTransactionCount {self.format_literal(block['tx_count'], 'xsd:decimal')} ;")

            # Add all transactions
            for tx in block.get('transactions', []):
                tx_uri = self.create_transaction_uri(tx['hash'])
                turtle_lines.append(f"    blockchain:hasTransaction {tx_uri} ;")

            if block['previous_id']:
                # Create previous block reference
                prev_block_uri = self.create_uri('block', f"id_{block['previous_id']}")
                turtle_lines.append(f"    blockchain:hasPreviousBlock {prev_block_uri} ;")

            # Protocol version information
            if block.get('proto_major') is not None:
                turtle_lines.append(f"    cardano:hasProtocolMajorVersion {self.format_literal(block['proto_major'], 'xsd:decimal')} ;")

            if block.get('proto_minor') is not None:
                turtle_lines.append(f"    cardano:hasProtocolMinorVersion {self.format_literal(block['proto_minor'], 'xsd:decimal')} ;")

            # Slot leader information
            if block['slot_leader_hash']:
                slot_leader_uri = self.create_uri('slot_leader', block['slot_leader_hash'])
                turtle_lines.append(f"    cardano:hasSlotLeader {slot_leader_uri} ;")

            # Remove trailing semicolon and add period
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            # Add blank line before slot leader entity
            turtle_lines.append("")

            # Create slot leader entity if it exists
            if block['slot_leader_hash']:
                slot_leader_uri = self.create_uri('slot_leader', block['slot_leader_hash'])
                turtle_lines.append(f"{slot_leader_uri} a cardano:SlotLeader ;")
                turtle_lines.append(f"    blockchain:hasHash {self.create_hash_literal(block['slot_leader_hash'])} ;")

                if block['pool_hash']:
                    pool_uri = self.create_pool_uri(block['pool_hash'])
                    turtle_lines.append(f"    cardano:hasStakeAccount {pool_uri} ;")

                # Remove trailing semicolon from slot leader
                if turtle_lines[-1].endswith(' ;'):
                    turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

        return '\n'.join(turtle_lines)