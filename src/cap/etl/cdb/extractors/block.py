from typing import Any, Optional, Iterator
from sqlalchemy.orm import subqueryload
from sqlalchemy import func
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.data.cdb_model import Block, SlotLeader, Tx

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class BlockExtractor(BaseExtractor):
    """Extracts block data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract blocks in batches."""
        with tracer.start_as_current_span("block_extraction") as span:
            if last_processed_id:
                query = self.db_session.query(Block).options(
                    subqueryload(Block.slot_leader).subqueryload(SlotLeader.pool_hash)
                ).order_by(Block.id).filter(Block.id > last_processed_id)
            else:
                self.db_session.query(Block).options(
                    subqueryload(Block.slot_leader).subqueryload(SlotLeader.pool_hash)
                ).order_by(Block.id)

            # Pre-fetch transaction hashes in bulk
            total_count = self.get_total_count()
            for offset in range(0, total_count or 0, self.batch_size):
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break

                # Bulk fetch transactions for all blocks in batch
                block_ids = [block.id for block in batch]
                tx_map = {}
                if block_ids:
                    txs = self.db_session.query(Tx.block_id, Tx.hash).filter(
                        Tx.block_id.in_(block_ids)
                    ).all()
                    for block_id, tx_hash in txs:
                        if block_id not in tx_map:
                            tx_map[block_id] = []
                        tx_map[block_id].append({'hash': tx_hash.hex(), 'epoch_no': None})

                # Serialize with pre-fetched data
                batch_data = []
                for block in batch:
                    serialized = self._serialize_block(block)
                    serialized['transactions'] = tx_map.get(block.id, [])
                    for tx in serialized['transactions']:
                        tx['epoch_no'] = block.epoch_no
                    batch_data.append(serialized)

                span.set_attribute("batch_size", len(batch))
                yield batch_data

    def _serialize_block(self, block: Block) -> dict[str, Any]:
        """Serialize a block to dictionary."""

        transactions = self.db_session.query(Tx).filter(
            Tx.block_id == block.id
        ).all()

        return {
            'id': block.id,
            'hash': block.hash.hex() if block.hash else None,
            'epoch_no': block.epoch_no,
            'slot_no': block.slot_no,
            'epoch_slot_no': block.epoch_slot_no,
            'block_no': block.block_no,
            'previous_id': block.previous_id,
            'slot_leader_id': block.slot_leader_id,
            'slot_leader_hash': block.slot_leader.hash.hex() if block.slot_leader and block.slot_leader.hash else None,
            'pool_hash': block.slot_leader.pool_hash.view if block.slot_leader and block.slot_leader.pool_hash else None,
            'size': block.size,
            'time': block.time.isoformat() if block.time else None,
            'tx_count': block.tx_count,
            'proto_major': block.proto_major,
            'proto_minor': block.proto_minor,
            'vrf_key': block.vrf_key,
            'op_cert_counter': block.op_cert_counter,
            'transactions': [{'hash': tx.hash.hex(), 'epoch_no': block.epoch_no} for tx in transactions]
        }

    def get_total_count(self) -> int:
        return self.db_session.query(func.count(Block.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(Block.id)).scalar()
        return result