from typing import Any, Optional, Iterator
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.data.cdb_model import StakeAddress, TxOut, MaTxOut, TxIn, Tx, Block

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class AccountExtractor(BaseExtractor):
    """Extracts account balance data from cardano-db-sync."""

    def __init__(self, db_session: Session, batch_size: int = 1000):
        super().__init__(db_session, batch_size)
        # Track processed accounts to avoid duplicates
        self.processed_accounts = set()

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract account balances in batches."""
        with tracer.start_as_current_span("account_balance_extraction") as span:
            # Get stake addresses with their UTXOs
            query = self.db_session.query(StakeAddress)

            if last_processed_id:
                query = query.filter(StakeAddress.id > last_processed_id)

            query = query.order_by(StakeAddress.id)

            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                # Process each stake address
                batch_data = []
                for stake_addr in batch:
                    if stake_addr.view in self.processed_accounts:
                        continue

                    account_data = self._extract_account_balance(stake_addr)
                    if account_data:
                        batch_data.append(account_data)
                        self.processed_accounts.add(stake_addr.view)

                if batch_data:
                    yield batch_data

                offset += self.batch_size

    def _extract_account_balance(self, stake_addr: StakeAddress) -> Optional[dict[str, Any]]:
        """Extract balance information for a stake address."""
        try:
            # Get all UTXOs for this stake address
            utxos = self.db_session.query(TxOut).filter(
                TxOut.stake_address_id == stake_addr.id
            ).all()

            # Filter out spent UTXOs
            unspent_utxos = []
            for utxo in utxos:
                # Check if this output has been spent
                spent = self.db_session.query(TxIn).filter(
                    and_(
                        TxIn.tx_out_id == utxo.tx_id,
                        TxIn.tx_out_index == utxo.index
                    )
                ).first()

                if not spent:
                    unspent_utxos.append(utxo)

            if not unspent_utxos:
                return None

            # Calculate ADA balance
            ada_balance = sum(int(utxo.value) for utxo in unspent_utxos)

            # Calculate native token balances
            token_balances = {}
            for utxo in unspent_utxos:
                for ma_out in utxo.multi_assets:
                    token_key = (
                        ma_out.multi_asset.fingerprint,
                        ma_out.multi_asset.policy.hex() if ma_out.multi_asset.policy else None,
                        ma_out.multi_asset.name.hex() if ma_out.multi_asset.name else None
                    )
                    if token_key not in token_balances:
                        token_balances[token_key] = {
                            'fingerprint': ma_out.multi_asset.fingerprint,
                            'policy': ma_out.multi_asset.policy.hex() if ma_out.multi_asset.policy else None,
                            'name': ma_out.multi_asset.name.hex() if ma_out.multi_asset.name else None,
                            'quantity': 0
                        }
                    token_balances[token_key]['quantity'] += int(ma_out.quantity)

            # Get first appearance (account creation)
            first_tx = self._get_first_appearance(stake_addr)

            return {
                'id': stake_addr.id,
                'stake_address': stake_addr.view,
                'stake_address_hash': stake_addr.hash_raw.hex() if stake_addr.hash_raw else None,
                'ada_balance': ada_balance,
                'token_balances': list(token_balances.values()),
                'utxo_count': len(unspent_utxos),
                'first_tx_hash': first_tx['hash'] if first_tx else None,
                'first_tx_timestamp': first_tx['timestamp'] if first_tx else None,
                'first_block_hash': first_tx['block_hash'] if first_tx else None,  # Added for block linking
                'first_block_timestamp': first_tx['block_timestamp'] if first_tx else None  # Added for queries
            }

        except Exception as e:
            logger.error(f"Error extracting balance for stake address {stake_addr.id}: {e}")
            return None

    def _get_first_appearance(self, stake_addr: StakeAddress) -> Optional[dict[str, Any]]:
        """Get the first transaction where this stake address appeared."""
        first_output = self.db_session.query(TxOut).filter(
            TxOut.stake_address_id == stake_addr.id
        ).order_by(TxOut.id).first()

        if first_output and first_output.tx and first_output.tx.block:
            return {
                'hash': first_output.tx.hash.hex() if first_output.tx.hash else None,
                'timestamp': first_output.tx.block.time.isoformat() if first_output.tx.block.time else None,
                'block_hash': first_output.tx.block.hash.hex() if first_output.tx.block.hash else None,
                'block_timestamp': first_output.tx.block.time.isoformat() if first_output.tx.block.time else None
            }

        return None

    def get_total_count(self) -> int:
        return self.db_session.query(func.count(StakeAddress.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(StakeAddress.id)).scalar()
        return result