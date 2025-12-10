from typing import Any, Optional, Iterator
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, select, exists
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.rdf.cdb_model import StakeAddress, MultiAsset, TxOut, MaTxOut, TxIn, Tx, Block

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
            stmt = select(StakeAddress).order_by(StakeAddress.id)

            if last_processed_id:
                stmt = stmt.filter(StakeAddress.id > last_processed_id)

            total_count = self.get_total_count()
            for offset in range(0, total_count, self.batch_size):
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

                if not batch:
                    break

                # Bulk process accounts
                stake_addr_ids = [sa.id for sa in batch]

                # Get all UTXOs for these addresses in one query
                utxo_data = self._bulk_get_utxo_data(stake_addr_ids)

                # Get first appearances in bulk
                first_appearances = self._bulk_get_first_appearances(stake_addr_ids)
                batch_data = []
                for stake_addr in batch:
                    if stake_addr.view in self.processed_accounts:
                        continue

                    account_data = self._build_account_data(
                        stake_addr,
                        utxo_data.get(stake_addr.id, {}),
                        first_appearances.get(stake_addr.id)
                    )

                    if account_data:
                        batch_data.append(account_data)
                        self.processed_accounts.add(stake_addr.view)

                if batch_data:
                    span.set_attribute("batch_size", len(batch_data))
                    yield batch_data

    def _bulk_get_utxo_data(self, stake_addr_ids: list[int]) -> dict:
        """Get UTXO data for multiple addresses efficiently."""
        if not stake_addr_ids:
            return {}

        # Subquery for spent outputs
        spent_subq = (
            select(TxIn.tx_out_id, TxIn.tx_out_index)
            .subquery()
        )

        # Get unspent UTXOs with their multi-assets using modern select
        stmt = (
            select(
                TxOut.stake_address_id,
                TxOut.value,
                MaTxOut.quantity,
                MultiAsset.fingerprint,
                MultiAsset.policy,
                MultiAsset.name
            )
            .outerjoin(MaTxOut, MaTxOut.tx_out_id == TxOut.id)
            .outerjoin(MultiAsset, MultiAsset.id == MaTxOut.ident)
            .filter(
                TxOut.stake_address_id.in_(stake_addr_ids),
                ~exists(
                    select(1).select_from(spent_subq).where(
                        and_(
                            spent_subq.c.tx_out_id == TxOut.tx_id,
                            spent_subq.c.tx_out_index == TxOut.index
                        )
                    )
                )
            )
        )

        utxos = self.db_session.execute(stmt).all()

        # Organize data by stake address
        result = {}
        for row in utxos:
            addr_id = row[0]
            if addr_id not in result:
                result[addr_id] = {
                    'ada_balance': 0,
                    'utxo_count': 0,
                    'tokens': {}
                }

            result[addr_id]['ada_balance'] += int(row[1] or 0)
            result[addr_id]['utxo_count'] += 1

            # Handle native tokens
            if row[3]:  # fingerprint exists
                token_key = (row[3], row[4].hex() if row[4] else None, row[5].hex() if row[5] else None)
                if token_key not in result[addr_id]['tokens']:
                    result[addr_id]['tokens'][token_key] = {
                        'fingerprint': row[3],
                        'policy': row[4].hex() if row[4] else None,
                        'name': row[5].hex() if row[5] else None,
                        'quantity': 0
                    }
                result[addr_id]['tokens'][token_key]['quantity'] += int(row[2] or 0)

        return result

    def _extract_account_balance(self, stake_addr: StakeAddress) -> Optional[dict[str, Any]]:
        """Extract balance information for a stake address."""
        try:
            # Get all UTXOs for this stake address using modern select
            stmt = select(TxOut).filter(TxOut.stake_address_id == stake_addr.id)
            utxos = self.db_session.execute(stmt).scalars().all()

            # Filter out spent UTXOs
            unspent_utxos = []
            for utxo in utxos:
                # Check if this output has been spent
                spent_stmt = select(TxIn).filter(
                    and_(
                        TxIn.tx_out_id == utxo.tx_id,
                        TxIn.tx_out_index == utxo.index
                    )
                )
                spent = self.db_session.execute(spent_stmt).scalar()

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
        stmt = (
            select(TxOut)
            .filter(TxOut.stake_address_id == stake_addr.id)
            .order_by(TxOut.id)
            .limit(1)
        )
        first_output = self.db_session.execute(stmt).scalar()

        if first_output and first_output.tx and first_output.tx.block:
            return {
                'hash': first_output.tx.hash.hex() if first_output.tx.hash else None,
                'timestamp': first_output.tx.block.time.isoformat() if first_output.tx.block.time else None,
                'block_hash': first_output.tx.block.hash.hex() if first_output.tx.block.hash else None,
                'block_timestamp': first_output.tx.block.time.isoformat() if first_output.tx.block.time else None
            }

        return None

    def get_total_count(self) -> int:
        stmt = select(func.count(StakeAddress.id))
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(StakeAddress.id))
        return self.db_session.execute(stmt).scalar()

    def _bulk_get_first_appearances(self, stake_addr_ids: list[int]) -> dict[int, dict[str, Any]]:
        """Get first transaction appearances for multiple stake addresses efficiently."""
        if not stake_addr_ids:
            return {}

        # Subquery to find the minimum tx_out.id for each stake_address_id
        first_outputs_subq = (
            select(
                TxOut.stake_address_id,
                func.min(TxOut.id).label('min_id')
            )
            .filter(TxOut.stake_address_id.in_(stake_addr_ids))
            .group_by(TxOut.stake_address_id)
            .subquery()
        )

        # Join to get full transaction and block details
        stmt = (
            select(
                TxOut.stake_address_id,
                Tx.hash.label('tx_hash'),
                Block.hash.label('block_hash'),
                Block.time.label('block_time')
            )
            .join(
                first_outputs_subq,
                and_(
                    TxOut.stake_address_id == first_outputs_subq.c.stake_address_id,
                    TxOut.id == first_outputs_subq.c.min_id
                )
            )
            .join(Tx, Tx.id == TxOut.tx_id)
            .join(Block, Block.id == Tx.block_id)
        )

        results = self.db_session.execute(stmt).all()

        # Build result dictionary
        appearances = {}
        for stake_addr_id, tx_hash, block_hash, block_time in results:
            appearances[stake_addr_id] = {
                'hash': tx_hash.hex() if tx_hash else None,
                'timestamp': block_time.isoformat() if block_time else None,
                'block_hash': block_hash.hex() if block_hash else None,
                'block_timestamp': block_time.isoformat() if block_time else None
            }

        return appearances

    def _build_account_data(self, stake_addr: StakeAddress, utxo_data: dict, first_appearance: Optional[dict]) -> Optional[dict]:
        """Build account data from aggregated information."""
        if not utxo_data:
            return None

        return {
            'id': stake_addr.id,
            'stake_address': stake_addr.view,
            'stake_address_hash': stake_addr.hash_raw.hex() if stake_addr.hash_raw else None,
            'ada_balance': utxo_data.get('ada_balance', 0),
            'token_balances': list(utxo_data.get('tokens', {}).values()),
            'utxo_count': utxo_data.get('utxo_count', 0),
            'first_tx_hash': first_appearance.get('hash') if first_appearance else None,
            'first_tx_timestamp': first_appearance.get('timestamp') if first_appearance else None,
            'first_block_hash': first_appearance.get('block_hash') if first_appearance else None,
            'first_block_timestamp': first_appearance.get('block_timestamp') if first_appearance else None
        }