from typing import Any, Optional, Iterator
from sqlalchemy.orm import selectinload, lazyload
from sqlalchemy import func, and_, exists, select, case
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.rdf.cdb_model import (
    PoolUpdate, PoolRetire, Reward, Withdrawal, StakeAddress,
    Delegation, TxIn, TxOut, Epoch
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class StakePoolExtractor(BaseExtractor):
    """Extracts stake pool data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract stake pools in batches."""
        with tracer.start_as_current_span("stake_pool_extraction") as span:
            # Get pool updates with metadata
            stmt = (
                select(PoolUpdate)
                .options(
                    selectinload(PoolUpdate.hash),
                    selectinload(PoolUpdate.meta),
                    selectinload(PoolUpdate.registered_tx)
                )
                .order_by(PoolUpdate.id)
            )

            if last_processed_id:
                stmt = stmt.filter(PoolUpdate.id > last_processed_id)

            offset = 0
            while True:
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_pool_update(pool_update) for pool_update in batch]
                offset += self.batch_size

    def _serialize_pool_update(self, pool_update: PoolUpdate) -> dict[str, Any]:
        """Serialize pool update to dictionary."""
        # Check for retirement
        retire_stmt = select(PoolRetire).filter(PoolRetire.hash_id == pool_update.hash_id)
        retirement = self.db_session.execute(retire_stmt).scalar()

        return {
            'id': pool_update.id,
            'pool_hash_id': pool_update.hash_id,
            'pool_hash': pool_update.hash.view if pool_update.hash else None,
            'pool_hash_raw': pool_update.hash.hash_raw.hex() if pool_update.hash and pool_update.hash.hash_raw else None,
            'cert_index': pool_update.cert_index,
            'vrf_key_hash': pool_update.vrf_key_hash.hex() if pool_update.vrf_key_hash else None,
            'pledge': str(pool_update.pledge),
            'reward_addr': pool_update.reward_addr.view if pool_update.reward_addr else None,  # Access through relationship
            'active_epoch_no': pool_update.active_epoch_no,
            'margin': float(pool_update.margin),
            'fixed_cost': str(pool_update.fixed_cost),
            'registered_tx_id': pool_update.registered_tx_id,
            'registered_tx_hash': pool_update.registered_tx.hash.hex() if pool_update.registered_tx and pool_update.registered_tx.hash else None,
            'metadata_url': pool_update.meta.url if pool_update.meta else None,
            'metadata_hash': pool_update.meta.hash.hex() if pool_update.meta and pool_update.meta.hash else None,
            'retirement_epoch': retirement.retiring_epoch if retirement else None,
            'retirement_tx_id': retirement.announced_tx_id if retirement else None
        }

    def get_total_count(self) -> int:
        stmt = select(func.count(PoolUpdate.id))
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(PoolUpdate.id))
        return self.db_session.execute(stmt).scalar()

class DelegationExtractor(BaseExtractor):
    """Extracts delegation data from cardano-db-sync."""
    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract delegations with stake amounts in batches."""
        with tracer.start_as_current_span("delegation_extraction") as span:
            current_epoch = self.db_session.execute(select(func.max(Epoch.no))).scalar()

            stmt = (
                select(Delegation)
                .options(
                    lazyload(Delegation.addr),
                    lazyload(Delegation.pool_hash),
                    lazyload(Delegation.tx)
                )
                .filter(Delegation.active_epoch_no <= current_epoch)
                .order_by(Delegation.id)
            )

            if last_processed_id:
                stmt = stmt.filter(Delegation.id > last_processed_id)

            # Bulk calculate stake amounts
            for offset in range(0, self.get_total_count(), self.batch_size):
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

                if not batch:
                    break

                # Get all stake amounts in one query
                addr_ids = [d.addr_id for d in batch]
                stake_amounts = self._bulk_calculate_stake_amounts(addr_ids)

                batch_data = []
                for delegation in batch:
                    serialized = self._serialize_delegation_with_stake(delegation)
                    serialized['stake_amount'] = stake_amounts.get(delegation.addr_id, 0)
                    batch_data.append(serialized)

                span.set_attribute("batch_size", len(batch))
                yield batch_data

    def _bulk_calculate_stake_amounts(self, addr_ids: list[int]) -> dict[int, int]:
        """Calculate stake amounts for multiple addresses in one query."""
        if not addr_ids:
            return {}

        # Subquery for spent outputs
        spent_outputs = (
            select(TxIn.tx_out_id, TxIn.tx_out_index)
            .subquery()
        )

        # Get all unspent outputs for the addresses
        stmt = (
            select(
                TxOut.stake_address_id,
                func.sum(TxOut.value).label('total_stake')
            )
            .filter(
                TxOut.stake_address_id.in_(addr_ids),
                ~exists(
                    select(1).select_from(spent_outputs).where(
                        and_(
                            spent_outputs.c.tx_out_id == TxOut.tx_id,
                            spent_outputs.c.tx_out_index == TxOut.index
                        )
                    )
                )
            )
            .group_by(TxOut.stake_address_id)
        )

        results = self.db_session.execute(stmt).all()
        return {addr_id: int(stake or 0) for addr_id, stake in results}

    def _serialize_delegation_with_stake(self, delegation: Delegation) -> dict[str, Any]:
        """Serialize delegation with calculated stake amount."""
        # Get stake amount for this delegation
        stake_amount = self._calculate_stake_amount(delegation.addr_id)

        return {
            'id': delegation.id,
            'addr_id': delegation.addr_id,
            'stake_address': delegation.addr.view if delegation.addr else None,
            'stake_address_hash': delegation.addr.hash_raw.hex() if delegation.addr and delegation.addr.hash_raw else None,
            'cert_index': delegation.cert_index,
            'pool_hash_id': delegation.pool_hash_id,
            'pool_hash': delegation.pool_hash.view if delegation.pool_hash else None,
            'active_epoch_no': delegation.active_epoch_no,
            'tx_id': delegation.tx_id,
            'tx_hash': delegation.tx.hash.hex() if delegation.tx and delegation.tx.hash else None,
            'slot_no': delegation.slot_no,
            'redeemer_id': delegation.redeemer_id,
            'stake_amount': stake_amount
        }

    def _calculate_stake_amount(self, stake_addr_id: int) -> int:
        """Calculate the total stake amount for a stake address."""
        # Get all UTXOs for this stake address
        stmt = select(TxOut).filter(TxOut.stake_address_id == stake_addr_id)
        utxos = self.db_session.execute(stmt).scalars().all()

        # Filter out spent UTXOs and sum the values
        total_stake = 0
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
                total_stake += int(utxo.value)

        return total_stake

    def get_total_count(self) -> int:
        current_epoch = self.db_session.execute(select(func.max(Epoch.no))).scalar()
        stmt = select(func.count(Delegation.id)).filter(Delegation.active_epoch_no <= current_epoch)
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(Delegation.id))
        return self.db_session.execute(stmt).scalar()

class RewardExtractor(BaseExtractor):
    """Extracts reward data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract rewards in batches."""
        with tracer.start_as_current_span("reward_extraction") as span:
            stmt = (
                select(Reward)
                .options(
                    selectinload(Reward.addr),
                    selectinload(Reward.pool)
                )
                .order_by(Reward.addr_id, Reward.type, Reward.earned_epoch)
            )

            # Since reward table doesn't have an id column, we need to use a different approach
            # We'll order by addr_id, type, and earned_epoch (the composite key)
            if last_processed_id:
                # last_processed_id will be a tuple of (addr_id, type, earned_epoch)
                if isinstance(last_processed_id, dict):
                    stmt = stmt.filter(
                        (Reward.addr_id > last_processed_id['addr_id']) |
                        ((Reward.addr_id == last_processed_id['addr_id']) &
                         (Reward.type > last_processed_id['type'])) |
                        ((Reward.addr_id == last_processed_id['addr_id']) &
                         (Reward.type == last_processed_id['type']) &
                         (Reward.earned_epoch > last_processed_id['earned_epoch']))
                    )

            offset = 0
            while True:
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_reward(reward) for reward in batch]
                offset += self.batch_size

    def _serialize_reward(self, reward: Reward) -> dict[str, Any]:
        """Serialize a reward to dictionary."""
        return {
            # Create composite id for tracking
            'id': f"{reward.addr_id}_{reward.type}_{reward.earned_epoch}",
            'addr_id': reward.addr_id,
            'stake_address': reward.addr.view if reward.addr else None,
            'stake_address_hash': reward.addr.hash_raw.hex() if reward.addr and reward.addr.hash_raw else None,
            'type': reward.type,
            'amount': str(reward.amount),
            'earned_epoch': reward.earned_epoch,
            'spendable_epoch': reward.spendable_epoch,
            'pool_id': reward.pool_id,
            'pool_hash': reward.pool.view if reward.pool else None,
            # Store composite key components for progress tracking
            '_composite_key': {
                'addr_id': reward.addr_id,
                'type': reward.type,
                'earned_epoch': reward.earned_epoch
            }
        }

    def get_total_count(self) -> int:
        stmt = select(func.count()).select_from(Reward)
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        return None

class StakeAddressExtractor(BaseExtractor):
    """Extracts stake address data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract stake addresses in batches."""
        with tracer.start_as_current_span("stake_address_extraction") as span:
            stmt = select(StakeAddress).order_by(StakeAddress.id)

            if last_processed_id:
                stmt = stmt.filter(StakeAddress.id > last_processed_id)

            total_count = self.db_session.execute(select(func.count(StakeAddress.id))).scalar() or 0
            for offset in range(0, total_count, self.batch_size):
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

                if not batch:
                    break

                batch_data = []
                for stake_addr in batch:
                    serialized = {
                        'id': stake_addr.id,
                        'hash_raw': stake_addr.hash_raw.hex() if stake_addr.hash_raw else None,
                        'view': stake_addr.view,
                        'script_hash': stake_addr.script_hash.hex() if stake_addr.script_hash else None,
                        'has_script': bool(stake_addr.script_hash),
                        'stake_amount': self._calculate_stake_amount(stake_addr.id)
                    }
                    batch_data.append(serialized)

                span.set_attribute("batch_size", len(batch))
                yield batch_data

    def _calculate_stake_amount(self, stake_addr_id: int) -> int:
        """Calculate the total stake amount for a stake address."""
        # Subquery for spent outputs
        spent_outputs = (
            select(TxIn.tx_out_id, TxIn.tx_out_index)
            .subquery()
        )

        # Calculate sum directly in database
        stmt = (
            select(func.coalesce(func.sum(TxOut.value), 0))
            .filter(
                TxOut.stake_address_id == stake_addr_id,
                ~exists(
                    select(1).select_from(spent_outputs).where(
                        and_(
                            spent_outputs.c.tx_out_id == TxOut.tx_id,
                            spent_outputs.c.tx_out_index == TxOut.index
                        )
                    )
                )
            )
        )

        result = self.db_session.execute(stmt).scalar()
        return int(result)

    def get_total_count(self) -> int:
        stmt = select(func.count(StakeAddress.id))
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(StakeAddress.id))
        return self.db_session.execute(stmt).scalar()

class WithdrawalExtractor(BaseExtractor):
    """Extracts withdrawal data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract withdrawals in batches."""
        with tracer.start_as_current_span("withdrawal_extraction") as span:
            stmt = (
                select(Withdrawal)
                .options(
                    selectinload(Withdrawal.addr),
                    selectinload(Withdrawal.tx)
                )
                .order_by(Withdrawal.id)
            )

            if last_processed_id:
                stmt = stmt.filter(Withdrawal.id > last_processed_id)

            offset = 0
            while True:
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_withdrawal(withdrawal) for withdrawal in batch]
                offset += self.batch_size

    def _serialize_withdrawal(self, withdrawal: Withdrawal) -> dict[str, Any]:
        """Serialize a withdrawal to dictionary."""
        return {
            'id': withdrawal.id,
            'addr_id': withdrawal.addr_id,
            'stake_address': withdrawal.addr.view if withdrawal.addr else None,
            'tx_id': withdrawal.tx_id,
            'tx_hash': withdrawal.tx.hash.hex() if withdrawal.tx and withdrawal.tx.hash else None,
            'amount': str(withdrawal.amount)
        }

    def get_total_count(self) -> int:
        stmt = select(func.count(Withdrawal.id))
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(Withdrawal.id))
        return self.db_session.execute(stmt).scalar()