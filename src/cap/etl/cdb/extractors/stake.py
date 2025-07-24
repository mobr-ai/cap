from typing import Any, Optional, Iterator
from sqlalchemy.orm import joinedload
from sqlalchemy import func, and_
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.data.cdb_model import (
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
            query = self.db_session.query(PoolUpdate).options(
                joinedload(PoolUpdate.hash),
                joinedload(PoolUpdate.meta),
                joinedload(PoolUpdate.registered_tx)
            )

            if last_processed_id:
                query = query.filter(PoolUpdate.id > last_processed_id)

            query = query.order_by(PoolUpdate.id)

            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_pool_update(pool_update) for pool_update in batch]
                offset += self.batch_size

    def _serialize_pool_update(self, pool_update: PoolUpdate) -> dict[str, Any]:
        """Serialize pool update to dictionary."""
        # Check for retirement
        retirement = self.db_session.query(PoolRetire).filter(
            PoolRetire.hash_id == pool_update.hash_id
        ).first()

        return {
            'id': pool_update.id,
            'pool_hash_id': pool_update.hash_id,
            'pool_hash': pool_update.hash.view if pool_update.hash else None,
            'pool_hash_raw': pool_update.hash.hash_raw.hex() if pool_update.hash and pool_update.hash.hash_raw else None,
            'cert_index': pool_update.cert_index,
            'vrf_key_hash': pool_update.vrf_key_hash.hex() if pool_update.vrf_key_hash else None,
            'pledge': str(pool_update.pledge),
            'reward_addr': pool_update.reward_addr,
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
        return self.db_session.query(func.count(PoolUpdate.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(PoolUpdate.id)).scalar()
        return result

class DelegationExtractor(BaseExtractor):
    """Extracts delegation data from cardano-db-sync."""
    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract delegations with stake amounts in batches."""
        with tracer.start_as_current_span("delegation_extraction") as span:
            # Get current epoch to determine active delegations
            current_epoch = self.db_session.query(func.max(Epoch.no)).scalar()

            query = self.db_session.query(Delegation).options(
                joinedload(Delegation.addr),
                joinedload(Delegation.pool_hash),
                joinedload(Delegation.tx)
            ).filter(
                Delegation.active_epoch_no <= current_epoch
            )

            if last_processed_id:
                query = query.filter(Delegation.id > last_processed_id)

            query = query.order_by(Delegation.id)

            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_delegation_with_stake(delegation) for delegation in batch]
                offset += self.batch_size

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
        utxos = self.db_session.query(TxOut).filter(
            TxOut.stake_address_id == stake_addr_id
        ).all()

        # Filter out spent UTXOs and sum the values
        total_stake = 0
        for utxo in utxos:
            # Check if this output has been spent
            from cap.data.cdb_model import TxIn
            spent = self.db_session.query(TxIn).filter(
                and_(
                    TxIn.tx_out_id == utxo.tx_id,
                    TxIn.tx_out_index == utxo.index
                )
            ).first()

            if not spent:
                total_stake += int(utxo.value)

        return total_stake

    def get_total_count(self) -> int:
        current_epoch = self.db_session.query(func.max(Epoch.no)).scalar()
        return self.db_session.query(func.count(Delegation.id)).filter(
            Delegation.active_epoch_no <= current_epoch
        ).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(Delegation.id)).scalar()
        return result

class RewardExtractor(BaseExtractor):
    """Extracts reward data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract rewards in batches."""
        with tracer.start_as_current_span("reward_extraction") as span:
            query = self.db_session.query(Reward).options(
                joinedload(Reward.addr),
                joinedload(Reward.earned_epoch_ref),
                joinedload(Reward.spendable_epoch_ref),
                joinedload(Reward.pool)
            )

            if last_processed_id:
                query = query.filter(Reward.id > last_processed_id)

            query = query.order_by(Reward.id)

            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_reward(reward) for reward in batch]
                offset += self.batch_size

    def _serialize_reward(self, reward: Reward) -> dict[str, Any]:
        """Serialize a reward to dictionary."""
        return {
            'id': reward.id,
            'addr_id': reward.addr_id,
            'stake_address': reward.addr.view if reward.addr else None,
            'stake_address_hash': reward.addr.hash_raw.hex() if reward.addr and reward.addr.hash_raw else None,
            'type': reward.type,
            'amount': str(reward.amount),
            'earned_epoch': reward.earned_epoch,
            'spendable_epoch': reward.spendable_epoch,
            'pool_id': reward.pool_id,
            'pool_hash': reward.pool.view if reward.pool else None
        }

    def get_total_count(self) -> int:
        return self.db_session.query(func.count(Reward.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(Reward.id)).scalar()
        return result

class StakeAddressExtractor(BaseExtractor):
    """Extracts stake address data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract stake addresses in batches."""
        with tracer.start_as_current_span("stake_address_extraction") as span:
            query = self.db_session.query(StakeAddress).options(
                joinedload(StakeAddress.registered_tx)
            )

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

                yield [self._serialize_stake_address(stake_addr) for stake_addr in batch]
                offset += self.batch_size

    def _serialize_stake_address(self, stake_addr: StakeAddress) -> dict[str, Any]:
        """Serialize stake address to dictionary."""
        stake_amount = self._calculate_stake_amount(stake_addr.id)

        return {
            'id': stake_addr.id,
            'hash_raw': stake_addr.hash_raw.hex() if stake_addr.hash_raw else None,
            'view': stake_addr.view,
            'script_hash': stake_addr.script_hash.hex() if stake_addr.script_hash else None,
            'registered_tx_id': stake_addr.registered_tx_id,
            'registered_tx_hash': stake_addr.registered_tx.hash.hex() if stake_addr.registered_tx and stake_addr.registered_tx.hash else None,
            'has_script': bool(stake_addr.script_hash),
            'stake_amount': stake_amount
        }

    def _calculate_stake_amount(self, stake_addr_id: int) -> int:
        """Calculate the total stake amount for a stake address."""
        # Get all UTXOs for this stake address
        utxos = self.db_session.query(TxOut).filter(
            TxOut.stake_address_id == stake_addr_id
        ).all()

        # Filter out spent UTXOs and sum the values
        total_stake = 0
        for utxo in utxos:
            # Check if this output has been spent
            spent = self.db_session.query(TxIn).filter(
                and_(
                    TxIn.tx_out_id == utxo.tx_id,
                    TxIn.tx_out_index == utxo.index
                )
            ).first()

            if not spent:
                total_stake += int(utxo.value)

        return total_stake

    def get_total_count(self) -> int:
        return self.db_session.query(func.count(StakeAddress.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(StakeAddress.id)).scalar()
        return result

class WithdrawalExtractor(BaseExtractor):
    """Extracts withdrawal data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract withdrawals in batches."""
        with tracer.start_as_current_span("withdrawal_extraction") as span:
            query = self.db_session.query(Withdrawal).options(
                joinedload(Withdrawal.addr),
                joinedload(Withdrawal.tx)
            )

            if last_processed_id:
                query = query.filter(Withdrawal.id > last_processed_id)

            query = query.order_by(Withdrawal.id)

            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
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
        return self.db_session.query(func.count(Withdrawal.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(Withdrawal.id)).scalar()
        return result