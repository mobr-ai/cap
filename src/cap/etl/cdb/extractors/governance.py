from typing import Any, Optional, Iterator
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.data.cdb_model import (
    GovernanceAction, VotingProcedure, DrepRegistration, DrepHash,
    VotingAnchor, Treasury, Reserve, PotTransfer, Tx, PoolHash
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class GovernanceExtractor(BaseExtractor):
    """Extracts governance data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract governance actions in batches."""
        with tracer.start_as_current_span("governance_extraction") as span:
            query = self.db_session.query(GovernanceAction).options(
                joinedload(GovernanceAction.tx)
            )

            if last_processed_id:
                query = query.filter(GovernanceAction.id > last_processed_id)

            query = query.order_by(GovernanceAction.id)

            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                # Also get related voting procedures
                batch_data = []
                for action in batch:
                    action_data = self._serialize_governance_action(action)

                    # Get voting procedures for this action
                    voting_procedures = self.db_session.query(VotingProcedure).filter(
                        VotingProcedure.gov_action_proposal_id == action.id
                    ).all()

                    action_data['voting_procedures'] = [
                        self._serialize_voting_procedure(vp) for vp in voting_procedures
                    ]

                    batch_data.append(action_data)

                yield batch_data
                offset += self.batch_size

    def _serialize_governance_action(self, action: GovernanceAction) -> dict[str, Any]:
        """Serialize a governance action to dictionary."""
        return {
            'id': action.id,
            'tx_id': action.tx_id,
            'tx_hash': action.tx.hash.hex() if action.tx and action.tx.hash else None,
            'index': action.index,
            'type': action.type,
            'description': action.description,
            'deposit': str(action.deposit),
            'return_address': action.return_address
        }

    def _serialize_voting_procedure(self, procedure: VotingProcedure) -> dict[str, Any]:
        """Serialize a voting procedure to dictionary."""
        # Get tx separately if not loaded
        tx = None
        if procedure.tx_id:
            tx = self.db_session.query(Tx).filter(Tx.id == procedure.tx_id).first()

        # Get pool hash if this is an SPO vote
        pool_hash = None
        if procedure.voter_role == 'SPO' and procedure.voter_hash:
            # Try to find the pool hash that matches this voter hash
            pool = self.db_session.query(PoolHash).filter(
                PoolHash.hash_raw == procedure.voter_hash
            ).first()
            if pool:
                pool_hash = pool.view

        return {
            'id': procedure.id,
            'tx_id': procedure.tx_id,
            'tx_hash': tx.hash.hex() if tx and tx.hash else None,
            'index': procedure.index,
            'gov_action_proposal_id': procedure.gov_action_proposal_id,
            'voter_role': procedure.voter_role,
            'voter_hash': procedure.voter_hash.hex() if procedure.voter_hash else None,
            'vote': procedure.vote,
            'pool_hash': pool_hash  # Add pool hash for SPO votes
        }

    def get_total_count(self) -> int:
        return self.db_session.query(func.count(GovernanceAction.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(GovernanceAction.id)).scalar()
        return result

class DRepExtractor(BaseExtractor):
    """Extracts DRep (Delegated Representative) data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract DRep registrations in batches."""
        with tracer.start_as_current_span("drep_extraction") as span:
            query = self.db_session.query(DrepRegistration).options(
                joinedload(DrepRegistration.tx),
                joinedload(DrepRegistration.drep_hash),
                joinedload(DrepRegistration.voting_anchor)
            )

            if last_processed_id:
                query = query.filter(DrepRegistration.id > last_processed_id)

            query = query.order_by(DrepRegistration.id)

            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_drep_registration(drep_reg) for drep_reg in batch]
                offset += self.batch_size

    def _serialize_drep_registration(self, drep_reg: DrepRegistration) -> dict[str, Any]:
        """Serialize a DRep registration to dictionary."""
        return {
            'id': drep_reg.id,
            'tx_id': drep_reg.tx_id,
            'tx_hash': drep_reg.tx.hash.hex() if drep_reg.tx and drep_reg.tx.hash else None,
            'cert_index': drep_reg.cert_index,
            'drep_hash_id': drep_reg.drep_hash_id,
            'drep_hash': drep_reg.drep_hash.view if drep_reg.drep_hash else None,
            'drep_hash_raw': drep_reg.drep_hash.raw.hex() if drep_reg.drep_hash and drep_reg.drep_hash.raw else None,
            'has_script': drep_reg.drep_hash.has_script if drep_reg.drep_hash else False,
            'deposit': str(drep_reg.deposit) if drep_reg.deposit else None,
            'voting_anchor_id': drep_reg.voting_anchor_id,
            'voting_anchor_url': drep_reg.voting_anchor.url if drep_reg.voting_anchor else None,
            'voting_anchor_hash': drep_reg.voting_anchor.data_hash.hex() if drep_reg.voting_anchor and drep_reg.voting_anchor.data_hash else None
        }

    def get_total_count(self) -> int:
        return self.db_session.query(func.count(DrepRegistration.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(DrepRegistration.id)).scalar()
        return result

class TreasuryExtractor(BaseExtractor):
    """Extracts treasury and reserve data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract treasury and reserve movements in batches."""
        with tracer.start_as_current_span("treasury_extraction") as span:
            # Process each type separately to handle different last_processed_ids

            offset = 0
            while True:
                combined_batch = []

                # Get treasury data
                treasury_query = self.db_session.query(Treasury).options(
                    joinedload(Treasury.addr),
                    joinedload(Treasury.tx)
                )

                if last_processed_id:
                    treasury_query = treasury_query.filter(Treasury.id > last_processed_id)

                treasury_batch = (treasury_query.order_by(Treasury.id)
                                .offset(offset)
                                .limit(self.batch_size // 3)
                                .all())

                # Get reserve data
                reserve_query = self.db_session.query(Reserve).options(
                    joinedload(Reserve.addr),
                    joinedload(Reserve.tx)
                )

                if last_processed_id:
                    reserve_query = reserve_query.filter(Reserve.id > last_processed_id)

                reserve_batch = (reserve_query.order_by(Reserve.id)
                               .offset(offset)
                               .limit(self.batch_size // 3)
                               .all())

                # Get pot transfers
                pot_transfer_query = self.db_session.query(PotTransfer).options(
                    joinedload(PotTransfer.tx)
                )

                if last_processed_id:
                    pot_transfer_query = pot_transfer_query.filter(PotTransfer.id > last_processed_id)

                pot_transfer_batch = (pot_transfer_query.order_by(PotTransfer.id)
                                    .offset(offset)
                                    .limit(self.batch_size // 3)
                                    .all())

                if not treasury_batch and not reserve_batch and not pot_transfer_batch:
                    break

                # Add treasury data
                for treasury in treasury_batch:
                    combined_batch.append(self._serialize_treasury(treasury))

                # Add reserve data
                for reserve in reserve_batch:
                    combined_batch.append(self._serialize_reserve(reserve))

                # Add pot transfer data
                for pot_transfer in pot_transfer_batch:
                    combined_batch.append(self._serialize_pot_transfer(pot_transfer))

                span.set_attribute("batch_size", len(combined_batch))
                span.set_attribute("offset", offset)

                if combined_batch:
                    yield combined_batch

                offset += self.batch_size // 3

    def _serialize_treasury(self, treasury: Treasury) -> dict[str, Any]:
        """Serialize treasury data to dictionary."""
        return {
            'id': treasury.id,
            'type': 'treasury',
            'addr_id': treasury.addr_id,
            'stake_address': treasury.addr.view if treasury.addr else None,
            'cert_index': treasury.cert_index,
            'amount': str(treasury.amount),
            'tx_id': treasury.tx_id,
            'tx_hash': treasury.tx.hash.hex() if treasury.tx and treasury.tx.hash else None
        }

    def _serialize_reserve(self, reserve: Reserve) -> dict[str, Any]:
        """Serialize reserve data to dictionary."""
        return {
            'id': reserve.id,
            'type': 'reserve',
            'addr_id': reserve.addr_id,
            'stake_address': reserve.addr.view if reserve.addr else None,
            'cert_index': reserve.cert_index,
            'amount': str(reserve.amount),
            'tx_id': reserve.tx_id,
            'tx_hash': reserve.tx.hash.hex() if reserve.tx and reserve.tx.hash else None
        }

    def _serialize_pot_transfer(self, pot_transfer: PotTransfer) -> dict[str, Any]:
        """Serialize pot transfer data to dictionary."""
        return {
            'id': pot_transfer.id,
            'type': 'pot_transfer',
            'cert_index': pot_transfer.cert_index,
            'treasury': str(pot_transfer.treasury),
            'reserves': str(pot_transfer.reserves),
            'tx_id': pot_transfer.tx_id,
            'tx_hash': pot_transfer.tx.hash.hex() if pot_transfer.tx and pot_transfer.tx.hash else None
        }

    def get_total_count(self) -> int:
        treasury_count = self.db_session.query(func.count(Treasury.id)).scalar() or 0
        reserve_count = self.db_session.query(func.count(Reserve.id)).scalar() or 0
        pot_transfer_count = self.db_session.query(func.count(PotTransfer.id)).scalar() or 0
        return treasury_count + reserve_count + pot_transfer_count

    def get_last_id(self) -> Optional[int]:
        treasury_max = self.db_session.query(func.max(Treasury.id)).scalar() or 0
        reserve_max = self.db_session.query(func.max(Reserve.id)).scalar() or 0
        pot_transfer_max = self.db_session.query(func.max(PotTransfer.id)).scalar() or 0
        return max(treasury_max, reserve_max, pot_transfer_max) if any([treasury_max, reserve_max, pot_transfer_max]) else None