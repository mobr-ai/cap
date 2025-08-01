from typing import Any, Optional, Iterator
from sqlalchemy.orm import selectinload
from sqlalchemy import func, select
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.data.cdb_model import (
    GovernanceAction, VotingProcedure, DrepRegistration,
    Treasury, Reserve, PotTransfer, Tx
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class GovernanceExtractor(BaseExtractor):
    """Extracts governance data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract governance actions in batches."""
        with tracer.start_as_current_span("governance_extraction") as span:
            stmt = (
                select(GovernanceAction)
                .options(selectinload(GovernanceAction.tx))
                .order_by(GovernanceAction.id)
            )

            if last_processed_id:
                stmt = stmt.filter(GovernanceAction.id > last_processed_id)

            offset = 0
            while True:
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                # Also get related voting procedures
                batch_data = []
                for action in batch:
                    action_data = self._serialize_governance_action(action)

                    # Get voting procedures for this action
                    vp_stmt = select(VotingProcedure).filter(
                        VotingProcedure.gov_action_proposal_id == action.id
                    )
                    voting_procedures = self.db_session.execute(vp_stmt).scalars().all()

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
            stmt = select(Tx).filter(Tx.id == procedure.tx_id)
            tx = self.db_session.execute(stmt).scalar()

        return {
            'id': procedure.id,
            'tx_id': procedure.tx_id,
            'tx_hash': tx.hash.hex() if tx and tx.hash else None,
            'index': procedure.index,
            'gov_action_proposal_id': procedure.gov_action_proposal_id,
            'voter_role': procedure.voter_role,
            'vote': procedure.vote
        }

    def get_total_count(self) -> int:
        stmt = select(func.count(GovernanceAction.id))
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(GovernanceAction.id))
        return self.db_session.execute(stmt).scalar()

class DRepExtractor(BaseExtractor):
    """Extracts DRep (Delegated Representative) data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract DRep registrations in batches."""
        with tracer.start_as_current_span("drep_extraction") as span:
            stmt = (
                select(DrepRegistration)
                .options(
                    selectinload(DrepRegistration.tx),
                    selectinload(DrepRegistration.drep_hash),
                    selectinload(DrepRegistration.voting_anchor)
                )
                .order_by(DrepRegistration.id)
            )

            if last_processed_id:
                stmt = stmt.filter(DrepRegistration.id > last_processed_id)

            offset = 0
            while True:
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

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
        stmt = select(func.count(DrepRegistration.id))
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(DrepRegistration.id))
        return self.db_session.execute(stmt).scalar()

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
                treasury_stmt = (
                    select(Treasury)
                    .options(
                        selectinload(Treasury.addr),
                        selectinload(Treasury.tx)
                    )
                    .order_by(Treasury.id)
                )

                if last_processed_id:
                    treasury_stmt = treasury_stmt.filter(Treasury.id > last_processed_id)

                treasury_batch = self.db_session.execute(
                    treasury_stmt.offset(offset).limit(self.batch_size // 3)
                ).scalars().all()

                # Get reserve data
                reserve_stmt = (
                    select(Reserve)
                    .options(
                        selectinload(Reserve.addr),
                        selectinload(Reserve.tx)
                    )
                    .order_by(Reserve.id)
                )

                if last_processed_id:
                    reserve_stmt = reserve_stmt.filter(Reserve.id > last_processed_id)

                reserve_batch = self.db_session.execute(
                    reserve_stmt.offset(offset).limit(self.batch_size // 3)
                ).scalars().all()

                # Get pot transfers
                pot_transfer_stmt = (
                    select(PotTransfer)
                    .options(selectinload(PotTransfer.tx))
                    .order_by(PotTransfer.id)
                )

                if last_processed_id:
                    pot_transfer_stmt = pot_transfer_stmt.filter(PotTransfer.id > last_processed_id)

                pot_transfer_batch = self.db_session.execute(
                    pot_transfer_stmt.offset(offset).limit(self.batch_size // 3)
                ).scalars().all()

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
        treasury_count = self.db_session.execute(select(func.count(Treasury.id))).scalar() or 0
        reserve_count = self.db_session.execute(select(func.count(Reserve.id))).scalar() or 0
        pot_transfer_count = self.db_session.execute(select(func.count(PotTransfer.id))).scalar() or 0
        return treasury_count + reserve_count + pot_transfer_count

    def get_last_id(self) -> Optional[int]:
        treasury_max = self.db_session.execute(select(func.max(Treasury.id))).scalar() or 0
        reserve_max = self.db_session.execute(select(func.max(Reserve.id))).scalar() or 0
        pot_transfer_max = self.db_session.execute(select(func.max(PotTransfer.id))).scalar() or 0
        return max(treasury_max, reserve_max, pot_transfer_max) if any([treasury_max, reserve_max, pot_transfer_max]) else None
