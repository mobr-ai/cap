from sqlalchemy.orm import Session
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.etl.cdb.extractors.account import AccountExtractor
from cap.etl.cdb.extractors.block import BlockExtractor
from cap.etl.cdb.extractors.transaction import TransactionExtractor
from cap.etl.cdb.extractors.stake import StakePoolExtractor, StakeAddressExtractor, DelegationExtractor, RewardExtractor, WithdrawalExtractor
from cap.etl.cdb.extractors.multi_asset import MultiAssetExtractor
from cap.etl.cdb.extractors.epoch import EpochExtractor
from cap.etl.cdb.extractors.script import ScriptExtractor
from cap.etl.cdb.extractors.governance import GovernanceExtractor, DRepExtractor, TreasuryExtractor
from cap.etl.cdb.extractors.datum import DatumExtractor

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class ExtractorFactory:
    """Factory class to create extractors for all Cardano entities."""

    @staticmethod
    def create_extractor(extractor_type: str, db_session: Session, batch_size: int = 1000) -> BaseExtractor:
        """Create an extractor instance."""
        extractors = {
            # Core blockchain entities
            'account': AccountExtractor,
            'epoch': EpochExtractor,
            'block': BlockExtractor,
            'transaction': TransactionExtractor,

            # Asset entities
            'multi_asset': MultiAssetExtractor,
            'script': ScriptExtractor,
            'datum': DatumExtractor,

            # Staking entities
            'stake_address': StakeAddressExtractor,
            'stake_pool': StakePoolExtractor,
            'delegation': DelegationExtractor,
            'reward': RewardExtractor,
            'withdrawal': WithdrawalExtractor,

            # Governance entities
            'governance_action': GovernanceExtractor,
            'drep_registration': DRepExtractor,
            'treasury': TreasuryExtractor,
            'voting_procedure': GovernanceExtractor,
            'drep_update': DRepExtractor,

            # Additional entities
            'pool_registration': StakePoolExtractor,
            'pool_retirement': StakePoolExtractor,
            'instantaneous_reward': RewardExtractor,
            'reserve': TreasuryExtractor,
            'pot_transfer': TreasuryExtractor,
            'committee_registration': GovernanceExtractor,
            'committee_deregistration': GovernanceExtractor,
            'asset_mint': MultiAssetExtractor,
            'certificate': TransactionExtractor,
            'voting_anchor': GovernanceExtractor,
            'pool_metadata': StakePoolExtractor,
            'protocol_parameters': EpochExtractor,
            'epoch_parameters': EpochExtractor
        }

        if extractor_type not in extractors:
            raise ValueError(f"Unknown extractor type: {extractor_type}")

        return extractors[extractor_type](db_session, batch_size)