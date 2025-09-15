import logging

from cap.etl.cdb.transformers.transformer import BaseTransformer
from cap.etl.cdb.transformers.account import AccountTransformer
from cap.etl.cdb.transformers.block import BlockTransformer
from cap.etl.cdb.transformers.transaction import TransactionTransformer
from cap.etl.cdb.transformers.stake import StakePoolTransformer, StakeAddressTransformer, DelegationTransformer, RewardTransformer, WithdrawalTransformer
from cap.etl.cdb.transformers.multi_asset import MultiAssetTransformer
from cap.etl.cdb.transformers.epoch import EpochTransformer
from cap.etl.cdb.transformers.script import ScriptTransformer
from cap.etl.cdb.transformers.governance import GovernanceTransformer, DRepTransformer, TreasuryTransformer
from cap.etl.cdb.transformers.datum import DatumTransformer

logger = logging.getLogger(__name__)

class TransformerFactory:
    """Factory class to create transformers for all Cardano entities."""

    @staticmethod
    def create_transformer(transformer_type: str) -> BaseTransformer:
        """Create a transformer instance."""
        transformers = {
            # Core blockchain entities
            'account': AccountTransformer,
            'epoch': EpochTransformer,
            'block': BlockTransformer,
            'transaction': TransactionTransformer,

            # Asset entities
            'multi_asset': MultiAssetTransformer,
            'script': ScriptTransformer,
            'datum': DatumTransformer,

            # Staking entities
            'stake_address': StakeAddressTransformer,
            'stake_pool': StakePoolTransformer,
            'delegation': DelegationTransformer,
            'reward': RewardTransformer,
            'withdrawal': WithdrawalTransformer,

            # Governance entities
            'governance_action': GovernanceTransformer,
            'drep_registration': DRepTransformer,
            'treasury': TreasuryTransformer,
            'voting_procedure': GovernanceTransformer,  # Same transformer handles both
            'drep_update': DRepTransformer,  # Same transformer handles updates

            # Additional entities using existing transformers
            'pool_registration': StakePoolTransformer,
            'pool_retirement': StakePoolTransformer,
            'instantaneous_reward': RewardTransformer,
            'reserve': TreasuryTransformer,
            'pot_transfer': TreasuryTransformer,
            'committee_registration': GovernanceTransformer,
            'committee_deregistration': GovernanceTransformer,
            'asset_mint': MultiAssetTransformer,
            'certificate': TransactionTransformer,  # Certificates are part of transactions
            'voting_anchor': GovernanceTransformer,
            'pool_metadata': StakePoolTransformer,
            'protocol_parameters': EpochTransformer,
            'epoch_parameters': EpochTransformer
        }

        if transformer_type not in transformers:
            raise ValueError(f"Unknown transformer type: {transformer_type}")

        return transformers[transformer_type]()