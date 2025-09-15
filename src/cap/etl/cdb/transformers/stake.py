import logging
from typing import Any

from cap.etl.cdb.transformers.transformer import BaseTransformer

logger = logging.getLogger(__name__)

class StakePoolTransformer(BaseTransformer):
    """Transformer for stake pool data aligned with Cardano ontology."""

    def transform(self, pools: list[dict[str, Any]]) -> str:
        """Transform stake pools to RDF Turtle format with complete coverage."""
        turtle_lines = []
        turtle_lines_append = turtle_lines.append

        for pool in pools:
            pool_uri = self.create_pool_uri(pool['pool_hash'])

            pool_lines = [f"{pool_uri} a cardano:StakePool ;"]

            if pool['pool_hash']:
                pool_lines.append(f"    blockchain:hasHash \"{pool['pool_hash']}\" ;")

            if pool['pledge']:
                pool_lines.append(f"    cardano:hasPoolPledge {self.create_amount_literal(pool['pledge'])} ;")

            if pool['margin'] is not None:
                pool_lines.append(f"    cardano:hasMargin {self.format_literal(pool['margin'], 'xsd:decimal')} ;")

            if pool['fixed_cost']:
                pool_lines.append(f"    cardano:hasFixedCost {self.create_amount_literal(pool['fixed_cost'])} ;")

            if pool['reward_addr']:
                reward_addr_uri = self.create_stake_address_uri(pool['reward_addr'])
                pool_lines.append(f"    cardano:hasStakeAccount {reward_addr_uri} ;")

            if pool['metadata_url']:
                metadata_uri = self.create_uri('pool_metadata', pool['id'])
                pool_lines.append(f"    cardano:hasPoolMetadata {metadata_uri} ;")

            if pool.get('retirement_epoch'):
                retirement_uri = self.create_uri('pool_retirement', pool['id'])
                pool_lines.append(f"    cardano:hasRetirement {retirement_uri} ;")

            pool_lines[-1] = pool_lines[-1][:-2] + ' .'

            for line in pool_lines:
                turtle_lines_append(line)
            turtle_lines_append("")

            # Add metadata entity
            if pool['metadata_url']:
                metadata_uri = self.create_uri('pool_metadata', pool['id'])
                turtle_lines_append(f"{metadata_uri} a cardano:PoolMetadata .")
                turtle_lines_append("")

            # Add retirement entity
            if pool.get('retirement_epoch'):
                retirement_uri = self.create_uri('pool_retirement', pool['id'])
                turtle_lines_append(f"{retirement_uri} a cardano:PoolRetirement .")
                turtle_lines_append("")

        return '\n'.join(turtle_lines)

class StakeAddressTransformer(BaseTransformer):
    """Transformer for stake address data."""

    def transform(self, stake_addresses: list[dict[str, Any]]) -> str:
        """Transform stake addresses to RDF Turtle format."""
        turtle_lines = []

        for addr in stake_addresses:
            addr_uri = self.create_stake_address_uri(addr['view'])

            turtle_lines.append(f"{addr_uri} a blockchain:Account ;")

            if addr['view']:
                turtle_lines.append(f"    blockchain:hasAccountAddress \"{addr['view']}\" ;")

            if addr['hash_raw']:
                turtle_lines.append(f"    blockchain:hasHash \"{addr['hash_raw']}\" ;")

            if addr.get('stake_amount') and addr['stake_amount'] > 0:
                turtle_lines.append(f"    cardano:hasStakeAmount {self.format_literal(addr['stake_amount'], 'xsd:decimal')} ;")

            # Remove trailing semicolon and add period
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

        return '\n'.join(turtle_lines)

class DelegationTransformer(BaseTransformer):
    """Transformer for delegation data aligned with Cardano ontology."""

    def transform(self, delegations: list[dict[str, Any]]) -> str:
        """Transform delegations with stake amounts to RDF Turtle format."""
        turtle_lines = []

        for delegation in delegations:
            stake_addr_uri = self.create_stake_address_uri(delegation['stake_address'])
            pool_uri = self.create_pool_uri(delegation['pool_hash'])

            # Create delegation relationship
            turtle_lines.append(f"{stake_addr_uri} cardano:delegatesTo {pool_uri} .")

            if delegation.get('stake_amount') and delegation['stake_amount'] > 0:
                # Create a unique token amount for this delegation state
                amount_uri = self.create_uri('token_amount', f"delegation_{delegation['id']}_stake")

                turtle_lines.append(f"{stake_addr_uri} blockchain:hasTokenAmount {amount_uri} .")

                turtle_lines.append(f"{amount_uri} a blockchain:TokenAmount ;")
                turtle_lines.append(f"    blockchain:hasCurrency cardano:ADA ;")
                turtle_lines.append(f"    blockchain:hasAmountValue {self.format_literal(delegation['stake_amount'], 'xsd:decimal')} .")

            turtle_lines.append("")

        return '\n'.join(turtle_lines)

class RewardTransformer(BaseTransformer):
    """Transformer for reward data aligned with Cardano ontology."""

    def transform(self, rewards: list[dict[str, Any]]) -> str:
        """Transform rewards to RDF Turtle format with complete coverage."""
        turtle_lines = []

        for reward in rewards:
            stake_addr_uri = self.create_stake_address_uri(reward['stake_address'])
            reward_uri = self.create_uri('reward', reward['id'])

            # Link stake address to reward
            turtle_lines.append(f"{stake_addr_uri} cardano:hasReward {reward_uri} .")

            # Create reward entity with minimal properties from ontology
            turtle_lines.append(f"{reward_uri} a cardano:Reward ;")

            # Create token amount for the reward
            amount_uri = self.create_uri('token_amount', f"reward_{reward['id']}")
            turtle_lines.append(f"    cardano:hasRewardAmount {amount_uri} ;")

            if reward['type']:
                turtle_lines.append(f"    cardano:hasRewardType \"{reward['type']}\" ;")

            # Remove trailing semicolon and add period
            if turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            # Create token amount entity
            turtle_lines.append(f"")
            turtle_lines.append(f"{amount_uri} a blockchain:TokenAmount ;")
            turtle_lines.append(f"    blockchain:hasCurrency cardano:ADA ;")
            turtle_lines.append(f"    blockchain:hasAmountValue {self.format_literal(reward['amount'], 'xsd:decimal')} .")

            turtle_lines.append("")

        return '\n'.join(turtle_lines)

class WithdrawalTransformer(BaseTransformer):
    """Transformer for withdrawal data aligned with Cardano ontology."""

    def transform(self, withdrawals: list[dict[str, Any]]) -> str:
        """Transform withdrawals to RDF Turtle format with complete coverage."""
        turtle_lines = []

        for withdrawal in withdrawals:
            withdrawal_uri = self.create_uri('withdrawal', withdrawal['id'])

            # Link withdrawal to account
            if withdrawal.get('stake_address'):
                stake_addr_uri = self.create_stake_address_uri(withdrawal['stake_address'])
                turtle_lines.append(f"{stake_addr_uri} cardano:hasWithdrawal {withdrawal_uri} .")

            # Create withdrawal entity
            turtle_lines.append(f"{withdrawal_uri} a cardano:Withdrawal ;")

            # Create token amount for withdrawal
            amount_uri = self.create_uri('token_amount', f"withdrawal_{withdrawal['id']}")
            turtle_lines.append(f"    cardano:hasWithdrawalAmount {amount_uri} ;")

            # Link to transaction
            if withdrawal.get('tx_hash'):
                tx_uri = self.create_transaction_uri(withdrawal['tx_hash'])
                turtle_lines.append(f"    cardano:withdrawnIn {tx_uri} ;")

            # Remove trailing semicolon and add period
            if turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            # Create token amount entity
            turtle_lines.append(f"")
            turtle_lines.append(f"{amount_uri} a blockchain:TokenAmount ;")
            turtle_lines.append(f"    blockchain:hasCurrency cardano:ADA ;")
            turtle_lines.append(f"    blockchain:hasAmountValue {self.format_literal(withdrawal['amount'], 'xsd:decimal')} .")

            turtle_lines.append("")

        return '\n'.join(turtle_lines)