import logging
from typing import Any

from cap.etl.cdb.transformers.transformer import BaseTransformer

logger = logging.getLogger(__name__)

class GovernanceTransformer(BaseTransformer):
    """Transforms governance data to RDF aligned with Cardano ontology."""

    def transform(self, governance_actions: list[dict[str, Any]]) -> str:
        """Transform governance actions to RDF Turtle format."""
        turtle_lines = []

        for action in governance_actions:
            action_uri = self.create_uri('governance_action', action['id'])

            # Governance Action entity
            turtle_lines.append(f"{action_uri} a cardano:GovernanceAction ;")

            if action.get('id'):
                turtle_lines.append(f"    cardano:hasActionId \"{action['id']}\" ;")

            if action['type']:
                turtle_lines.append(f"    cardano:hasActionType \"{action['type']}\" ;")

            # Better proposal linking
            proposal_uri = None
            if action.get('tx_hash'):  # Use tx_hash instead of proposal_tx_hash
                proposal_uri = self.create_uri('governance_proposal', f"{action['tx_hash']}_proposal")
                turtle_lines.append(f"    cardano:executesGovernanceProposal {proposal_uri} ;")

                # Create the proposal if it doesn't exist
                turtle_lines.append(f"")
                turtle_lines.append(f"{proposal_uri} a cardano:GovernanceProposal ;")
                turtle_lines.append(f"    cardano:hasProposalId \"{action['tx_hash']}_proposal\" ;")
                turtle_lines.append(f"    cardano:hasProposalStatus \"active\" .")
                turtle_lines.append(f"")

            # Process voting procedures
            vote_count = 0
            for i, vote_proc in enumerate(action.get('voting_procedures', [])):
                vote_uri = self.create_uri('vote', f"{action['id']}_vote_{i}")
                turtle_lines.append(f"    cardano:hasVote {vote_uri} ;")
                vote_count += 1

                # Create vote entity
                turtle_lines.append(f"")
                turtle_lines.append(f"{vote_uri} a cardano:Vote ;")

                if vote_proc['vote']:
                    # Map vote values to ontology values
                    vote_value = vote_proc['vote'].lower()
                    if vote_value in ['yes', 'no', 'abstain']:
                        turtle_lines.append(f"    cardano:hasVotingResult \"{vote_value}\" ;")
                    else:
                        turtle_lines.append(f"    cardano:hasVotingResult \"abstain\" ;")

                # Bidirectional link between proposal and vote
                if proposal_uri:
                    turtle_lines.append(f"    cardano:hasVotingResult \"{vote_proc.get('vote', 'abstain').lower()}\" .")
                    turtle_lines.append(f"{proposal_uri} cardano:hasVote {vote_uri} .")

                # Create account-vote relationship with correct direction
                if vote_proc.get('voter_hash'):
                    # Determine voter type and create appropriate account
                    voter_role = vote_proc.get('voter_role', '')

                    if voter_role == 'SPO' and vote_proc.get('pool_hash'):
                        # For SPO votes, link to stake pool
                        pool_uri = self.create_pool_uri(vote_proc['pool_hash'])
                        turtle_lines.append(f"{pool_uri} cardano:castsVote {vote_uri} .")
                    elif voter_role == 'DRep':
                        # For DRep votes, create DRep account
                        drep_uri = self.create_uri('drep', vote_proc['voter_hash'])
                        turtle_lines.append(f"{drep_uri} a cardano:DRep ;")
                        turtle_lines.append(f"    blockchain:hasHash \"{vote_proc['voter_hash']}\" ;")
                        turtle_lines.append(f"    cardano:castsVote {vote_uri} .")
                    else:
                        # For other votes, create generic account
                        voter_account_uri = self.create_uri('account', vote_proc['voter_hash'])
                        turtle_lines.append(f"{voter_account_uri} a blockchain:Account ;")
                        turtle_lines.append(f"    blockchain:hasHash \"{vote_proc['voter_hash']}\" ;")
                        turtle_lines.append(f"    cardano:castsVote {vote_uri} .")

            # Add vote count to proposal for easier querying
            if proposal_uri and vote_count > 0:
                turtle_lines.append(f"")
                turtle_lines.append(f"# Total votes for this proposal: {vote_count}")

            # Remove trailing semicolon and add period
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

        return '\n'.join(turtle_lines)

class DRepTransformer(BaseTransformer):
    """Transforms DRep data to RDF aligned with Cardano ontology."""

    def transform(self, drep_registrations: list[dict[str, Any]]) -> str:
        """Transform DRep registrations to RDF Turtle format."""
        turtle_lines = []

        for drep_reg in drep_registrations:
            drep_uri = self.create_uri('drep', drep_reg['drep_hash'])
            registration_uri = self.create_uri('drep_registration', drep_reg['id'])

            # DRep entity
            turtle_lines.append(f"{drep_uri} a cardano:DRep ;")
            turtle_lines.append(f"    blockchain:hasHash \"{drep_reg['drep_hash']}\" .")

            # DRep Registration entity
            turtle_lines.append(f"")
            turtle_lines.append(f"{registration_uri} a cardano:DRepRegistration .")

            # Voting anchor if present
            if drep_reg['voting_anchor_url']:
                anchor_uri = self.create_uri('voting_anchor', drep_reg['voting_anchor_id'])
                turtle_lines.append(f"")
                turtle_lines.append(f"{anchor_uri} a cardano:VotingAnchor .")

            turtle_lines.append("")

        return '\n'.join(turtle_lines)

class TreasuryTransformer(BaseTransformer):
    """Transforms treasury and reserve data to RDF aligned with Cardano ontology."""

    def transform(self, treasury_data: list[dict[str, Any]]) -> str:
        """Transform treasury and reserve movements to RDF Turtle format."""
        turtle_lines = []

        for item in treasury_data:
            item_type = item['type']

            if item_type == 'treasury':
                turtle_lines.extend(self._transform_treasury(item))
            elif item_type == 'reserve':
                turtle_lines.extend(self._transform_reserve(item))
            elif item_type == 'pot_transfer':
                turtle_lines.extend(self._transform_pot_transfer(item))

        return '\n'.join(turtle_lines)

    def _transform_treasury(self, treasury: dict[str, Any]) -> list[str]:
        """Transform treasury movement to RDF."""
        lines = []
        treasury_uri = self.create_uri('treasury_movement', treasury['id'])

        lines.append(f"{treasury_uri} a cardano:TreasuryMovement ;")
        lines.append(f"    cardano:hasTreasuryAmount {self.format_literal(treasury['amount'], 'xsd:integer')} ;")
        lines.append(f"    cardano:hasCertIndex {self.format_literal(treasury['cert_index'], 'xsd:integer')} ;")

        if treasury['stake_address']:
            stake_addr_uri = self.create_stake_address_uri(treasury['stake_address'])
            lines.append(f"    cardano:belongsToAccount {stake_addr_uri} ;")

        if treasury['tx_hash']:
            tx_uri = self.create_transaction_uri(treasury['tx_hash'])
            lines.append(f"    cardano:hasTreasuryTransaction {tx_uri} ;")

        # Remove trailing semicolon and add period
        if lines and lines[-1].endswith(' ;'):
            lines[-1] = lines[-1][:-2] + ' .'

        lines.append("")
        return lines

    def _transform_reserve(self, reserve: dict[str, Any]) -> list[str]:
        """Transform reserve movement to RDF."""
        lines = []
        reserve_uri = self.create_uri('reserve_movement', reserve['id'])

        lines.append(f"{reserve_uri} a cardano:ReserveMovement ;")
        lines.append(f"    cardano:hasReserveAmount {self.format_literal(reserve['amount'], 'xsd:integer')} ;")
        lines.append(f"    cardano:hasCertIndex {self.format_literal(reserve['cert_index'], 'xsd:integer')} ;")

        if reserve['stake_address']:
            stake_addr_uri = self.create_stake_address_uri(reserve['stake_address'])
            lines.append(f"    cardano:belongsToAccount {stake_addr_uri} ;")

        if reserve['tx_hash']:
            tx_uri = self.create_transaction_uri(reserve['tx_hash'])
            lines.append(f"    cardano:hasReserveTransaction {tx_uri} ;")

        # Remove trailing semicolon and add period
        if lines and lines[-1].endswith(' ;'):
            lines[-1] = lines[-1][:-2] + ' .'

        lines.append("")
        return lines

    def _transform_pot_transfer(self, pot_transfer: dict[str, Any]) -> list[str]:
        """Transform pot transfer to RDF."""
        lines = []
        transfer_uri = self.create_uri('pot_transfer', pot_transfer['id'])

        lines.append(f"{transfer_uri} a cardano:PotTransfer ;")
        lines.append(f"    cardano:hasTreasuryTransfer {self.format_literal(pot_transfer['treasury'], 'xsd:integer')} ;")
        lines.append(f"    cardano:hasReserveTransfer {self.format_literal(pot_transfer['reserves'], 'xsd:integer')} ;")
        lines.append(f"    cardano:hasCertIndex {self.format_literal(pot_transfer['cert_index'], 'xsd:integer')} ;")

        if pot_transfer['tx_hash']:
            tx_uri = self.create_transaction_uri(pot_transfer['tx_hash'])
            lines.append(f"    cardano:hasTransferTransaction {tx_uri} ;")

        # Remove trailing semicolon and add period
        if lines and lines[-1].endswith(' ;'):
            lines[-1] = lines[-1][:-2] + ' .'

        lines.append("")
        return lines