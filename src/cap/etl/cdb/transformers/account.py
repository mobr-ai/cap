import logging
from typing import Any

from cap.etl.cdb.transformers.transformer import BaseTransformer

logger = logging.getLogger(__name__)

class AccountTransformer(BaseTransformer):
    """Transforms account balance data to RDF aligned with Cardano ontology."""

    def transform(self, accounts: list[dict[str, Any]]) -> str:
        """Transform account balances to RDF Turtle format."""
        turtle_lines = []
        turtle_lines_append = turtle_lines.append

        for account in accounts:
            account_uri = self.create_stake_address_uri(account['stake_address'])

            acc_lines = [
                f"{account_uri} a blockchain:Account ;",
                f"    blockchain:hasAccountAddress \"{account['stake_address']}\" ;"
            ]

            if account.get('stake_address_hash'):
                acc_lines.append(f"    blockchain:hasHash \"{account['stake_address_hash']}\" ;")

            if account.get('first_tx_hash'):
                tx_uri = self.create_transaction_uri(account['first_tx_hash'])
                acc_lines.append(f"    blockchain:firstAppearedInTransaction {tx_uri} ;")

            acc_lines[-1] = acc_lines[-1][:-2] + ' .'

            for line in acc_lines:
                turtle_lines_append(line)

            # Add block-transaction relationship
            if account.get('first_block_hash') and account.get('first_block_timestamp'):
                block_uri = self.create_block_uri(account['first_block_hash'])
                tx_uri = self.create_transaction_uri(account['first_tx_hash'])

                turtle_lines_append("")
                turtle_lines_append(f"{block_uri} a blockchain:Block ;")
                turtle_lines_append(f"    blockchain:hasTransaction {tx_uri} ;")
                turtle_lines_append(f"    blockchain:hasTimestamp {self.format_literal(account['first_block_timestamp'], 'xsd:dateTime')} .")

            # Add ADA balance
            if account['ada_balance'] > 0:
                ada_amount_uri = self.create_uri('token_amount', f"ada_balance_{account['id']}")
                turtle_lines_append("")
                turtle_lines_append(f"{account_uri} blockchain:hasTokenAmount {ada_amount_uri} .")

                turtle_lines_append(f"{ada_amount_uri} a blockchain:TokenAmount ;")
                turtle_lines_append(f"    blockchain:hasCurrency cardano:ADA ;")
                turtle_lines_append(f"    blockchain:hasAmountValue {self.format_literal(account['ada_balance'], 'xsd:decimal')} .")

            # Add native token balances
            for i, token in enumerate(account['token_balances']):
                token_uri = self.create_uri('native_token', token['fingerprint'])
                amount_uri = self.create_uri('token_amount', f"{account['id']}_token_{i}")

                turtle_lines_append(f"{account_uri} blockchain:hasTokenAmount {amount_uri} .")

                turtle_lines_append(f"{amount_uri} a blockchain:TokenAmount ;")
                turtle_lines_append(f"    blockchain:hasCurrency {token_uri} ;")
                turtle_lines_append(f"    blockchain:hasAmountValue {self.format_literal(token['quantity'], 'xsd:decimal')} .")

            turtle_lines_append("")

        return '\n'.join(turtle_lines)