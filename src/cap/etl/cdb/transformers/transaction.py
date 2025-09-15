import logging
from typing import Any

from cap.etl.cdb.transformers.transformer import BaseTransformer

logger = logging.getLogger(__name__)

class TransactionTransformer(BaseTransformer):
    """Transformer for transaction data aligned with Cardano ontology."""

    def _is_nft(self, minted_asset: dict[str, Any], tx_metadata: list[dict[str, Any]]) -> bool:
        """
        Determine if a minted asset is an NFT based on quantity and metadata.
        Checks for CIP-25 NFT metadata standard.
        """
        # First check: quantity must be 1
        if int(minted_asset['quantity']) != 1:
            return False

        # Second check: look for CIP-25 metadata (label 721)
        for metadata in tx_metadata:
            if metadata['key'] == '721':
                # Check if this policy/asset is in the metadata
                if metadata.get('json'):
                    try:
                        import json
                        meta_obj = json.loads(metadata['json']) if isinstance(metadata['json'], str) else metadata['json']
                        # CIP-25 structure: {policy_id: {asset_name: {...}}}
                        if isinstance(meta_obj, dict) and minted_asset['policy'] in str(meta_obj):
                            return True
                    except:
                        pass

        # Third check: common NFT patterns in asset name
        asset_name = minted_asset.get('name_utf8', '').lower()
        nft_indicators = ['nft', 'token', 'collectible', 'art', 'punk', 'ape']
        if any(indicator in asset_name for indicator in nft_indicators):
            return True

        return False

    def transform(self, transactions: list[dict[str, Any]]) -> str:
        """Transform transactions to RDF Turtle format with complete data coverage."""
        turtle_lines = []

        # Track blocks that need transaction links
        block_tx_links = []

        for tx in transactions:
            tx_uri = self.create_transaction_uri(tx['hash'])

            # Transaction as blockchain:Transaction
            turtle_lines.append(f"{tx_uri} a blockchain:Transaction ;")

            if tx['hash']:
                turtle_lines.append(f"    cardano:hasTransactionID \"{tx['hash']}\" ;")
                turtle_lines.append(f"    blockchain:hasHash \"{tx['hash']}\" ;")

            if tx['fee']:
                turtle_lines.append(f"    cardano:hasFee {self.format_literal(tx['fee'], 'xsd:decimal')} ;")

            # Link to block and ensure timestamp propagation
            if tx.get('block_hash'):
                block_uri = self.create_block_uri(tx['block_hash'])
                turtle_lines.append(f"    cardano:hasAnchor {block_uri} ;")

                # Track this for reverse relationship
                block_tx_links.append((block_uri, tx_uri, tx.get('block_timestamp')))

            # Add protocol era if available
            if tx.get('block_epoch_no') is not None:
                epoch_uri = self.create_epoch_uri(tx['block_epoch_no'])
                turtle_lines.append(f"    cardano:belongsToEra {epoch_uri} ;")

            # Process minted assets with NFT detection
            for minted in tx.get('minted_assets', []):
                if minted['fingerprint']:
                    asset_uri = self.create_uri('native_token', minted['fingerprint'])
                    turtle_lines.append(f"    cardano:hasMintedAsset {asset_uri} ;")

            # Process inputs
            for i, tx_input in enumerate(tx['inputs']):
                input_uri = self.create_uri('tx_input', f"{tx['hash']}_input_{i}")
                turtle_lines.append(f"    cardano:hasInput {input_uri} ;")

            # Process outputs
            for output in tx['outputs']:
                output_uri = self.create_uri('tx_output', f"{tx['hash']}_output_{output['index']}")
                turtle_lines.append(f"    cardano:hasOutput {output_uri} ;")

            # Process metadata
            for metadata in tx['metadata']:
                meta_uri = self.create_uri('tx_metadata', f"{tx['hash']}_meta_{metadata['key']}")
                turtle_lines.append(f"    cardano:hasTransactionMetadata {meta_uri} ;")

            # Remove trailing semicolon and add period to close the transaction
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

            # Now process the related entities that were referenced above

            # Process minted assets (create the entities)
            for minted in tx.get('minted_assets', []):
                if minted['fingerprint']:
                    asset_uri = self.create_uri('native_token', minted['fingerprint'])

                    # Check if it's an NFT
                    if self._is_nft(minted, tx.get('metadata', [])):
                        # Create NFT instance
                        turtle_lines.append(f"{asset_uri} a blockchain:NFT ;")
                        if minted.get('name_utf8'):
                            turtle_lines.append(f"    blockchain:hasTokenName \"{minted['name_utf8']}\" ;")
                        if minted.get('policy'):
                            turtle_lines.append(f"    cardano:hasPolicyId \"{minted['policy']}\" ;")

                        # Add transfer relationship
                        turtle_lines.append(f"    cardano:transferredIn {tx_uri} ;")

                        # Remove trailing semicolon
                        if turtle_lines[-1].endswith(' ;'):
                            turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'
                        turtle_lines.append("")
                    else:
                        # Create regular token
                        turtle_lines.append(f"{asset_uri} a cardano:CNT ;")
                        if minted.get('name_utf8'):
                            turtle_lines.append(f"    blockchain:hasTokenName \"{minted['name_utf8']}\" ;")
                        if minted.get('policy'):
                            turtle_lines.append(f"    cardano:hasPolicyId \"{minted['policy']}\" ;")

                        # Add transfer relationship
                        turtle_lines.append(f"    cardano:transferredIn {tx_uri} ;")

                        # Remove trailing semicolon
                        if turtle_lines[-1].endswith(' ;'):
                            turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'
                        turtle_lines.append("")

            # Process inputs (create the entities)
            for i, tx_input in enumerate(tx['inputs']):
                input_uri = self.create_uri('tx_input', f"{tx['hash']}_input_{i}")

                # Create input entity
                turtle_lines.append(f"{input_uri} a cardano:RegularInput ;")
                turtle_lines.append(f"    cardano:referencesOutputIndex {self.format_literal(tx_input['tx_out_index'], 'xsd:decimal')} ;")

                # Remove trailing semicolon from input
                if turtle_lines[-1].endswith(' ;'):
                    turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'
                turtle_lines.append("")

            # Process outputs (create the entities)
            for output in tx['outputs']:
                output_uri = self.create_uri('tx_output', f"{tx['hash']}_output_{output['index']}")

                # Create output entity
                turtle_lines.append(f"{output_uri} a cardano:RegularOutput ;")

                # Add ADA amount
                if output['value']:
                    ada_amount_uri = self.create_uri('token_amount', f"{tx['hash']}_{output['index']}_ada")
                    turtle_lines.append(f"    blockchain:hasTokenAmount {ada_amount_uri} ;")

                # Process multi-assets
                for ma in output['multi_assets']:
                    asset_uri = self.create_uri('native_token', ma['fingerprint'])
                    amount_uri = self.create_uri('token_amount', f"{tx['hash']}_{output['index']}_{ma['fingerprint']}")

                    turtle_lines.append(f"    blockchain:hasTokenAmount {amount_uri} ;")

                # Handle datum if present
                if output.get('data_hash'):
                    datum_uri = self.create_uri('datum', output['data_hash'])
                    turtle_lines.append(f"    cardano:hasTransactionDatum {datum_uri} ;")

                # Remove trailing semicolon from output
                if turtle_lines[-1].endswith(' ;'):
                    turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'
                turtle_lines.append("")

                # Create the token amount entities for this output
                # ADA amount
                if output['value']:
                    ada_amount_uri = self.create_uri('token_amount', f"{tx['hash']}_{output['index']}_ada")
                    turtle_lines.append(f"{ada_amount_uri} a blockchain:TokenAmount ;")
                    turtle_lines.append(f"    blockchain:hasCurrency cardano:ADA ;")
                    turtle_lines.append(f"    blockchain:hasAmountValue {self.format_literal(output['value'], 'xsd:decimal')} .")
                    turtle_lines.append("")

                # Multi-asset amounts
                for ma in output['multi_assets']:
                    asset_uri = self.create_uri('native_token', ma['fingerprint'])
                    amount_uri = self.create_uri('token_amount', f"{tx['hash']}_{output['index']}_{ma['fingerprint']}")

                    # Create token amount entity
                    turtle_lines.append(f"{amount_uri} a blockchain:TokenAmount ;")
                    turtle_lines.append(f"    blockchain:hasCurrency {asset_uri} ;")
                    turtle_lines.append(f"    blockchain:hasAmountValue {self.format_literal(ma['quantity'], 'xsd:decimal')} .")
                    turtle_lines.append("")

            # Process metadata (create the entities)
            for metadata in tx['metadata']:
                meta_uri = self.create_uri('tx_metadata', f"{tx['hash']}_meta_{metadata['key']}")

                # Create metadata entity
                turtle_lines.append(f"{meta_uri} a cardano:TransactionMetadata .")

                # Check if metadata contains governance proposal
                if metadata.get('json'):
                    try:
                        import json
                        meta_obj = json.loads(metadata['json']) if isinstance(metadata['json'], str) else metadata['json']

                        # Check various proposal patterns
                        is_proposal = False
                        if isinstance(meta_obj, dict):
                            # Check for CIP-30 governance proposal patterns
                            if any(key in str(meta_obj).lower() for key in ['proposal', 'governance', 'vote', 'treasury']):
                                is_proposal = True
                            # Check for specific proposal fields
                            if any(key in meta_obj for key in ['title', 'abstract', 'motivation', 'rationale']):
                                is_proposal = True

                        if is_proposal:
                            proposal_uri = self.create_uri('governance_proposal', f"{tx['hash']}_proposal_{metadata['key']}")
                            turtle_lines.append(f"{meta_uri} cardano:hasGovernanceProposal {proposal_uri} .")

                            # Create the proposal entity with proper ID
                            turtle_lines.append("")
                            turtle_lines.append(f"{proposal_uri} a cardano:GovernanceProposal ;")
                            turtle_lines.append(f"    cardano:hasProposalId \"{tx['hash']}_proposal_{metadata['key']}\" ;")

                            # Extract proposal details if available
                            if isinstance(meta_obj, dict):
                                if 'title' in meta_obj:
                                    title = str(meta_obj['title']).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                                    turtle_lines.append(f"    cardano:hasProposalTitle \"{title}\" ;")
                                if 'abstract' in meta_obj or 'description' in meta_obj:
                                    desc = meta_obj.get('abstract', meta_obj.get('description', ''))
                                    escaped_desc = str(desc).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                                    turtle_lines.append(f"    cardano:hasProposalDescription \"{escaped_desc}\" ;")
                                if 'proposer' in meta_obj:
                                    proposer = str(meta_obj['proposer']).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                                    turtle_lines.append(f"    cardano:hasProposalProposer \"{proposer}\" ;")
                                if 'budget' in meta_obj or 'amount' in meta_obj:
                                    budget = meta_obj.get('budget', meta_obj.get('amount', 0))
                                    turtle_lines.append(f"    cardano:hasRequestedBudget {self.format_literal(budget, 'xsd:decimal')} ;")

                            # Default status
                            turtle_lines.append(f"    cardano:hasProposalStatus \"active\" ;")

                            # Remove trailing semicolon
                            if turtle_lines[-1].endswith(' ;'):
                                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'
                            turtle_lines.append("")
                    except Exception as e:
                        logger.debug(f"Error parsing metadata JSON: {e}")

                turtle_lines.append("")

        # Add block-transaction relationships
        for block_uri, tx_uri, timestamp in block_tx_links:
            turtle_lines.append(f"{block_uri} blockchain:hasTransaction {tx_uri} .")
            if timestamp:
                turtle_lines.append(f"{block_uri} blockchain:hasTimestamp {self.format_literal(timestamp, 'xsd:dateTime')} .")

        turtle_lines.append("")

        return '\n'.join(turtle_lines)