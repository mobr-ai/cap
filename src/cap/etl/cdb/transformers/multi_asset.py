import logging
from typing import Any

from cap.etl.cdb.transformers.transformer import BaseTransformer

logger = logging.getLogger(__name__)

class MultiAssetTransformer(BaseTransformer):
    """Transforms multi-asset (native token) data to RDF aligned with Cardano ontology."""

    def transform(self, assets: list[dict[str, Any]]) -> str:
        """Transform multi-assets to RDF Turtle format."""
        turtle_lines = []

        for asset in assets:
            asset_uri = self.create_uri('native_token', asset['fingerprint'])

            # Asset as cardano:CNT
            turtle_lines.append(f"{asset_uri} a cardano:CNT ;")

            # Use blockchain:hasHash for fingerprint
            if asset['fingerprint']:
                turtle_lines.append(f"    blockchain:hasHash \"{asset['fingerprint']}\" ;")

            if asset['policy']:
                turtle_lines.append(f"    cardano:hasPolicyId \"{asset['policy']}\" ;")

            if asset['name']:
                turtle_lines.append(f"    blockchain:hasTokenName \"{asset['name']}\" ;")

            # Remove trailing semicolon and add period
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

        return '\n'.join(turtle_lines)
