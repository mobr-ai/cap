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

            # Asset as c:CNT
            turtle_lines.append(f"{asset_uri} a c:CNT ;")

            # Use b:hasHash for fingerprint
            if asset['fingerprint']:
                turtle_lines.append(f"    b:hasHash \"{asset['fingerprint']}\" ;")

            if asset['policy']:
                turtle_lines.append(f"    c:hasPolicyId \"{asset['policy']}\" ;")

            if asset['name']:
                # Escape the name properly
                escaped_name = asset['name'].replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                turtle_lines.append(f"    b:hasTokenName \"{escaped_name}\" ;")

            # Remove trailing semicolon and add period
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

        return '\n'.join(turtle_lines)
