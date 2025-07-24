import logging
from typing import Any

from cap.etl.cdb.transformers.transformer import BaseTransformer

logger = logging.getLogger(__name__)

class DatumTransformer(BaseTransformer):
    """Transforms datum data to RDF aligned with Cardano ontology."""

    def transform(self, datums: list[dict[str, Any]]) -> str:
        """Transform datums to RDF Turtle format."""
        turtle_lines = []

        for datum in datums:
            datum_uri = self.create_uri('datum', datum['hash'])

            # Datum as cardano:Datum
            turtle_lines.append(f"{datum_uri} a cardano:Datum ;")

            if datum['hash']:
                turtle_lines.append(f"    blockchain:hasHash \"{datum['hash']}\" ;")

            if datum['value']:
                # Escape the JSON value properly
                escaped_value = datum['value'].replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                turtle_lines.append(f"    cardano:hasDatumContent {self.format_literal(escaped_value)} ;")

            if datum['bytes']:
                turtle_lines.append(f"    cardano:hasDatumBytes \"{datum['bytes']}\" ;")

            if datum['tx_hash']:
                tx_uri = self.create_transaction_uri(datum['tx_hash'])
                turtle_lines.append(f"    cardano:datumEmbeddedIn {tx_uri} ;")

            # Remove trailing semicolon and add period
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

        return '\n'.join(turtle_lines)
