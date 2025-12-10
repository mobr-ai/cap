import logging
import json
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

            # Datum as c:Datum
            turtle_lines.append(f"{datum_uri} a c:Datum ;")

            if datum['hash']:
                turtle_lines.append(f"    b:hasHash \"{datum['hash']}\" ;")

            if datum['value'] is not None:
                # Handle the case where value might be a dict or string
                if isinstance(datum['value'], dict):
                    # Convert dict to JSON string
                    value_str = json.dumps(datum['value'])
                else:
                    # It's already a string
                    value_str = str(datum['value'])

                # Now escape the string
                escaped_value = value_str.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                turtle_lines.append(f"    c:hasDatumContent {self.format_literal(escaped_value)} ;")

            if datum['bytes']:
                turtle_lines.append(f"    c:hasDatumBytes \"{datum['bytes']}\" ;")

            if datum['tx_hash']:
                tx_uri = self.create_transaction_uri(datum['tx_hash'])
                turtle_lines.append(f"    c:datumEmbeddedIn {tx_uri} ;")

            # Remove trailing semicolon and add period
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

        return '\n'.join(turtle_lines)