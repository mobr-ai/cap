import logging
from typing import Any

from cap.etl.cdb.transformers.transformer import BaseTransformer

logger = logging.getLogger(__name__)

class EpochTransformer(BaseTransformer):
    """Transforms epoch data to RDF aligned with Cardano ontology."""

    def transform(self, epochs: list[dict[str, Any]]) -> str:
        """Transform epochs to RDF Turtle format."""
        turtle_lines = []

        for epoch in epochs:
            epoch_uri = self.create_epoch_uri(epoch['no'])

            # Epoch as c:Epoch
            turtle_lines.append(f"{epoch_uri} a c:Epoch ;")

            if epoch['no'] is not None:
                turtle_lines.append(f"    c:hasEpochNumber {self.format_literal(epoch['no'], 'xsd:decimal')} ;")

            if epoch['start_time']:
                turtle_lines.append(f"    c:hasStartTime {self.format_literal(epoch['start_time'], 'xsd:dateTime')} ;")

            if epoch['end_time']:
                turtle_lines.append(f"    c:hasEndTime {self.format_literal(epoch['end_time'], 'xsd:dateTime')} ;")

            # Properties not in ontology removed: hasTxCount, hasBlockCount, hasOutputSum, hasTotalFees

            # Remove trailing semicolon and add period
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

        return '\n'.join(turtle_lines)