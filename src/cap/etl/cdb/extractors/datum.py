from typing import Any, Optional, Iterator
from sqlalchemy.orm import joinedload
from sqlalchemy import func
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.data.cdb_model import Datum

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class DatumExtractor(BaseExtractor):
    """Extracts datum data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract datums in batches."""
        with tracer.start_as_current_span("datum_extraction") as span:
            query = self.db_session.query(Datum).options(
                joinedload(Datum.tx)
            )

            if last_processed_id:
                query = query.filter(Datum.id > last_processed_id)

            query = query.order_by(Datum.id)

            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_datum(datum) for datum in batch]
                offset += self.batch_size

    def _serialize_datum(self, datum: Datum) -> dict[str, Any]:
        """Serialize datum to dictionary."""
        return {
            'id': datum.id,
            'hash': datum.hash.hex() if datum.hash else None,
            'tx_id': datum.tx_id,
            'tx_hash': datum.tx.hash.hex() if datum.tx and datum.tx.hash else None,
            'value': datum.value,
            'bytes': datum.bytes.hex() if datum.bytes else None
        }

    def get_total_count(self) -> int:
        return self.db_session.query(func.count(Datum.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(Datum.id)).scalar()
        return result