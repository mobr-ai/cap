from typing import Any, Optional, Iterator
from sqlalchemy.orm import selectinload
from sqlalchemy import func, select
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.rdf.cdb_model import Datum

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class DatumExtractor(BaseExtractor):
    """Extracts datum data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract datums in batches."""
        with tracer.start_as_current_span("datum_extraction") as span:
            stmt = (
                select(Datum)
                .options(selectinload(Datum.tx))
                .order_by(Datum.id)
            )

            if last_processed_id:
                stmt = stmt.filter(Datum.id > last_processed_id)

            offset = 0
            while True:
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

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
        stmt = select(func.count(Datum.id))
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(Datum.id))
        return self.db_session.execute(stmt).scalar()