from typing import Any, Optional, Iterator
from sqlalchemy.orm import selectinload
from sqlalchemy import func, select
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.rdf.cdb_model import Script

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class ScriptExtractor(BaseExtractor):
    """Extracts script data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract scripts in batches."""
        with tracer.start_as_current_span("script_extraction") as span:
            stmt = (
                select(Script)
                .options(selectinload(Script.tx))
                .order_by(Script.id)
            )

            if last_processed_id:
                stmt = stmt.filter(Script.id > last_processed_id)

            offset = 0
            while True:
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_script(script) for script in batch]
                offset += self.batch_size

    def _serialize_script(self, script: Script) -> dict[str, Any]:
        """Serialize script to dictionary."""
        return {
            'id': script.id,
            'tx_id': script.tx_id,
            'tx_hash': script.tx.hash.hex() if script.tx and script.tx.hash else None,
            'hash': script.hash.hex() if script.hash else None,
            'type': script.type,
            'json': script.json,
            'bytes': script.bytes.hex() if script.bytes else None,
            'serialised_size': script.serialised_size
        }

    def get_total_count(self) -> int:
        stmt = select(func.count(Script.id))
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(Script.id))
        return self.db_session.execute(stmt).scalar()