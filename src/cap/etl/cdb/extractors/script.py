from typing import Any, Optional, Iterator
from sqlalchemy.orm import joinedload
from sqlalchemy import func
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.data.cdb_model import Script

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class ScriptExtractor(BaseExtractor):
    """Extracts script data from cardano-db-sync."""
    
    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract scripts in batches."""
        with tracer.start_as_current_span("script_extraction") as span:
            query = self.db_session.query(Script).options(
                joinedload(Script.tx)
            )
            
            if last_processed_id:
                query = query.filter(Script.id > last_processed_id)
            
            query = query.order_by(Script.id)
            
            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
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
        return self.db_session.query(func.count(Script.id)).scalar()
    
    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(Script.id)).scalar()
        return result
