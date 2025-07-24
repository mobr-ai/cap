from typing import Any, Optional, Iterator
from sqlalchemy import func
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.data.cdb_model import Epoch

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class EpochExtractor(BaseExtractor):
    """Extracts epoch data from cardano-db-sync."""
    
    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract epochs in batches."""
        with tracer.start_as_current_span("epoch_extraction") as span:
            query = self.db_session.query(Epoch)
            
            if last_processed_id:
                query = query.filter(Epoch.id > last_processed_id)
            
            query = query.order_by(Epoch.id)
            
            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break
                
                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)
                
                yield [self._serialize_epoch(epoch) for epoch in batch]
                offset += self.batch_size
    
    def _serialize_epoch(self, epoch: Epoch) -> dict[str, Any]:
        """Serialize epoch to dictionary."""
        return {
            'id': epoch.id,
            'no': epoch.no,
            'out_sum': str(epoch.out_sum) if epoch.out_sum else None,
            'fees': str(epoch.fees) if epoch.fees else None,
            'tx_count': epoch.tx_count,
            'blk_count': epoch.blk_count,
            'start_time': epoch.start_time.isoformat() if epoch.start_time else None,
            'end_time': epoch.end_time.isoformat() if epoch.end_time else None
        }
    
    def get_total_count(self) -> int:
        return self.db_session.query(func.count(Epoch.id)).scalar()
    
    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(Epoch.id)).scalar()
        return result
