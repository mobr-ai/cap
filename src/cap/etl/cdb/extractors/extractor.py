"""
Base extractor for cardano-db-sync ETL pipeline.
Each extractor handles specific data extraction from PostgreSQL.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Iterator
from sqlalchemy.orm import Session

class BaseExtractor(ABC):
    """Base class for all data extractors."""
    
    def __init__(self, db_session: Session, batch_size: int = 1000):
        self.db_session = db_session
        self.batch_size = batch_size
    
    @abstractmethod
    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract data in batches."""
        pass

    @abstractmethod
    def get_total_count(self) -> int:
        """Get total number of records to process."""
        pass
    
    @abstractmethod
    def get_last_id(self) -> Optional[int]:
        """Get the highest ID in the table."""
        pass
