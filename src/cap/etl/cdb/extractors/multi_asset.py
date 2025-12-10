from typing import Any, Optional, Iterator
from sqlalchemy import func, select
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.rdf.cdb_model import MultiAsset

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class MultiAssetExtractor(BaseExtractor):
    """Extracts multi-asset (native token) data from cardano-db-sync."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract multi-assets in batches."""
        with tracer.start_as_current_span("multi_asset_extraction") as span:
            stmt = select(MultiAsset).order_by(MultiAsset.id)

            if last_processed_id:
                stmt = stmt.filter(MultiAsset.id > last_processed_id)

            offset = 0
            while True:
                batch = self.db_session.execute(
                    stmt.offset(offset).limit(self.batch_size)
                ).scalars().all()

                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_multi_asset(asset) for asset in batch]
                offset += self.batch_size

    def _serialize_multi_asset(self, asset: MultiAsset) -> dict[str, Any]:
        """Serialize multi-asset to dictionary."""
        return {
            'id': asset.id,
            'policy': asset.policy.hex() if asset.policy else None,
            'name': asset.name.hex() if asset.name else None,
            'name_utf8': asset.name.decode('utf-8', errors='ignore') if asset.name else None,
            'fingerprint': asset.fingerprint
        }

    def get_total_count(self) -> int:
        stmt = select(func.count(MultiAsset.id))
        return self.db_session.execute(stmt).scalar()

    def get_last_id(self) -> Optional[int]:
        stmt = select(func.max(MultiAsset.id))
        return self.db_session.execute(stmt).scalar()