"""
Policy that decides when the ChromaDB embedding index should be regenerated.

Rules:
  - More than 24 hours have elapsed since the last full regeneration, OR
  - At least 100 new queries have been successfully cached since the last regeneration.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_REGENERATION_INTERVAL_HOURS: int = 24
_NEW_QUERIES_THRESHOLD: int = 100


@dataclass
class RegenerationState:
    last_regenerated_at: Optional[datetime] = field(default=None)
    cached_since_last_regen: int = field(default=0)

    def record_new_cache(self) -> None:
        self.cached_since_last_regen += 1

    def record_regenerated(self) -> None:
        self.last_regenerated_at = datetime.now(timezone.utc)
        self.cached_since_last_regen = 0


class EmbeddingRegenerationPolicy:
    """Stateless policy: given a RegenerationState, decides if regeneration is needed."""

    @staticmethod
    def should_regenerate(state: RegenerationState) -> bool:
        if state.last_regenerated_at is None:
            logger.debug("Regeneration needed: never regenerated before.")
            return True

        elapsed_hours = (
            datetime.now(timezone.utc) - state.last_regenerated_at
        ).total_seconds() / 3600

        if elapsed_hours >= _REGENERATION_INTERVAL_HOURS:
            logger.debug(
                f"Regeneration needed: {elapsed_hours:.1f}h elapsed "
                f"(threshold: {_REGENERATION_INTERVAL_HOURS}h)."
            )
            return True

        if state.cached_since_last_regen >= _NEW_QUERIES_THRESHOLD:
            logger.debug(
                f"Regeneration needed: {state.cached_since_last_regen} new queries "
                f"cached since last regen (threshold: {_NEW_QUERIES_THRESHOLD})."
            )
            return True

        return False