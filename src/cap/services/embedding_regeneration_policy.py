"""
Stateless policy that decides when the ChromaDB embedding index must be rebuilt.

Triggers:
  - The index has never been built, OR
  - 24 hours have elapsed since the last rebuild, OR
  - 100 new queries have been successfully cached since the last rebuild.

No I/O. No app dependencies. Trivially unit-testable.
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
    """Given a RegenerationState, returns whether a rebuild is due."""

    @staticmethod
    def should_regenerate(state: RegenerationState) -> bool:
        if state.last_regenerated_at is None:
            logger.debug("Rebuild needed: index has never been built.")
            return True

        elapsed_hours = (
            datetime.now(timezone.utc) - state.last_regenerated_at
        ).total_seconds() / 3600

        if elapsed_hours >= _REGENERATION_INTERVAL_HOURS:
            logger.debug(
                f"Rebuild needed: {elapsed_hours:.1f}h elapsed "
                f"(threshold: {_REGENERATION_INTERVAL_HOURS}h)."
            )
            return True

        if state.cached_since_last_regen >= _NEW_QUERIES_THRESHOLD:
            logger.debug(
                f"Rebuild needed: {state.cached_since_last_regen} new queries cached "
                f"since last rebuild (threshold: {_NEW_QUERIES_THRESHOLD})."
            )
            return True

        return False