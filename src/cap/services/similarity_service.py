"""
Finds similar cached NL queries.

Strategy:
  1. Embedding similarity (multilingual-e5-small + ChromaDB).
     The index is rebuilt on demand per EmbeddingRegenerationPolicy.
  2. Jaccard token-overlap fallback — activated only when embeddings fail.

The regeneration state lives here because SimilarityService is the only
consumer of the policy. Neither RedisNLClient nor nl_service know it exists.
"""
import json
import logging
from typing import Any
from enum import Enum
from opentelemetry import trace

from cap.rdf.cache.query_normalizer import QueryNormalizer
from cap.services.redis_nl_client import get_redis_nl_client
from cap.services.embedding_service import get_embedding_service
from cap.services.embedding_regeneration_policy import (
    EmbeddingRegenerationPolicy,
    RegenerationState,
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

# Process-lifetime regeneration state owned exclusively by this module.
_regen_state = RegenerationState()


class SearchStrategy(str, Enum):
    auto = "auto"
    embeddings = "embeddings"
    jaccard = "jaccard"
    none = "none"


class SimilarityService:
    """
    Similarity search over the NL query cache.

    Public surface:
        find_similar_queries()   — called by LLMClient for few-shot examples.
        notify_new_cache_entry() — called by nl_service after a successful cache_query().
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    async def find_similar_queries(
        strategy: SearchStrategy,
        nl_query: str,
        top_n: int = 5,
        min_similarity: float = 0.0,
    ) -> list[dict[str, Any]]:
        """Return the top-N most similar cached queries."""
        with tracer.start_as_current_span("similarity_service.find_similar_queries") as span:
            span.set_attribute("input_query", nl_query)

            # --- Primary: embedding search ---
            if strategy == SearchStrategy.auto or strategy == SearchStrategy.embeddings:
                try:
                    await SimilarityService._ensure_index_is_fresh()
                    results = await get_embedding_service().search(
                        nl_query=nl_query,
                        top_n=top_n,
                        min_similarity=min_similarity,
                    )
                    span.set_attribute("strategy", "embedding")
                    logger.info(f"Embedding search: {len(results)} results for '{nl_query}'.")
                    return results

                except Exception as embedding_exc:
                    logger.warning(
                        f"Embedding search failed ({embedding_exc}); "
                        "falling back to Jaccard similarity."
                    )
                    span.set_attribute("strategy", "jaccard_fallback")
                    span.set_attribute("embedding_error", str(embedding_exc))

            # --- Fallback: Jaccard over Redis ---
            if strategy == SearchStrategy.auto or strategy == SearchStrategy.jaccard:
                try:
                    results = await SimilarityService._jaccard_search(
                        nl_query=nl_query,
                        top_n=top_n,
                        min_similarity=min_similarity,
                    )
                    logger.info(f"Jaccard fallback: {len(results)} results for '{nl_query}'.")
                    return results

                except Exception as jaccard_exc:
                    span.set_attribute("jaccard_error", str(jaccard_exc))
                    logger.error(f"Jaccard fallback also failed: {jaccard_exc}", exc_info=True)
                    return []

        return []


    @staticmethod
    async def notify_new_cache_entry() -> None:
        """
        Signal that a new query was successfully written to Redis.
        Called by nl_service — never by RedisNLClient.

        Increments the counter; triggers a background index rebuild
        if the regeneration policy says it is due.
        """
        _regen_state.record_new_cache()

        if EmbeddingRegenerationPolicy.should_regenerate(_regen_state):
            try:
                await SimilarityService._rebuild_index()
            except Exception as exc:
                # Rebuild failure must never surface to the NL pipeline.
                logger.error(f"Background index rebuild failed: {exc}", exc_info=True)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _ensure_index_is_fresh() -> None:
        """Trigger a rebuild before search if the policy requires it."""
        if EmbeddingRegenerationPolicy.should_regenerate(_regen_state):
            await SimilarityService._rebuild_index()

    @staticmethod
    async def _rebuild_index() -> None:
        """Read all entries from Redis and hand them to EmbeddingService.rebuild()."""
        with tracer.start_as_current_span("similarity_service.rebuild_index"):
            redis_client = get_redis_nl_client()
            client = await redis_client._get_nl_client()

            entries: list[dict[str, Any]] = []
            async for cache_key in client.scan_iter(match="nlq:cache:*"):
                raw = await client.get(cache_key)
                if not raw:
                    continue
                try:
                    entries.append(json.loads(raw))
                except json.JSONDecodeError:
                    logger.warning(f"Skipping malformed cache entry at key '{cache_key}'.")

            await get_embedding_service().rebuild(entries)
            _regen_state.record_regenerated()
            logger.info(f"Embedding index rebuilt from {len(entries)} Redis entries.")

    @staticmethod
    async def _jaccard_search(
        nl_query: str,
        top_n: int,
        min_similarity: float,
    ) -> list[dict[str, Any]]:
        """Scan Redis and rank entries by Jaccard token-overlap on normalised queries."""
        redis_client = get_redis_nl_client()
        client = await redis_client._get_nl_client()
        normalized_input = QueryNormalizer.normalize(nl_query)

        candidates: list[dict[str, Any]] = []

        async for cache_key in client.scan_iter(match="nlq:cache:*"):
            raw = await client.get(cache_key)
            if not raw:
                continue
            try:
                entry = json.loads(raw)
            except json.JSONDecodeError:
                continue

            cached_normalized = entry.get("normalized_query", "")
            score = SimilarityService._jaccard(normalized_input, cached_normalized)
            if score < min_similarity:
                continue

            original_nl = entry.get("original_query", "")
            sparql_data = await redis_client.get_cached_query_with_original(
                normalized_query=cached_normalized,
                original_query=original_nl,
            )
            candidates.append({
                "original_query": original_nl,
                "normalized_query": cached_normalized,
                "sparql_query": sparql_data["sparql_query"],
                "similarity_score": score,
                "is_sequential": entry.get("is_sequential", False),
                "precached": entry.get("precached", False),
            })

        candidates.sort(key=lambda x: x["similarity_score"], reverse=True)
        return candidates[:top_n]

    @staticmethod
    def _jaccard(a: str, b: str) -> float:
        sa, sb = set(a.split()), set(b.split())
        if not sa or not sb:
            return 0.0
        return len(sa & sb) / len(sa | sb)