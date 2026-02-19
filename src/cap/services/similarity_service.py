"""
Finds similar cached NL queries.

Strategy (in order):
  1. Embedding similarity via EmbeddingService (multilingual-e5-small + ChromaDB).
     The index is rebuilt on demand, governed by EmbeddingRegenerationPolicy.
  2. Jaccard token-overlap fallback (scans Redis) — used only when embeddings fail.
"""
import logging
import json
from typing import Any

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

# Shared regeneration state — lives for the lifetime of the process.
_regeneration_state = RegenerationState()


class SimilarityService:
    """Find similar cached queries, preferring embedding-based similarity."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    async def find_similar_queries(
        nl_query: str,
        top_n: int = 5,
        min_similarity: float = 0.0,
    ) -> list[dict[str, Any]]:
        """
        Return the top-N most similar cached queries.

        Attempts embedding-based search first; falls back to Jaccard on failure.

        Each result dict contains:
            original_query, normalized_query, sparql_query,
            similarity_score, is_sequential, precached
        """
        with tracer.start_as_current_span("similarity_service.find_similar_queries") as span:
            span.set_attribute("input_query", nl_query)
            span.set_attribute("top_n", top_n)

            try:
                results = await SimilarityService._embedding_search(
                    nl_query=nl_query,
                    top_n=top_n,
                    min_similarity=min_similarity,
                )
                span.set_attribute("strategy", "embedding")
                logger.info(
                    f"Embedding search returned {len(results)} results for '{nl_query}'."
                )
                return results

            except Exception as embedding_error:
                logger.warning(
                    f"Embedding search failed ({embedding_error}); "
                    "falling back to Jaccard similarity."
                )
                span.set_attribute("strategy", "jaccard_fallback")
                span.set_attribute("embedding_error", str(embedding_error))

            try:
                results = await SimilarityService._jaccard_search(
                    nl_query=nl_query,
                    top_n=top_n,
                    min_similarity=min_similarity,
                )
                logger.info(
                    f"Jaccard fallback returned {len(results)} results for '{nl_query}'."
                )
                return results

            except Exception as jaccard_error:
                span.set_attribute("jaccard_error", str(jaccard_error))
                logger.error(f"Jaccard fallback also failed: {jaccard_error}")
                return []

    @staticmethod
    async def notify_cache_updated() -> None:
        """
        Must be called every time a new query is successfully cached in Redis.
        Increments the counter and triggers index regeneration if the policy says so.
        """
        _regeneration_state.record_new_cache()

        if EmbeddingRegenerationPolicy.should_regenerate(_regeneration_state):
            await SimilarityService._rebuild_embedding_index()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _rebuild_embedding_index() -> None:
        """Load all entries from Redis and hand them to EmbeddingService.rebuild()."""
        with tracer.start_as_current_span("similarity_service.rebuild_index"):
            try:
                redis_client = get_redis_nl_client()
                client = await redis_client._get_nl_client()

                cached_entries: list[dict[str, Any]] = []
                async for cache_key in client.scan_iter(match="nlq:cache:*"):
                    raw = await client.get(cache_key)
                    if not raw:
                        continue
                    try:
                        entry = json.loads(raw)
                        cached_entries.append(entry)
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse cache entry for key {cache_key}.")

                embedding_service = get_embedding_service()
                await embedding_service.rebuild(cached_entries)
                _regeneration_state.record_regenerated()

            except Exception as exc:
                logger.error(
                    f"Failed to rebuild embedding index: {exc}", exc_info=True
                )

    @staticmethod
    async def _embedding_search(
        nl_query: str,
        top_n: int,
        min_similarity: float,
    ) -> list[dict[str, Any]]:
        """Delegate to EmbeddingService, ensuring the index is fresh first."""
        if EmbeddingRegenerationPolicy.should_regenerate(_regeneration_state):
            await SimilarityService._rebuild_embedding_index()

        embedding_service = get_embedding_service()
        return await embedding_service.search(
            nl_query=nl_query,
            top_n=top_n,
            min_similarity=min_similarity,
        )

    @staticmethod
    async def _jaccard_search(
        nl_query: str,
        top_n: int,
        min_similarity: float,
    ) -> list[dict[str, Any]]:
        """
        Scan every Redis cache entry and rank by Jaccard token-overlap.
        Preserves the original behaviour of the legacy similarity service.
        """
        redis_client = get_redis_nl_client()
        client = await redis_client._get_nl_client()
        normalized_input = QueryNormalizer.normalize(nl_query)

        candidates: list[dict[str, Any]] = []

        async for cache_key in client.scan_iter(match="nlq:cache:*"):
            raw = await client.get(cache_key)
            if not raw:
                continue

            try:
                cached_data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            cached_normalized = cached_data.get("normalized_query", "")
            score = SimilarityService._jaccard(normalized_input, cached_normalized)

            if score < min_similarity:
                continue

            original_nl_query = cached_data.get("original_query", "")
            sparql_data = await redis_client.get_cached_query_with_original(
                normalized_query=cached_normalized,
                original_query=original_nl_query,
            )

            candidates.append(
                {
                    "original_query": original_nl_query,
                    "normalized_query": cached_normalized,
                    "sparql_query": sparql_data,
                    "similarity_score": score,
                    "is_sequential": cached_data.get("is_sequential", False),
                    "precached": cached_data.get("precached", False),
                }
            )

        candidates.sort(key=lambda x: x["similarity_score"], reverse=True)
        return candidates[:top_n]

    @staticmethod
    def _jaccard(query1: str, query2: str) -> float:
        words1 = set(query1.split())
        words2 = set(query2.split())
        if not words1 or not words2:
            return 0.0
        return len(words1 & words2) / len(words1 | words2)