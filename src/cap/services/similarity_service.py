"""Service for finding similar cached natural language queries."""
import logging
import json
from typing import Any
from opentelemetry import trace

from cap.rdf.cache.query_normalizer import QueryNormalizer
from cap.services.redis_nl_client import get_redis_nl_client

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class SimilarityService:
    """Find similar cached queries using various similarity metrics."""

    @staticmethod
    def calculate_jaccard_similarity(query1: str, query2: str) -> float:
        """Calculate Jaccard similarity between two normalized queries."""
        words1 = set(query1.split())
        words2 = set(query2.split())

        if not words1 or not words2:
            return 0.0

        intersection = words1.intersection(words2)
        union = words1.union(words2)

        return len(intersection) / len(union)

    @staticmethod
    async def find_similar_queries(
        nl_query: str,
        top_n: int = 5,
        min_similarity: float = 0.3
    ) -> list[dict[str, Any]]:
        """
        Find top N most similar cached queries.

        Args:
            nl_query: Natural language query to find similarities for
            top_n: Number of top similar queries to return
            min_similarity: Minimum similarity threshold (0.0 to 1.0)

        Returns:
            List of dicts containing original_query, sparql_query, similarity_score
        """
        with tracer.start_as_current_span("find_similar_queries") as span:
            span.set_attribute("input_query", nl_query)
            span.set_attribute("top_n", top_n)

            try:
                redis_client = get_redis_nl_client()
                normalized_input = QueryNormalizer.normalize(nl_query)
                client = await redis_client._get_nl_client()

                similar_queries = []

                async for cache_key in client.scan_iter(match="nlq:cache:*"):
                    cached_data_str = await client.get(cache_key)
                    if not cached_data_str:
                        continue

                    cached_data = json.loads(cached_data_str)
                    cached_normalized = cached_data.get("normalized_query", "")

                    similarity = SimilarityService.calculate_jaccard_similarity(
                        normalized_input,
                        cached_normalized
                    )

                    if similarity >= min_similarity:
                        original_nl_query = cached_data.get("original_query", "")
                        sparql_query = await redis_client.get_cached_query_with_original(
                            normalized_query=cached_normalized,
                            original_query=original_nl_query
                        )
                        similar_queries.append({
                            "original_query": original_nl_query,
                            "normalized_query": cached_normalized,
                            "sparql_query": sparql_query,
                            "similarity_score": similarity,
                            "is_sequential": cached_data.get("is_sequential", False),
                            "precached": cached_data.get("precached", False)
                        })

                similar_queries.sort(key=lambda x: x["similarity_score"], reverse=True)
                result = similar_queries[:top_n]

                span.set_attribute("results_found", len(result))
                logger.info(f"Found {len(result)} similar queries for '{nl_query}'")

                return result

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Failed to find similar queries: {e}", exc_info=True)
                return []
