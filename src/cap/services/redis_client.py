"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import json
import logging
import os
from typing import Optional, Any, Tuple

import redis.asyncio as redis
from opentelemetry import trace

from cap.data.cache.placeholder_counters import PlaceholderCounters
from cap.data.cache.placeholder_restorer import PlaceholderRestorer
from cap.data.cache.query_normalizer import QueryNormalizer
from cap.data.cache.query_file_parser import QueryFileParser
from cap.data.cache.sparql_normalizer import SPARQLNormalizer
from cap.data.cache.value_extractor import ValueExtractor

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class RedisClient:
    """Client for Redis caching operations."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 6379,
        db: int = 0,
        ttl: int = 86400 * 365
    ):
        """Initialize Redis client."""
        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", port))
        self.db = db
        self.ttl = ttl
        self._client: Optional[redis.Redis] = None

    async def _get_client(self) -> redis.Redis:
        """Get or create Redis client."""
        if self._client is None:
            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True
            )
        return self._client

    async def close(self):
        """Close the Redis client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def _make_cache_key(self, nl_query: str) -> str:
        """Create cache key from natural language query."""
        normalized_nl = QueryNormalizer.normalize(nl_query)
        return f"nlq:cache:{normalized_nl}"

    def _make_count_key(self, nl_query: str) -> str:
        """Create count key from natural language query."""
        normalized = QueryNormalizer.normalize(nl_query)
        return f"nlq:count:{normalized}"

    async def cache_query(
        self,
        nl_query: str,
        sparql_query: str,
        ttl: Optional[int] = None
    ) -> bool:
        """Cache query with placeholder normalization."""
        with tracer.start_as_current_span("cache_sparql_query") as span:
            span.set_attribute("nl_query", nl_query)

            try:
                client = await self._get_client()
                normalized = QueryNormalizer.normalize(nl_query)

                # Process SPARQL (single or sequential)
                normalized_sparql, placeholder_map = self._normalize_sparql(sparql_query)

                cache_key = self._make_cache_key(nl_query)
                count_key = self._make_count_key(nl_query)

                cache_data = {
                    "original_query": nl_query,
                    "normalized_query": normalized,
                    "sparql_query": normalized_sparql,
                    "placeholder_map": placeholder_map,
                    "precached": False
                }

                ttl_value = ttl or self.ttl
                await client.setex(cache_key, ttl_value, json.dumps(cache_data))
                await client.incr(count_key)
                await client.expire(count_key, ttl_value)

                return True

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Failed to cache query: {e}")
                return False

    def _normalize_sparql(self, sparql_query: str) -> Tuple[str, dict[str, str]]:
        """Normalize SPARQL query (handles single and sequential)."""
        try:
            parsed = json.loads(sparql_query)
            if isinstance(parsed, list):
                return self._normalize_sequential_sparql(parsed)
            else:
                normalizer = SPARQLNormalizer()
                return normalizer.normalize(sparql_query)
        except (json.JSONDecodeError, TypeError):
            normalizer = SPARQLNormalizer()
            return normalizer.normalize(sparql_query)

    def _normalize_sequential_sparql(self, queries: list[dict]) -> Tuple[str, dict[str, str]]:
        """Normalize sequential SPARQL queries with global counters."""
        normalized_queries = []
        all_placeholders = {}
        counters = PlaceholderCounters()

        for query_info in queries:
            normalizer = SPARQLNormalizer()
            norm_q, placeholders = normalizer.normalize(query_info['query'], counters)

            # Update global counters
            for placeholder in placeholders.keys():
                counters.update_from_placeholder(placeholder)

            all_placeholders.update(placeholders)
            query_info['query'] = norm_q
            normalized_queries.append(query_info)

        return json.dumps(normalized_queries), all_placeholders

    async def get_cached_query_with_original(
        self,
        normalized_query: str,
        original_query: str
    ) -> Optional[dict[str, Any]]:
        """Retrieve cached query and restore placeholders."""
        with tracer.start_as_current_span("get_cached_query_with_original") as span:
            try:
                client = await self._get_client()
                cache_key = self._make_cache_key(normalized_query)

                cached = await client.get(cache_key)
                if not cached:
                    span.set_attribute("cache_hit", False)
                    return None

                data = json.loads(cached)
                current_values = ValueExtractor.extract(original_query)
                placeholder_map = data.get("placeholder_map", {})

                if not placeholder_map:
                    span.set_attribute("cache_hit", True)
                    return data

                # Restore placeholders
                restored_sparql = self._restore_sparql(
                    data["sparql_query"],
                    placeholder_map,
                    current_values
                )
                data["sparql_query"] = restored_sparql

                span.set_attribute("cache_hit", True)
                return data

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Failed to retrieve cached query: {e}", exc_info=True)
                return None

    def _restore_sparql(
        self,
        sparql: str,
        placeholder_map: dict[str, str],
        current_values: dict[str, list[str]]
    ) -> str:
        """Restore SPARQL with actual values."""
        try:
            parsed = json.loads(sparql)
            if isinstance(parsed, list):
                for query_info in parsed:
                    query_info['query'] = PlaceholderRestorer.restore(
                        query_info['query'],
                        placeholder_map,
                        current_values
                    )
                return json.dumps(parsed)
        except (json.JSONDecodeError, TypeError):
            pass

        return PlaceholderRestorer.restore(sparql, placeholder_map, current_values)

    async def get_query_count(self, nl_query: str) -> int:
        """Get the number of times a query has been asked."""
        try:
            client = await self._get_client()
            count_key = self._make_count_key(nl_query)
            count = await client.get(count_key)
            return int(count) if count else 0
        except Exception as e:
            logger.error(f"Failed to get query count: {e}")
            return 0

    async def get_popular_queries(self, limit: int = 5) -> list[dict[str, Any]]:
        """Get most popular queries."""
        with tracer.start_as_current_span("get_popular_queries") as span:
            span.set_attribute("limit", limit)

            try:
                client = await self._get_client()
                count_keys = []

                async for key in client.scan_iter(match="nlq:count:*"):
                    count_keys.append(key)

                queries_with_counts = []
                for count_key in count_keys:
                    count = await client.get(count_key)
                    if count:
                        normalized = count_key.replace("nlq:count:", "")
                        cache_key = f"nlq:cache:{normalized}"
                        cache_data = await client.get(cache_key)

                        if cache_data:
                            data = json.loads(cache_data)
                            queries_with_counts.append({
                                "original_query": data.get("original_query", normalized),
                                "normalized_query": normalized,
                                "count": int(count)
                            })

                queries_with_counts.sort(key=lambda x: x["count"], reverse=True)
                return queries_with_counts[:limit]

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Failed to get popular queries: {e}")
                return []

    async def get_query_variations(self, nl_query: str) -> list[str]:
        """Get cached variations of a query."""
        normalized = QueryNormalizer.normalize(nl_query)
        client = await self._get_client()

        variations = []
        async for key in client.scan_iter(match=f"nlq:cache:*{normalized}*"):
            variations.append(key.replace("nlq:cache:", ""))

        return variations

    async def health_check(self) -> bool:
        """Check if Redis is available."""
        try:
            client = await self._get_client()
            await client.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            return False

    async def precache_from_file(
        self,
        file_path: str,
        ttl: Optional[int] = None
    ) -> dict[str, Any]:
        """Pre-cache natural language to SPARQL mappings from a file."""
        with tracer.start_as_current_span("precache_from_file") as span:
            span.set_attribute("file_path", file_path)

            stats = {
                "total_queries": 0,
                "cached_successfully": 0,
                "failed": 0,
                "skipped_duplicates": 0,
                "errors": []
            }

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                queries = QueryFileParser.parse(content)
                stats["total_queries"] = len(queries)

                client = await self._get_client()
                ttl_value = ttl or self.ttl

                for nl_query, sparql_query in queries:
                    try:
                        cache_key = self._make_cache_key(nl_query)

                        if await client.exists(cache_key):
                            stats["skipped_duplicates"] += 1
                            continue

                        success = await self.cache_query(nl_query, sparql_query, ttl_value)

                        if success:
                            cached_data = await client.get(cache_key)
                            if cached_data:
                                data = json.loads(cached_data)
                                data["precached"] = True
                                await client.setex(cache_key, ttl_value, json.dumps(data))

                            stats["cached_successfully"] += 1
                        else:
                            stats["failed"] += 1
                            stats["errors"].append(f"cache_query returned False for: {nl_query}...")

                    except Exception as e:
                        stats["failed"] += 1
                        error_msg = f"Failed to cache '{nl_query}...': {str(e)}"
                        stats["errors"].append(error_msg)
                        logger.error(error_msg, exc_info=True)

                logger.info(
                    f"Pre-caching completed: {stats['cached_successfully']} cached, "
                    f"{stats['failed']} failed, {stats['skipped_duplicates']} skipped"
                )

                return stats

            except Exception as e:
                error_msg = f"Error during pre-caching: {str(e)}"
                span.set_attribute("error", error_msg)
                logger.error(error_msg, exc_info=True)
                stats["errors"].append(error_msg)
                return stats


# Global client instance
_redis_client: Optional[RedisClient] = None


def get_redis_client() -> RedisClient:
    """Get or create global Redis client instance."""
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisClient()
    return _redis_client


async def cleanup_redis_client():
    """Cleanup global Redis client."""
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        _redis_client = None