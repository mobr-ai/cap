"""
Redis client for caching SPARQL queries.
"""
import json
import os
from typing import Optional, Any

import redis.asyncio as redis

class RedisSPARQLClient:
    """Client for Redis SPARQL caching operations."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 6379,
        db: int = 0,
        ttl: int = 60 * 6
    ):
        """Initialize Redis client."""
        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", port))
        self.db = db
        self.ttl = ttl
        self._client: Optional[redis.Redis] = None

    async def _get_sparql_client(self) -> redis.Redis:
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

    def _make_cache_key(self, normalized_nl: str) -> str:
        """Create cache key from normalized natural language query."""
        return f"sparql:cache:{normalized_nl}"

    def _make_count_key(self, normalized_nl: str) -> str:
        """Create count key from normalized natural language query."""
        return f"sparql:count:{normalized_nl}"

    async def cache_query(
        self,
        sparql_query: str,
        results: dict,
        ttl: Optional[int] = None
    ) -> int:
        """Cache query with placeholder normalization."""
        try:
            client = await self._get_sparql_client()
            cache_key = self._make_cache_key(sparql_query)

            # Checking if query already exists
            if await client.exists(cache_key):
                return 0  # Indicates duplicate, not cached

            count_key = self._make_count_key(sparql_query)

            cache_data = {
                "sparql_query": sparql_query,
                "results": results
            }

            ttl_value = ttl or self.ttl
            await client.setex(cache_key, ttl_value, json.dumps(cache_data))
            await client.incr(count_key)
            await client.expire(count_key, ttl_value)

            return 1  # Successfully cached

        except Exception as e:
            return -1  # cache error

    async def get_cached_results(
        self,
        sparql_query: str
    ) -> Optional[dict[str, Any]]:
        """Retrieve cached query and restore placeholders."""
        try:
            client = await self._get_sparql_client()

            # Try exact normalized match first
            cache_key = self._make_cache_key(sparql_query)
            cached = await client.get(cache_key)

            if not cached:
                return None

            data = json.loads(cached)
            return data

        except Exception as e:
            return None

    async def get_query_count(self, sparql_query: str) -> int:
        """Get the number of times a query has been asked."""
        try:
            client = await self._get_sparql_client()
            count_key = self._make_count_key(sparql_query)
            count = await client.get(count_key)
            return int(count) if count else 0
        except Exception as e:
            return 0

    async def health_check(self) -> bool:
        """Check if Redis is available."""
        try:
            client = await self._get_sparql_client()
            await client.ping()
            return True
        except Exception as e:
            return False

# Global client instance
_redis_sparql_client: Optional[RedisSPARQLClient] = None


def get_redis_sparql_client() -> RedisSPARQLClient:
    """Get or create global Redis client instance."""
    global _redis_sparql_client
    if _redis_sparql_client is None:
        _redis_sparql_client = RedisSPARQLClient()
    return _redis_sparql_client


async def cleanup_redis_sparql_client():
    """Cleanup global Redis client."""
    global _redis_sparql_client
    if _redis_sparql_client:
        await _redis_sparql_client.close()
        _redis_sparql_client = None