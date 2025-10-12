"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import json
import logging
from typing import Optional, Any
import redis.asyncio as redis
from opentelemetry import trace
import os
import re

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class RedisClient:
    """Client for Redis caching operations."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 6379,
        db: int = 0,
        ttl: int = 86400 * 365  # 1 year is the default TTL
    ):
        """
        Initialize Redis client.

        Args:
            host: Redis host (default: localhost)
            port: Redis port
            db: Redis database number
            ttl: Default TTL for cache entries in seconds
        """
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

    async def cache_query(
        self,
        nl_query: str,
        sparql_query: str,
        results: dict[str, Any],
        ttl: Optional[int] = None
    ) -> bool:
        """
        Cache a successful SPARQL query execution.

        Args:
            nl_query: Natural language query (cache key)
            sparql_query: Generated SPARQL query
            results: Query results
            ttl: Time-to-live in seconds (uses default if None)

        Returns:
            True if cached successfully
        """
        with tracer.start_as_current_span("cache_sparql_query") as span:
            span.set_attribute("nl_query", nl_query)

            try:
                client = await self._get_client()
                cache_key = self._make_cache_key(nl_query)
                count_key = self._make_count_key(nl_query)

                # Store query data
                cache_data = {
                    "sparql_query": sparql_query,
                    "results": results
                }

                ttl_value = ttl or self.ttl

                # Cache the query results
                await client.setex(
                    cache_key,
                    ttl_value,
                    json.dumps(cache_data)
                )

                # Increment query count
                await client.incr(count_key)
                # Set TTL on count key if it's new
                await client.expire(count_key, ttl_value)

                span.set_attribute("cached", True)
                logger.debug(f"Cached query: {nl_query[:50]}...")
                return True

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Failed to cache query: {e}")
                return False

    async def get_cached_query(
        self,
        nl_query: str
    ) -> Optional[dict[str, Any]]:
        """
        Retrieve cached query results.

        Args:
            nl_query: Natural language query

        Returns:
            Cached data (sparql_query and results) or None
        """
        with tracer.start_as_current_span("get_cached_query") as span:
            span.set_attribute("nl_query", nl_query)

            try:
                client = await self._get_client()
                cache_key = self._make_cache_key(nl_query)

                cached = await client.get(cache_key)

                if cached:
                    span.set_attribute("cache_hit", True)
                    logger.debug(f"Cache hit for: {nl_query[:50]}...")
                    return json.loads(cached)

                span.set_attribute("cache_hit", False)
                return None

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Failed to retrieve cached query: {e}")
                return None

    async def get_query_count(self, nl_query: str) -> int:
        """
        Get the number of times a query has been asked.

        Args:
            nl_query: Natural language query

        Returns:
            Query count
        """
        try:
            client = await self._get_client()
            count_key = self._make_count_key(nl_query)

            count = await client.get(count_key)
            return int(count) if count else 0

        except Exception as e:
            logger.error(f"Failed to get query count: {e}")
            return 0

    async def get_popular_queries(self, limit: int = 10) -> list[tuple[str, int]]:
        """
        Get most popular queries.

        Args:
            limit: Maximum number of queries to return

        Returns:
            List of (query, count) tuples
        """
        with tracer.start_as_current_span("get_popular_queries") as span:
            span.set_attribute("limit", limit)

            try:
                client = await self._get_client()

                # Get all count keys
                count_keys = []
                async for key in client.scan_iter(match="nlq:count:*"):
                    count_keys.append(key)

                # Get counts for all keys
                queries_with_counts = []
                for key in count_keys:
                    count = await client.get(key)
                    if count:
                        # Extract original query from key
                        nl_query = key.replace("nlq:count:", "")
                        queries_with_counts.append((nl_query, int(count)))

                # Sort by count and return top N
                queries_with_counts.sort(key=lambda x: x[1], reverse=True)
                return queries_with_counts[:limit]

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Failed to get popular queries: {e}")
                return []

    async def health_check(self) -> bool:
        """
        Check if Redis is available.

        Returns:
            True if healthy
        """
        try:
            client = await self._get_client()
            await client.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            return False

    def _make_cache_key(self, nl_query: str) -> str:
        """Create cache key from natural language query."""
        # Normalize the query for consistent caching
        normalized = nl_query.lower().strip()
        return f"nlq:cache:{normalized}"

    def _make_count_key(self, nl_query: str) -> str:
        """Create count key from natural language query."""
        normalized = nl_query.lower().strip()
        return f"nlq:count:{normalized}"

    async def precache_from_file(
        self,
        file_path: str,
        ttl: Optional[int] = None
    ) -> dict[str, Any]:
        """
        Pre-cache natural language to SPARQL mappings from a file.

        File format expected:
        MESSAGE user <natural language query>
        MESSAGE assistant <sparql query>

        Args:
            file_path: Path to the file containing query mappings
            ttl: Time-to-live in seconds (uses default if None)

        Returns:
            Statistics about the pre-caching operation
        """
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

                # Parse the file content
                queries = self._parse_query_file(content)
                stats["total_queries"] = len(queries)

                logger.info(f"Parsed {len(queries)} queries from {file_path}")

                client = await self._get_client()
                ttl_value = ttl or self.ttl

                # Pre-cache each query
                for nl_query, sparql_query in queries:
                    try:
                        cache_key = self._make_cache_key(nl_query)
                        count_key = self._make_count_key(nl_query)

                        # Check if already cached
                        exists = await client.exists(cache_key)
                        if exists:
                            stats["skipped_duplicates"] += 1
                            logger.debug(f"Skipping duplicate: {nl_query[:50]}...")
                            continue

                        # Create cache data structure
                        # Note: We're caching without actual results since we don't execute the query
                        cache_data = {
                            "sparql_query": sparql_query,
                            "results": None,  # No results for pre-cached queries
                            "precached": True
                        }

                        # Cache the query
                        await client.setex(
                            cache_key,
                            ttl_value,
                            json.dumps(cache_data)
                        )

                        # Initialize count to 0 for pre-cached queries
                        await client.set(count_key, 0)
                        await client.expire(count_key, ttl_value)

                        stats["cached_successfully"] += 1
                        logger.debug(f"Pre-cached: {nl_query[:50]}...")

                    except Exception as e:
                        stats["failed"] += 1
                        error_msg = f"Failed to cache '{nl_query[:50]}...': {str(e)}"
                        stats["errors"].append(error_msg)
                        logger.error(error_msg)

                span.set_attribute("cached_successfully", stats["cached_successfully"])
                span.set_attribute("failed", stats["failed"])
                span.set_attribute("skipped_duplicates", stats["skipped_duplicates"])

                logger.info(
                    f"Pre-caching completed: {stats['cached_successfully']} cached, "
                    f"{stats['failed']} failed, {stats['skipped_duplicates']} skipped"
                )

                return stats

            except FileNotFoundError:
                error_msg = f"File not found: {file_path}"
                span.set_attribute("error", error_msg)
                logger.error(error_msg)
                stats["errors"].append(error_msg)
                return stats

            except Exception as e:
                error_msg = f"Error during pre-caching: {str(e)}"
                span.set_attribute("error", error_msg)
                logger.error(error_msg)
                stats["errors"].append(error_msg)
                return stats

    def _parse_query_file(self, content: str) -> list[tuple[str, str]]:
        """
        Parse query file content into (natural_language, sparql) pairs.

        Expected format:
        MESSAGE user <natural language query>
        MESSAGE assistant <sparql query>

        Args:
            content: File content to parse

        Returns:
            List of (natural_language_query, sparql_query) tuples
        """
        queries = []
        lines = content.strip().split('\n')

        current_nl_query = None
        current_sparql_lines = []
        in_sparql = False

        for line in lines:
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            # Check for user message (natural language query)
            if line.startswith('MESSAGE user'):
                # Save previous query if exists
                if current_nl_query and current_sparql_lines:
                    sparql_query = '\n'.join(current_sparql_lines).strip()
                    # Remove surrounding quotes if present
                    sparql_query = self._clean_sparql_from_file(sparql_query)
                    queries.append((current_nl_query, sparql_query))

                # Start new query
                current_nl_query = line.replace('MESSAGE user', '').strip()
                current_sparql_lines = []
                in_sparql = False

            # Check for assistant message (SPARQL query)
            elif line.startswith('MESSAGE assistant'):
                in_sparql = True
                # Check if query starts on same line
                remaining = line.replace('MESSAGE assistant', '').strip()
                if remaining and remaining != '"""':
                    current_sparql_lines.append(remaining)

            # Collect SPARQL query lines
            elif in_sparql:
                # Skip opening/closing triple quotes
                if line == '"""':
                    continue
                current_sparql_lines.append(line)

        # Don't forget the last query
        if current_nl_query and current_sparql_lines:
            sparql_query = '\n'.join(current_sparql_lines).strip()
            sparql_query = self._clean_sparql_from_file(sparql_query)
            queries.append((current_nl_query, sparql_query))

        return queries

    def _clean_sparql_from_file(self, sparql: str) -> str:
        """
        Clean SPARQL query from file content.

        Args:
            sparql: Raw SPARQL query string

        Returns:
            Cleaned SPARQL query
        """
        # Remove surrounding triple quotes if present
        sparql = re.sub(r'^"""', '', sparql)
        sparql = re.sub(r'"""$', '', sparql)

        # Remove extra whitespace
        sparql = sparql.strip()

        return sparql

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