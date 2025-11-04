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
import unicodedata

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

    def _normalize_query(self, query: str) -> str:
        """
        Normalize natural language query for better cache hits.
        Replaces specific values with generic placeholders.
        """
        # Convert to lowercase
        normalized = query.lower()

        # Normalize unicode
        normalized = unicodedata.normalize('NFKD', normalized)
        normalized = normalized.encode('ascii', 'ignore').decode('ascii')

        # Replace "top N" with placeholder (e.g., "top 5", "top 10" -> "top __N__")
        normalized = re.sub(r'\btop\s+\d+\b', 'top __N__', normalized)

        # Replace text numbers with placeholder - ENTIRE phrase
        # "2 billion" -> "__N__", "699 millions" -> "__N__"
        normalized = re.sub(
            r'\b\d+(?:\.\d+)?\s+(?:billion(?:s)?|million(?:s)?|thousand(?:s)?|hundred(?:s)?)\b',
            '__N__',
            normalized,
            flags=re.IGNORECASE
        )

        # Replace token names with placeholder, but exclude common words
        normalized = re.sub(r'\b(ada|snek|hosky|[a-z]{3,10})\b(?=\s+(holder|token|account))',
                        lambda m: '__TOKEN__' if m.group(1) not in ['many', 'much', 'what', 'which', 'have', 'define'] else m.group(1),
                        normalized)

        # Replace formatted numbers first (e.g., 200,000 or 200.000 or 200_000)
        normalized = re.sub(r'\b\d{1,3}(?:[,._]\d{3})+(?:\.\d+)?\b(?!\s*%)', '__N__', normalized)

        # Replace other standalone numbers with placeholder
        normalized = re.sub(r'\b\d+(?:\.\d+)?\b(?!\s*%)', '__N__', normalized)

        # Remove punctuation
        normalized = re.sub(r'[^\w\s]', '', normalized)

        # Normalize whitespace
        normalized = re.sub(r'\s+', ' ', normalized)

        # Remove filler words
        filler_words = {
            'please', 'could', 'can', 'you', 'show', 'me', 'the', 'plot', 'have',
            'what', 'is', 'are', 'was', 'were', 'tell', 'define', 'your',
            'give', 'find', 'get', 'a', 'an', 'of', 'in', 'on', 'draw', 'yours',
            'much', 'many', 'do', 'does', 'how'
        }

        words = normalized.split()
        filtered_words = [w for w in words if w not in filler_words]

        # Keep question words at start
        question_words = {'who', 'what', 'when', 'where', 'why', 'which'}
        question_start = []
        remaining_words = []

        for word in filtered_words:
            if word in question_words and not question_start:
                question_start.append(word)
            else:
                remaining_words.append(word)

        # Sort remaining words for consistency
        remaining_words.sort()

        normalized = ' '.join(question_start + remaining_words)
        return normalized.strip()

    def _make_cache_key(self, nl_query: str, sparql_query: Optional[str] = None) -> str:
        """Create cache key from natural language query only."""
        normalized_nl = self._normalize_query(nl_query)
        return f"nlq:cache:{normalized_nl}"

    def _make_count_key(self, nl_query: str) -> str:
        """Create count key from natural language query."""
        normalized = self._normalize_query(nl_query)
        return f"nlq:count:{normalized}"

    async def get_query_variations(self, nl_query: str) -> list[str]:
        """
        Get cached variations of a query.

        Useful for debugging cache hits.
        """
        normalized = self._normalize_query(nl_query)
        client = await self._get_client()

        variations = []
        async for key in client.scan_iter(match=f"nlq:cache:*{normalized}*"):
            variations.append(key.replace("nlq:cache:", ""))

        return variations

    async def cache_query(
        self,
        nl_query: str,
        sparql_query: str,
        ttl: Optional[int] = None
    ) -> bool:
        """Cache query with placeholder normalization (supports both single and sequential)."""
        with tracer.start_as_current_span("cache_sparql_query") as span:
            span.set_attribute("nl_query", nl_query)

            try:
                client = await self._get_client()
                normalized = self._normalize_query(nl_query)

                # Detect if sequential or single
                try:
                    parsed = json.loads(sparql_query)
                    if isinstance(parsed, list):
                        # Sequential queries - use global placeholder counter
                        normalized_queries = []
                        all_placeholders = {}

                        # Global counters for unique placeholders across all queries
                        pct_counter = 0
                        num_counter = 0
                        str_counter = 0
                        lim_counter = 0
                        uri_counter = 0
                        cur_counter = 0
                        inject_counter = 0

                        for query_info in parsed:
                            # Pass current counters to extraction
                            norm_q, placeholders = self._extract_literals_and_instances(
                                query_info['query'],
                                start_pct_counter=pct_counter,
                                start_num_counter=num_counter,
                                start_str_counter=str_counter,
                                start_lim_counter=lim_counter,
                                start_uri_counter=uri_counter,
                                start_cur_counter=cur_counter,
                                start_inject_counter=inject_counter
                            )

                            # Update counters based on what was extracted
                            for placeholder in placeholders.keys():
                                try:
                                    # Handle INJECT placeholders**
                                    if placeholder.startswith("__INJECT_"):
                                        idx = int(placeholder.replace('__INJECT_', '').replace('__', ''))
                                        inject_counter = max(inject_counter, idx + 1)
                                    elif placeholder.startswith("__PCT_DECIMAL_"):
                                        # Extract index from __PCT_DECIMAL_N__
                                        idx = int(placeholder.replace('__PCT_DECIMAL_', '').replace('__', ''))
                                        pct_counter = max(pct_counter, idx + 1)
                                    elif placeholder.startswith("__PCT_"):
                                        # Extract index from __PCT_N__
                                        idx = int(placeholder.replace('__PCT_', '').replace('__', ''))
                                        pct_counter = max(pct_counter, idx + 1)
                                    elif placeholder.startswith("__NUM_"):
                                        # Extract index from __NUM_N__
                                        idx = int(placeholder.replace('__NUM_', '').replace('__', ''))
                                        num_counter = max(num_counter, idx + 1)
                                    elif placeholder.startswith("__STR_"):
                                        # Extract index from __STR_N__
                                        idx = int(placeholder.replace('__STR_', '').replace('__', ''))
                                        str_counter = max(str_counter, idx + 1)
                                    elif placeholder.startswith("__LIM_"):
                                        # Extract index from __LIM_N__
                                        idx = int(placeholder.replace('__LIM_', '').replace('__', ''))
                                        lim_counter = max(lim_counter, idx + 1)
                                    elif placeholder.startswith("__URI_"):
                                        # Extract index from __URI_N__
                                        idx = int(placeholder.replace('__URI_', '').replace('__', ''))
                                        uri_counter = max(uri_counter, idx + 1)
                                    elif placeholder.startswith("__CUR_"):
                                        # Extract index from __CUR_N__
                                        idx = int(placeholder.replace('__CUR_', '').replace('__', ''))
                                        cur_counter = max(cur_counter, idx + 1)
                                        logger.debug(f"Updated cur_counter to {cur_counter} from {placeholder}")
                                except (AttributeError, ValueError) as e:
                                    logger.warning(f"Failed to parse index from {placeholder}: {e}")

                            all_placeholders.update(placeholders)
                            query_info['query'] = norm_q
                            normalized_queries.append(query_info)

                        normalized_sparql = json.dumps(normalized_queries)
                        placeholder_map = all_placeholders
                    else:
                        # Single query in JSON
                        normalized_sparql, placeholder_map = self._extract_literals_and_instances(sparql_query)
                except (json.JSONDecodeError, TypeError):
                    # Single query as string
                    normalized_sparql, placeholder_map = self._extract_literals_and_instances(sparql_query)

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

    def _restore_placeholders(
        self,
        sparql: str,
        placeholder_map: dict[str, str],
        current_values: dict[str, list[str]]
    ) -> str:
        """
        Restore placeholders with current values from the new query.
        """

        # Extract prefixes to avoid replacing placeholders in them
        prefix_pattern = r'^((?:PREFIX\s+\w+:\s*<[^>]+>\s*)+)'
        prefix_match = re.match(prefix_pattern, sparql, re.MULTILINE | re.IGNORECASE)

        prefixes = ""
        query_body = sparql

        if prefix_match:
            prefixes = prefix_match.group(1).strip()
            query_body = sparql[prefix_match.end():].strip()

        restored = query_body

        # Process each placeholder type in order
        for placeholder in placeholder_map.keys():
            replacement = None

            if placeholder.startswith("__INJECT_"):
                # Get the cached INJECT statement (which may contain nested placeholders)
                inject_template = placeholder_map.get(placeholder, "")

                # Recursively restore any nested placeholders within the INJECT
                replacement = inject_template
                nested_placeholders = re.findall(r'__(?:PCT_DECIMAL|PCT|NUM|STR|LIM|CUR|URI)_\d+__', inject_template)
                for nested_ph in nested_placeholders:
                    nested_replacement = None

                    if nested_ph.startswith("__PCT_DECIMAL_"):
                        if current_values.get("percentages_decimal"):
                            try:
                                idx = int(nested_ph.replace('__PCT_DECIMAL_', '').replace('__', ''))
                                cycle_idx = idx % len(current_values["percentages_decimal"])
                                nested_replacement = current_values["percentages_decimal"][cycle_idx]
                            except ValueError:
                                nested_replacement = placeholder_map.get(nested_ph, "0.01")
                        else:
                            nested_replacement = placeholder_map.get(nested_ph, "0.01")

                    if nested_replacement:
                        replacement = replacement.replace(nested_ph, nested_replacement)
                        logger.debug(f"Restored nested {nested_ph} in INJECT with: {nested_replacement}")

                logger.debug(f"Restoring {placeholder} with: {replacement}")

            elif placeholder.startswith("__PCT_DECIMAL_"):
                if current_values.get("percentages_decimal"):
                    try:
                        idx = int(placeholder.replace('__PCT_DECIMAL_', '').replace('__', ''))
                        cycle_idx = idx % len(current_values["percentages_decimal"])
                        replacement = current_values["percentages_decimal"][cycle_idx]
                        logger.debug(f"Restoring {placeholder} with NEW decimal: {replacement}")
                    except ValueError:
                        replacement = placeholder_map.get(placeholder, "0.01")
                        logger.warning(f"Failed to parse index from {placeholder}, using cached: {replacement}")
                else:
                    replacement = placeholder_map.get(placeholder, "0.01")
                    logger.warning(f"No current value for {placeholder}, using cached: {replacement}")

            elif placeholder.startswith("__PCT_"):
                if current_values.get("percentages"):
                    try:
                        idx = int(placeholder.replace('__PCT_', '').replace('__', ''))
                        cycle_idx = idx % len(current_values["percentages"])
                        replacement = current_values["percentages"][cycle_idx]
                        logger.debug(f"Restoring {placeholder} with NEW value: {replacement}")
                    except ValueError:
                        replacement = placeholder_map.get(placeholder, "1")
                else:
                    replacement = placeholder_map.get(placeholder, "1")
                    logger.warning(f"No current value for {placeholder}, using cached: {replacement}")

            elif placeholder.startswith("__NUM_"):
                if current_values.get("numbers"):
                    try:
                        idx = int(placeholder.replace('__NUM_', '').replace('__', ''))
                        cycle_idx = idx % len(current_values["numbers"])
                        replacement = current_values["numbers"][cycle_idx]
                        logger.debug(f"Restoring {placeholder} with NEW value: {replacement}")
                    except ValueError:
                        replacement = placeholder_map.get(placeholder, "1")
                        logger.warning(f"Failed to parse index from {placeholder}, using cached: {replacement}")
                else:
                    replacement = placeholder_map.get(placeholder, "1")
                    logger.warning(f"No current value for {placeholder}, using cached: {replacement}")

            elif placeholder.startswith("__STR_"):
                if current_values.get("tokens"):
                    try:
                        idx = int(placeholder.replace('__STR_', '').replace('__', ''))
                        cycle_idx = idx % len(current_values["tokens"])
                        token = current_values["tokens"][cycle_idx]
                        original = placeholder_map.get(placeholder, '""')
                        quote_char = original[0] if original and original[0] in ['"', "'"] else '"'
                        replacement = f'{quote_char}{token}{quote_char}'
                        logger.debug(f"Restoring {placeholder} with NEW token: {replacement}")
                    except ValueError:
                        replacement = placeholder_map.get(placeholder, '""')
                else:
                    replacement = placeholder_map.get(placeholder, '""')
                    logger.warning(f"No current value for {placeholder}, using cached: {replacement}")

            elif placeholder.startswith("__LIM_"):
                if current_values.get("limits"):
                    try:
                        idx = int(placeholder.replace('__LIM_', '').replace('__', ''))
                        cycle_idx = idx % len(current_values["limits"])
                        replacement = current_values["limits"][cycle_idx]
                        logger.debug(f"Restoring {placeholder} with NEW limit: {replacement}")
                    except ValueError:
                        replacement = placeholder_map.get(placeholder, "10")
                else:
                    replacement = placeholder_map.get(placeholder, "10")
                    logger.warning(f"No current value for {placeholder}, using cached: {replacement}")

            elif placeholder.startswith("__CUR_"):
                if current_values.get("tokens"):
                    try:
                        idx = int(placeholder.replace('__CUR_', '').replace('__', ''))
                        valid_tokens = [t for t in current_values["tokens"] if t.strip()]

                        if valid_tokens:
                            cycle_idx = idx % len(valid_tokens)
                            token = valid_tokens[cycle_idx]
                            # Construct URI - ensure token is lowercase
                            replacement = f'<http://www.mobr.ai/ontologies/cardano#cnt/{token.lower()}>'
                            logger.debug(f"Restoring {placeholder} with NEW currency URI: {replacement}")
                        else:
                            replacement = placeholder_map.get(placeholder, "")
                            logger.warning(f"No valid tokens for {placeholder}, using cached: {replacement}")
                    except (ValueError, IndexError) as e:
                        replacement = placeholder_map.get(placeholder, "")
                        logger.error(f"Failed to restore {placeholder}: {e}, using cached: {replacement}")
                else:
                    replacement = placeholder_map.get(placeholder, "")
                    logger.warning(f"No current tokens for {placeholder}, using cached: {replacement}")

            elif placeholder.startswith("__URI_"):
                replacement = placeholder_map.get(placeholder, "")

            # Use simple string replacement since each placeholder is unique
            # There is no partial matches for placeholders
            if replacement is not None:
                restored = restored.replace(placeholder, replacement)
                logger.debug(f"Replaced {placeholder} with {replacement}")

        # Restore prefixes
        if prefixes:
            restored = prefixes + "\n\n" + restored

        return restored


    def _extract_values_from_nl_query(self, nl_query: str) -> dict[str, list[str]]:
        """
        Extract all actual values from natural language query BEFORE normalization.

        This is CRITICAL for placeholder restoration - these values will replace
        the placeholders in the cached SPARQL with the NEW query's values.
        """
        values = {
            "percentages": [],
            "percentages_decimal": [],
            "limits": [],
            "tokens": [],
            "numbers": []
        }

        # Extract percentages with % symbol
        for match in re.finditer(r'(\d+(?:\.\d+)?)\s*%', nl_query, re.IGNORECASE):
            pct = match.group(1)
            if pct not in values["percentages"]:
                values["percentages"].append(pct)
                decimal = float(pct) / 100
                decimal_str = str(decimal)
                values["percentages_decimal"].append(decimal_str)
                logger.debug(f"Extracted percentage: {pct}% → decimal: {decimal_str}")

        # Extract "N percent" format
        for match in re.finditer(r'(\d+(?:\.\d+)?)\s+percent', nl_query, re.IGNORECASE):
            pct = match.group(1)
            if pct not in values["percentages"]:
                values["percentages"].append(pct)
                decimal = float(pct) / 100
                decimal_str = f"{decimal:.2f}"
                values["percentages_decimal"].append(decimal_str)
                logger.debug(f"Extracted percentage: {pct} percent → decimal: {decimal_str}")

        # Extract decimal percentages (0.01, 0.02, etc.) - these might be standalone
        for match in re.finditer(r'\b(0\.\d+)\b', nl_query):
            decimal = match.group(1)
            decimal_float = float(decimal)
            if 0 < decimal_float < 1.0:  # Likely a percentage decimal
                if decimal not in values["percentages_decimal"]:
                    values["percentages_decimal"].append(decimal)
                    pct = str(decimal_float * 100)
                    if pct.endswith('.0'):
                        pct = pct[:-2]
                    if pct not in values["percentages"]:
                        values["percentages"].append(pct)
                    logger.debug(f"Extracted decimal percentage: {decimal} → {pct}%")

        # Extract "top N" numbers
        for match in re.finditer(r'top\s+(\d+)', nl_query, re.IGNORECASE):
            limit = match.group(1)
            if limit not in values["limits"]:
                values["limits"].append(limit)
                logger.debug(f"Extracted top limit: {limit}")

        # Extract token names - preserve original case
        for match in re.finditer(r'\b([A-Za-z]{3,10})\b\s+(?:(?:(?<!token\s)holder|token(?=\s+state)|account|supply|balance))', nl_query, re.IGNORECASE):
            token = match.group(1)
            token_upper = token.upper()
            # Exclude common question/filler words that might precede keywords
            excluded_words = ['THE', 'FOR', 'TOP', 'MANY', 'MUCH', 'HOW', 'WHAT', 'WHICH', 'ARE', 'DEFINE', "SHOW", "LIST"]
            if token_upper not in values["tokens"] and token_upper not in excluded_words:
                values["tokens"].append(token_upper)
                logger.debug(f"Extracted token: {token_upper}")

        # Extract text-formatted numbers (billion, million, etc.)
        for match in re.finditer(
            r'\b(\d+(?:\.\d+)?)\s+(billion(?:s)?|million(?:s)?|thousand(?:s)?|hundred(?:s)?)\b',
            nl_query,
            re.IGNORECASE
        ):
            num = match.group(1)
            unit = match.group(2).lower()
            if unit.endswith("s"):
                unit = unit[:-1]

            # Convert to actual number
            multipliers = {
                'hundred': 100,
                'thousand': 1000,
                'million': 1000000,
                'billion': 1000000000,
            }

            base_num = float(num)
            actual_value = str(int(base_num * multipliers.get(unit, 1)))

            # Check if context mentions ADA
            context = nl_query[max(0, match.start()-20):min(len(nl_query), match.end()+10)]
            if 'ADA' in context.upper():
                # Convert to lovelace
                lovelace_value = str(int(actual_value) * 1000000)
                if lovelace_value not in values["numbers"]:
                    values["numbers"].append(lovelace_value)
                    logger.debug(f"Extracted text number: {num} {unit} ADA -> lovelace: {lovelace_value}")
            else:
                if actual_value not in values["numbers"]:
                    values["numbers"].append(actual_value)
                    logger.debug(f"Extracted text number: {num} {unit} -> {actual_value}")

        # Extract formatted numbers (e.g., 200,000 or 200.000 or 200_000)
        # This pattern captures numbers with thousand separators
        for match in re.finditer(r'\b\d{1,3}(?:[,._]\d{3})+(?:\.\d+)?\b', nl_query):
            num = match.group(0)
            # Normalize: remove separators to get raw number
            normalized_num = re.sub(r'[,._]', '', num)
            if (normalized_num not in values["limits"] and
                normalized_num not in values["percentages"] and
                normalized_num not in values["percentages_decimal"]):
                if normalized_num not in values["numbers"]:
                    # Convert ADA amounts to lovelace (multiply by 1,000,000)
                    # Check context to see if this is an ADA amount
                    context = nl_query[max(0, match.start()-20):min(len(nl_query), match.end()+10)]
                    if 'ADA' in context.upper():
                        lovelace_value = str(int(normalized_num) * 1000000)
                        values["numbers"].append(lovelace_value)
                        logger.debug(f"Extracted ADA number: {num} -> {normalized_num} -> lovelace: {lovelace_value}")
                    else:
                        values["numbers"].append(normalized_num)
                        logger.debug(f"Extracted number: {num} -> normalized: {normalized_num}")

        # Extract simple numbers (no separators)
        for match in re.finditer(r'\b\d+(?:\.\d+)?\b', nl_query):
            num = match.group(0)
            # Skip if it's part of a formatted number we already extracted
            if re.search(r'\b\d{1,3}[,._]\d', nl_query[max(0, match.start()-1):match.end()+2]):
                continue
            if (num not in values["limits"] and
                num not in values["percentages"] and
                num not in values["percentages_decimal"]):
                if num not in values["numbers"]:
                    values["numbers"].append(num)
                    logger.debug(f"Extracted number: {num}")

        logger.info(f"Extracted values from '{nl_query}': {values}")
        return values

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

    async def get_popular_queries(self, limit: int = 5) -> list[tuple[str, int]]:
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
                for count_key in count_keys:
                    count = await client.get(count_key)
                    if count:
                        # Get corresponding cache key to retrieve original query
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

                # Sort by count and return top N
                queries_with_counts.sort(key=lambda x: x["count"], reverse=True)
                return queries_with_counts[:limit]

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Failed to get popular queries: {e}")
                return []

    def _extract_literals_and_instances(
        self,
        sparql_query: str,
        start_pct_counter: int = 0,
        start_num_counter: int = 0,
        start_str_counter: int = 0,
        start_lim_counter: int = 0,
        start_uri_counter: int = 0,
        start_cur_counter: int = 0,
        start_inject_counter: int = 0
    ) -> tuple[str, dict[str, str]]:
        """
        Extract literals and instances from SPARQL, replace with typed placeholders.
        Returns: (normalized_sparql, placeholder_map)
        """

        # ORDER of the extraction steps:
        # 1. Extract INJECT statements FIRST
        # 2. Extract currency URIs SECOND
        # 3. Extract percentages
        # 4. Extract string literals
        # 5. Extract LIMIT/OFFSET
        # 6. Extract other URIs
        # 7. Extract formatted numbers
        # 8. Extract plain numbers

        placeholder_map = {}

        # Extract and store prefixes FIRST - these should NEVER be normalized
        prefix_pattern = r'^((?:PREFIX\s+\w+:\s*<[^>]+>\s*)+)'
        prefix_match = re.match(prefix_pattern, sparql_query, re.MULTILINE | re.IGNORECASE)

        prefixes = ""
        query_body = sparql_query

        if prefix_match:
            prefixes = prefix_match.group(1).strip()
            query_body = sparql_query[prefix_match.end():].strip()
            logger.debug(f"Extracted {len(prefixes.splitlines())} PREFIX lines - these will be preserved as-is")

        # Now work ONLY on query_body, not the prefixes
        normalized = query_body

        # Use passed-in counters instead of starting at 0
        num_counter = start_num_counter
        pct_counter = start_pct_counter
        str_counter = start_str_counter
        lim_counter = start_lim_counter
        uri_counter = start_uri_counter
        cur_counter = start_cur_counter
        inject_counter = start_inject_counter

        # Extract INJECT statements FIRST before any other placeholders
        # This prevents nested placeholders inside INJECT expressions
        def extract_inject_statements(text):
            nonlocal inject_counter, pct_counter
            result = text
            changes = []

            pattern = r'INJECT(?:_FROM_PREVIOUS)?\('
            pos = 0

            while True:
                match = re.search(pattern, result[pos:], re.IGNORECASE)
                if not match:
                    break

                start = pos + match.start()
                paren_count = 1
                i = start + len(match.group(0))

                # Find matching closing parenthesis
                while i < len(result) and paren_count > 0:
                    if result[i] == '(':
                        paren_count += 1
                    elif result[i] == ')':
                        paren_count -= 1
                    i += 1

                if paren_count == 0:
                    original = result[start:i]
                    placeholder = f"__INJECT_{inject_counter}__"
                    inject_counter += 1

                    # Store the INJECT statement but parameterize percentage decimals inside it
                    parameterized_inject = original
                    # Find percentage decimals like 0.01, 0.02 inside the INJECT
                    pct_decimal_matches = list(re.finditer(r'\b(0\.\d+)\b', original))
                    for match in reversed(pct_decimal_matches):  # Reverse to maintain positions
                        decimal_val = match.group(1)
                        decimal_float = float(decimal_val)
                        if 0 < decimal_float < 1.0:  # It's a percentage decimal
                            pct_placeholder = f"__PCT_DECIMAL_{pct_counter}__"
                            pct_counter += 1
                            start_pos = match.start()
                            end_pos = match.end()
                            parameterized_inject = (parameterized_inject[:start_pos] +
                                                pct_placeholder +
                                                parameterized_inject[end_pos:])
                            placeholder_map[pct_placeholder] = decimal_val
                            logger.debug(f"Parameterized {decimal_val} as {pct_placeholder} inside INJECT")

                    placeholder_map[placeholder] = parameterized_inject

                    changes.append((start, i, placeholder, original))
                    logger.debug(f"Extracted INJECT statement: {placeholder} = {original}")
                    pos = i
                else:
                    break

            # Apply all changes from right to left to preserve positions
            for start_pos, end_pos, placeholder, original in sorted(changes, reverse=True):
                result = result[:start_pos] + placeholder + result[end_pos:]

            return result

        # 1. Extract INJECT statements FIRST, before ANY other extraction
        normalized = extract_inject_statements(normalized)

        # 2. Extract currency URIs separately
        currency_pattern = r'<http://www\.mobr\.ai/ontologies/cardano#cnt/[^>]+>|cardano:cnt/\w+'
        currency_matches = list(re.finditer(currency_pattern, normalized))
        for match in reversed(currency_matches):  # Process right to left
            original = match.group(0)
            # Normalize to full URI format if short form
            if not original.startswith('<'):
                full_uri = f'<http://www.mobr.ai/ontologies/cardano#{original}>'
            else:
                full_uri = original

            # Skip if already replaced
            if normalized[match.start():match.end()].startswith('__CUR_'):
                continue

            placeholder = f"__CUR_{cur_counter}__"
            cur_counter += 1
            placeholder_map[placeholder] = full_uri
            # Use position-based replacement instead of string replace
            normalized = normalized[:match.start()] + placeholder + normalized[match.end():]
            logger.debug(f"Extracted currency: {match.group(0)} -> {full_uri} as {placeholder}")

        # 3. Extract percentages (e.g., "1%", "0.01") - outside INJECT blocks
        pct_pattern = r'(\d+(?:\.\d+)?)\s*%|0\.\d+'
        for match in re.finditer(pct_pattern, normalized):
            original = match.group(0)
            # Skip if already a placeholder
            if original.startswith('__'):
                continue
            placeholder = f"__PCT_{pct_counter}__"
            pct_counter += 1
            placeholder_map[placeholder] = original
            normalized = normalized.replace(original, placeholder, 1)

        # 4. Extract string literals (tokens, policy IDs, etc.) - outside INJECT blocks
        string_pattern = r'["\']([^"\']+)["\']'
        for match in re.finditer(string_pattern, normalized):
            original = match.group(0)
            # Skip if already a placeholder
            if '__STR_' in original:
                continue
            placeholder = f"__STR_{str_counter}__"
            str_counter += 1
            placeholder_map[placeholder] = original
            normalized = normalized.replace(original, placeholder, 1)

        # 5. Extract LIMIT/OFFSET numbers - outside INJECT blocks
        limit_pattern = r'(LIMIT|OFFSET)\s+(\d+)'
        for match in re.finditer(limit_pattern, normalized, re.IGNORECASE):
            original_num = match.group(2)
            # Skip if already a placeholder
            if original_num.startswith('__'):
                continue
            placeholder = f"__LIM_{lim_counter}__"
            lim_counter += 1
            placeholder_map[placeholder] = original_num
            normalized = re.sub(
                f'{match.group(1)}\\s+{re.escape(original_num)}',
                f'{match.group(1)} {placeholder}',
                normalized,
                count=1,
                flags=re.IGNORECASE
            )

        # 6. Extract URIs (addresses, assets, etc.)
        uri_pattern = r'(cardano:(?:addr|asset|stake|pool|tx)[a-zA-Z0-9]+)'
        for match in re.finditer(uri_pattern, normalized):
            original = match.group(0)
            # Skip if already a placeholder
            if original.startswith('__'):
                continue
            placeholder = f"__URI_{uri_counter}__"
            uri_counter += 1
            placeholder_map[placeholder] = original
            normalized = normalized.replace(original, placeholder, 1)

        # 7. Extract remaining formatted numbers that aren't placeholders yet
        formatted_num_pattern = r'\b\d{1,3}(?:[,._]\d{3})+(?:\.\d+)?\b'
        for match in re.finditer(formatted_num_pattern, normalized):
            original = match.group(0)
            # Get surrounding context
            context_start = max(0, match.start() - 15)
            context_end = min(len(normalized), match.end() + 15)
            context = normalized[context_start:context_end]

            # Skip if already a placeholder, inside URL, or part of namespace
            if (original.startswith('__') or
                '__NUM_' in normalized[max(0, match.start()-10):match.end()+10] or
                '<http' in context or
                '://' in context or
                'XMLSchema' in context or
                '.w3.org' in context):
                logger.debug(f"Skipping formatted number {original} - inside URL or already placeholder")
                continue

            # Normalize the number (remove separators)
            cleaned_num = re.sub(r'[,._]', '', original)
            placeholder = f"__NUM_{num_counter}__"
            num_counter += 1
            placeholder_map[placeholder] = cleaned_num  # Store normalized version
            normalized = normalized.replace(original, placeholder, 1)
            logger.debug(f"Extracted formatted number: {original} -> {cleaned_num} as {placeholder}")

        # 8. Extract plain numbers (e.g., in HAVING clauses)
        plain_num_pattern = r'\b\d{1,}\b'
        for match in re.finditer(plain_num_pattern, normalized):
            original = match.group(0)

            # Get context around the match to check if it's part of a URL or prefix
            context_start = max(0, match.start() - 20)
            context_end = min(len(normalized), match.end() + 20)
            context = normalized[context_start:context_end]

            # Skip if:
            # - Already a placeholder
            # - Part of another placeholder
            # - Inside a URL/URI (has :// or common URL patterns)
            # - Inside XML namespace declarations
            if (original.startswith('__') or
                '__NUM_' in normalized[max(0, match.start()-10):match.end()+10] or
                '://' in context or
                '<http' in context or
                'www.' in context or
                '.org' in context or
                '.com' in context or
                'XMLSchema' in context or
                '/ontologies/' in context):
                logger.debug(f"Skipping plain number {original} - inside URL/URI or already placeholder")
                continue

            placeholder = f"__NUM_{num_counter}__"
            num_counter += 1
            placeholder_map[placeholder] = original
            normalized = normalized.replace(original, placeholder, 1)
            logger.debug(f"Extracted plain number: {original} as {placeholder}")

        if prefixes:
            normalized = prefixes + "\n\n" + normalized
            logger.debug("Restored PREFIX declarations to normalized query")

        return normalized, placeholder_map


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

    async def precache_from_file(
        self,
        file_path: str,
        ttl: Optional[int] = None
    ) -> dict[str, Any]:
        """
        Pre-cache natural language to SPARQL mappings from a file.
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

                queries = self._parse_query_file(content)
                stats["total_queries"] = len(queries)

                logger.info(f"Parsed {len(queries)} queries from {file_path}")

                client = await self._get_client()
                ttl_value = ttl or self.ttl

                for nl_query, sparql_query in queries:
                    try:
                        cache_key = self._make_cache_key(nl_query)

                        # Check for duplicates
                        exists = await client.exists(cache_key)
                        if exists:
                            stats["skipped_duplicates"] += 1
                            logger.debug(f"Skipping duplicate: {nl_query}...")
                            continue

                        # IMPORTANT: _clean_sparql_from_file already returns proper format
                        # Either JSON string for sequential or plain SPARQL for single
                        # We need to detect which one and handle accordingly

                        # Use cache_query which handles both formats
                        success = await self.cache_query(
                            nl_query=nl_query,
                            sparql_query=sparql_query,
                            ttl=ttl_value
                        )

                        if success:
                            # Update the precached flag
                            cached_data = await client.get(cache_key)
                            if cached_data:
                                data = json.loads(cached_data)
                                data["precached"] = True
                                await client.setex(cache_key, ttl_value, json.dumps(data))

                            stats["cached_successfully"] += 1
                            logger.debug(f"Pre-cached: {nl_query}...")
                        else:
                            stats["failed"] += 1
                            error_msg = f"cache_query returned False for: {nl_query}..."
                            stats["errors"].append(error_msg)
                            logger.error(error_msg)

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

    async def get_cached_query_with_original(
        self,
        normalized_query: str,
        original_query: str
    ) -> Optional[dict[str, Any]]:
        """
        Retrieve cached query and restore placeholders using original query for value extraction.

        Args:
            normalized_query: Lowercased/normalized query for cache key lookup
            original_query: Original query with proper casing for value extraction
        """
        with tracer.start_as_current_span("get_cached_query_with_original") as span:
            try:
                client = await self._get_client()
                cache_key = self._make_cache_key(normalized_query)

                cached = await client.get(cache_key)
                if not cached:
                    span.set_attribute("cache_hit", False)
                    return None

                data = json.loads(cached)

                # Extract values from ORIGINAL query, not normalized
                current_values = self._extract_values_from_nl_query(original_query)
                logger.info(f"Original query used for extraction: '{original_query}'")
                logger.info(f"Normalized query for cache key: '{normalized_query}'")

                placeholder_map = data.get("placeholder_map", {})

                logger.info(f"Cache key: {cache_key}")
                logger.info(f"Extracted values from original query: {current_values}")
                logger.info(f"Placeholder map has {len(placeholder_map)} entries")

                if not placeholder_map:
                    span.set_attribute("cache_hit", True)
                    return data

                # Restore placeholders in SPARQL
                restored_sparql = data["sparql_query"]
                logger.info(f"Restored cached sparql: {restored_sparql}")

                try:
                    # Try to parse as JSON first
                    parsed = json.loads(restored_sparql)
                    if isinstance(parsed, list):
                        # Sequential queries - restore each query separately
                        for idx, query_info in enumerate(parsed):
                            original_query_text = query_info['query']
                            restored_query_text = self._restore_placeholders(
                                original_query_text,
                                placeholder_map,
                                current_values
                            )
                            query_info['query'] = restored_query_text

                            # Verify restoration
                            remaining = re.findall(r'__(?:PCT|NUM|STR|LIM|CUR|URI|INJECT)_(?:DECIMAL_)?\d+__', restored_query_text)
                            if remaining:
                                logger.error(f"Query {idx+1} still has placeholders: {remaining}")
                                # CRITICAL: Try one more restoration pass for missed placeholders
                                restored_query_text = self._restore_placeholders(
                                    restored_query_text,
                                    placeholder_map,
                                    current_values
                                )
                                query_info['query'] = restored_query_text

                                # Check again
                                remaining_after_retry = re.findall(r'__(?:PCT|NUM|STR|LIM|CUR|URI|INJECT)_(?:DECIMAL_)?\d+__', restored_query_text)
                                if remaining_after_retry:
                                    logger.error(f"Query {idx+1} STILL has placeholders after retry: {remaining_after_retry}")
                            else:
                                logger.info(f"Query {idx+1} fully restored - no placeholders remaining")

                        # Serialize back to JSON string
                        data["sparql_query"] = json.dumps(parsed)
                        logger.info(f"Parsed sequential sparql query {data['sparql_query']}")
                    else:
                        # Single query stored as JSON object
                        data["sparql_query"] = self._restore_placeholders(
                            restored_sparql,
                            placeholder_map,
                            current_values
                        )

                except (json.JSONDecodeError, TypeError):
                    # Plain SPARQL string
                    data["sparql_query"] = self._restore_placeholders(
                        restored_sparql,
                        placeholder_map,
                        current_values
                    )

                span.set_attribute("cache_hit", True)
                return data

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Failed to retrieve cached query: {e}", exc_info=True)
                return None

    def _parse_query_file(self, content: str) -> list[tuple[str, str]]:
        """
        Parse query file content into (natural_language, sparql) pairs.

        Supports both single SPARQL queries and sequential queries with ---split--- format.

        Expected format:
        MESSAGE user <natural language query>
        MESSAGE assistant <sparql query or sequential queries>

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
        in_triple_quotes = False

        for line in lines:
            # Don't strip yet - we need to preserve SPARQL formatting

            # Skip completely empty lines outside of SPARQL blocks
            if not line.strip() and not in_sparql:
                continue

            # Check for user message (natural language query)
            if line.strip().startswith('MESSAGE user'):
                # Save previous query if exists
                if current_nl_query and current_sparql_lines:
                    sparql_query = '\n'.join(current_sparql_lines).strip()
                    sparql_query = self._clean_sparql_from_file(sparql_query)
                    queries.append((current_nl_query, sparql_query))

                # Start new query
                current_nl_query = line.strip().replace('MESSAGE user', '').strip()
                current_sparql_lines = []
                in_sparql = False
                in_triple_quotes = False

            # Check for assistant message (SPARQL query)
            elif line.strip().startswith('MESSAGE assistant'):
                in_sparql = True
                remaining = line.strip().replace('MESSAGE assistant', '').strip()

                # Check if triple quotes start on same line
                if remaining == '"""':
                    in_triple_quotes = True
                elif remaining:
                    # SPARQL might start immediately without quotes
                    current_sparql_lines.append(remaining)

            # Collect SPARQL query lines
            elif in_sparql:
                stripped = line.strip()

                # Handle triple quotes
                if stripped == '"""':
                    if in_triple_quotes:
                        # Closing quotes - end of this assistant message
                        in_triple_quotes = False
                        in_sparql = False
                    else:
                        # Opening quotes
                        in_triple_quotes = True
                    continue

                # preserve original formatting for SPARQL
                if in_triple_quotes or not stripped.startswith('MESSAGE'):
                    current_sparql_lines.append(line.rstrip())  # Keep indentation

        # Don't forget the last query
        if current_nl_query and current_sparql_lines:
            sparql_query = '\n'.join(current_sparql_lines).strip()
            sparql_query = self._clean_sparql_from_file(sparql_query)
            queries.append((current_nl_query, sparql_query))

        logger.info(f"Parsed {len(queries)} query pairs from file")
        return queries

    def _clean_sparql_from_file(self, sparql: str) -> str:
        """Clean and normalize SPARQL from file (supports both single and sequential)."""
        sparql = sparql.strip()

        # Remove triple quotes
        if sparql.startswith('"""') and sparql.endswith('"""'):
            sparql = sparql[3:-3].strip()

        # Check if sequential (has split markers)
        if '---split' in sparql or '---query' in sparql:
            queries = []
            parts = re.split(r'---query\s+\d+[^-]*---', sparql)

            for part in parts[1:]:
                part = part.strip()
                if not part or part.startswith('---'):
                    continue

                # Remove split markers
                part = re.sub(r'---split[^-]*---', '', part).strip()

                queries.append({
                    'query': part,
                    'inject_params': [] # Always empty, INJECTs are in placeholders
                })

            logger.info(f"Parsed sequential SPARQL with {len(queries)} queries")
            return json.dumps(queries)

        return sparql

    def _parse_sequential_sparql_from_cache(self, sparql_text: str) -> list[dict[str, Any]]:
        """
        Parse sequential SPARQL from cache file format.

        Expected format:
        ---split in two queries---
        ---query 1 description---
        SPARQL QUERY 1

        ---query 2 description---
        SPARQL QUERY 2
        """
        queries = []

        # Split by query markers
        # Match both "---query N ..." and just "---query N---"
        parts = re.split(r'---query\s+\d+[^-]*---', sparql_text)

        # Skip first part (usually the split declaration)
        for part in parts[1:]:
            part = part.strip()
            if not part or part.startswith('---'):
                continue

            # Remove any remaining split markers
            part = re.sub(r'---split[^-]*---', '', part).strip()

            # Extract injection parameters from the query
            inject_pattern = r'INJECT\([^)]+\)'
            inject_matches = re.findall(inject_pattern, part)

            queries.append({
                'query': part,
                'inject_params': inject_matches
            })

        logger.debug(f"Parsed {len(queries)} sequential queries from cache")
        return queries

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