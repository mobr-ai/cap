"""
Ollama client for interacting with ollama.
"""
import os
import logging
import json
import re
from typing import AsyncIterator, Optional, Any, Union
import httpx
from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class OllamaClient:
    """Client for interacting with Ollama LLM service."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        llm_model: str = None,
        timeout: float = 120.0
    ):
        """
        Initialize Ollama client.

        Args:
            base_url: Ollama API base URL (default: http://localhost:11434)
            llm_model: Model for converting NL to SPARQL
            timeout: Request timeout in seconds
        """
        self.base_url = (base_url or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")).rstrip("/")
        self.llm_model = (llm_model or os.getenv("OLLAMA_MODEL_NAME", "mobr/cap"))
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    def _load_prompt(self, env_key: str, default: str = "") -> str:
        """Load prompt from environment, refreshed on each call."""
        return os.getenv(env_key, default)

    @property
    def nl_to_sparql_prompt(self) -> str:
        """Get NL to SPARQL prompt (refreshed from env)."""
        return self._load_prompt(
            "NL_TO_SPARQL_PROMPT",
            "Convert the following natural language query to SPARQL for Cardano blockchain data."
        )

    @property
    def contextualize_prompt(self) -> str:
        """Get contextualization prompt (refreshed from env)."""
        return self._load_prompt(
            "CONTEXTUALIZE_PROMPT",
            "Based on the query results, provide a clear and helpful answer."
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def nl_to_sequential_sparql(
        self,
        natural_query: str
    ) -> list[dict[str, Any]]:
        """
        Convert natural language query to sequential SPARQL queries.

        Returns:
            List of query dictionaries with 'query' and 'inject_params' keys
        """
        with tracer.start_as_current_span("nl_to_sequential_sparql") as span:
            span.set_attribute("query", natural_query)

            sparql_response = await self.generate_complete(
                prompt=natural_query,
                model=self.llm_model,
                temperature=0.0
            )

            # Parse sequential queries
            queries = self._parse_sequential_sparql(sparql_response)
            span.set_attribute("query_count", len(queries))

            return queries

    def _parse_sequential_sparql(self, sparql_text: str) -> list[dict[str, Any]]:
        """
        Parse sequential SPARQL queries from LLM response.

        Expected format:
        ---query sequence 1: description---
        SPARQL query 1
        ---query sequence 2: description---
        SPARQL query 2 with INJECT_FROM_PREVIOUS(expression)
        """
        queries = []

        # Split by query sequence markers
        parts = re.split(r'---query sequence \d+:.*?---', sparql_text)

        for part in parts[1:]:  # Skip first empty part
            cleaned = self._clean_sparql(part)
            if not cleaned:
                continue
            cleaned = self._ensure_prefixes(cleaned)

            # Check for injection parameters
            inject_pattern = r'INJECT_FROM_PREVIOUS\((.*?)\)'
            inject_matches = re.findall(inject_pattern, cleaned)

            queries.append({
                'query': cleaned,
                'inject_params': inject_matches
            })

        return queries

    def detect_and_parse_sparql(self, sparql_text: str) -> tuple[bool, Union[str, list[dict[str, Any]]]]:
        """
        Detect if the SPARQL text contains sequential queries and parse accordingly.

        Returns:
            Tuple of (is_sequential: bool, content: str or list[dict])
        """
        # Check for sequential markers
        if re.search(r'---query sequence \d+:.*?---', sparql_text, re.IGNORECASE | re.DOTALL):
            queries = self._parse_sequential_sparql(sparql_text)
            return len(queries) > 0, queries  # True if parsed successfully
        else:
            cleaned = self._clean_sparql(sparql_text)
            cleaned = self._ensure_prefixes(cleaned)
            return False, cleaned

    async def generate_stream(
        self,
        prompt: str,
        model: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.1
    ) -> AsyncIterator[str]:
        """
        Generate streaming response from Ollama.

        Args:
            prompt: User's input prompt
            model: Model name to use
            system_prompt: Optional system prompt for context
            temperature: Sampling temperature (0.0-1.0, lower = more deterministic)

        Yields:
            Chunks of generated text
        """
        with tracer.start_as_current_span("ollama_generate_stream") as span:
            client = await self._get_client()

            request_data = {
                "model": model,
                "prompt": prompt,
                "stream": True,
                "options": {
                    "temperature": temperature
                }
            }

            if system_prompt:
                request_data["system"] = system_prompt

            try:
                async with client.stream(
                    "POST",
                    f"{self.base_url}/api/generate",
                    json=request_data,
                    timeout=None
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if not line:
                            continue

                        try:
                            chunk = json.loads(line)

                            if "response" in chunk:
                                yield chunk["response"]

                            if chunk.get("done", False):
                                break

                        except json.JSONDecodeError:
                            logger.warning(f"Failed to decode JSON: {line}")
                            continue

            except httpx.HTTPStatusError as e:
                logger.error(f"Ollama HTTP error: {e}")
                span.set_attribute("error", str(e))
                raise

            except Exception as e:
                logger.error(f"Ollama streaming error: {e}")
                span.set_attribute("error", str(e))
                raise

    async def generate_complete(
        self,
        prompt: str,
        model: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.1
    ) -> str:
        """
        Generate complete (non-streaming) response from Ollama.

        Args:
            prompt: User's input prompt
            model: Model name to use
            system_prompt: Optional system prompt for context
            temperature: Sampling temperature

        Returns:
            Complete generated text
        """
        with tracer.start_as_current_span("ollama_generate_complete") as span:
            span.set_attribute("model", model)
            span.set_attribute("prompt_length", len(prompt))

            client = await self._get_client()

            request_data = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature
                }
            }

            if system_prompt:
                request_data["system"] = system_prompt

            try:
                response = await client.post(
                    f"{self.base_url}/api/generate",
                    json=request_data
                )
                response.raise_for_status()

                result = response.json()
                generated_text = result.get("response", "")

                span.set_attribute("response_length", len(generated_text))
                return generated_text

            except httpx.HTTPStatusError as e:
                span.set_attribute("error", str(e))
                logger.error(f"Ollama HTTP error: {e}")
                raise

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Ollama generation error: {e}")
                raise

    async def nl_to_sparql(
        self,
        natural_query: str
    ) -> str:
        """Convert natural language query to SPARQL."""
        with tracer.start_as_current_span("nl_to_sparql") as span:
            span.set_attribute("query", natural_query)

            # Use fresh prompt from environment
            system_prompt = ""
            nl_prompt = f"""
                {self.nl_to_sparql_prompt}
                User Question: {natural_query}
            """

            sparql_response = await self.generate_complete(
                prompt=nl_prompt,
                model=self.llm_model,
                system_prompt=system_prompt,
                temperature=0.0
            )

            is_sequential, content = self.detect_and_parse_sparql(sparql_response)
            if is_sequential:
                # For backward compatibility, return first query if sequential (or raise/log)
                logger.warning("Sequential SPARQL detected in single nl_to_sparql call; using first query")
                return content[0]['query'] if content else ""
            else:
                span.set_attribute("sparql_length", len(content))
                return content

    def _clean_sparql(self, sparql_text: str) -> str:
        """
        Clean and extract SPARQL query from LLM response.

        Args:
            sparql_text: Raw text from LLM

        Returns:
            Cleaned SPARQL query
        """
        # Remove markdown code blocks
        sparql_text = re.sub(r'```sparql\s*', '', sparql_text)
        sparql_text = re.sub(r'```\s*', '', sparql_text)

        # Extract SPARQL query pattern
        # Look for PREFIX or SELECT/ASK/CONSTRUCT/DESCRIBE
        match = re.search(
            r'((?:PREFIX[^\n]+\n)*\s*(?:SELECT|ASK|CONSTRUCT|DESCRIBE).*)',
            sparql_text,
            re.IGNORECASE | re.DOTALL
        )

        if match:
            sparql_text = match.group(1)

        # Remove common explanatory text
        sparql_text = re.sub(r'(?i)here is the sparql query:?\s*', '', sparql_text)
        sparql_text = re.sub(r'(?i)the query is:?\s*', '', sparql_text)
        sparql_text = re.sub(r'(?i)this query will:?\s*.*$', '', sparql_text, flags=re.MULTILINE)

        # Remaining nl before PREFIX
        index = sparql_text.find("PREFIX")
        if index > -1:
            sparql_text = sparql_text[index:]

        # Clean up whitespace
        lines = [line.strip() for line in sparql_text.strip().split('\n') if line.strip()]

        # Filter out lines that are explanatory text
        sparql_lines = []
        in_query = False
        for line in lines:
            upper_line = line.upper()
            if any(keyword in upper_line for keyword in ['PREFIX', 'SELECT', 'ASK', 'CONSTRUCT', 'DESCRIBE', 'WHERE', 'FROM', 'ORDER', 'LIMIT', 'OFFSET', 'GROUP', 'HAVING', 'FILTER']):
                in_query = True

            if in_query:
                sparql_lines.append(line)

        cleaned = '\n'.join(sparql_lines).strip()

        logger.debug(f"Cleaned SPARQL query: {cleaned}")
        return cleaned

    def _ensure_prefixes(self, query: str) -> str:
        """
        Ensure the four required PREFIX declarations are present in the SPARQL query.
        Prepends missing ones at the top if not found.
        """
        required_prefixes = {
            "rdf": "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
            "blockchain": "PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>",
            "cardano": "PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>",
            "xsd": "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>",
        }

        stripped = query.strip()
        query_upper = query.upper()

        # Check which prefixes are already present
        missing_prefixes = []
        for prefix_name, prefix_declaration in required_prefixes.items():
            # Look for the prefix declaration pattern (case-insensitive)
            # Check for both "PREFIX rdf:" and "PREFIX rdf :" patterns
            pattern1 = f"PREFIX {prefix_name}:".upper()
            pattern2 = f"PREFIX {prefix_name} :".upper()

            if pattern1 not in query_upper and pattern2 not in query_upper:
                missing_prefixes.append(prefix_declaration)

        if missing_prefixes:
            # Prepend missing prefixes with newline separation
            prepend = "\n".join(missing_prefixes) + "\n\n"
            query = prepend + stripped
            logger.debug(f"Added {len(missing_prefixes)} missing prefixes to SPARQL query")
        else:
            logger.debug("All required prefixes already present in SPARQL query")

        return query

    async def contextualize_answer(
        self,
        user_query: str,
        sparql_query: str,
        sparql_results: dict[str, Any],
        system_prompt: str = None
    ) -> AsyncIterator[str]:
        """
        Generate contextualized answer based on SPARQL results.

        Args:
            user_query: Original natural language query
            sparql_query: SPARQL query that was executed
            sparql_results: Results from SPARQL execution
            system_prompt: System prompt for answer generation

        Yields:
            Chunks of contextualized answer
        """
        with tracer.start_as_current_span("contextualize_answer") as span:
            context_res = ""
            try:
                if sparql_results:
                    context_res = json.dumps(sparql_results, indent=2)

            except Exception as e:
                logger.warning(f"json.dumps failed: {e}")
                context_res = sparql_results

            # Format the prompt with query and results
            prompt = f"""
                User Question: {user_query}

                SPARQL Query Executed:
                {sparql_query}

                Query Results:
                {context_res}

                {self.contextualize_prompt}
            """

            logger.info(f"calling ollama model\n    prompt: {prompt}\n")
            async for chunk in self.generate_stream(
                prompt=prompt,
                model=self.llm_model,
                system_prompt=system_prompt,
                temperature=0.3
            ):
                yield chunk

    async def health_check(self) -> bool:
        """
        Check if Ollama service is available.

        Returns:
            True if service is healthy, False otherwise
        """
        try:
            client = await self._get_client()
            response = await client.get(f"{self.base_url}/api/tags")
            healthy = response.status_code == 200
            if not healthy:
                logger.warning(f"Ollama health check with invalid status code {response}")
            return healthy
        except Exception as e:
            logger.warning(f"Ollama health check failed: {e}")
            return False


# Global client instance
_ollama_client: Optional[OllamaClient] = None


def get_ollama_client() -> OllamaClient:
    """Get or create global Ollama client instance."""
    global _ollama_client
    if _ollama_client is None:
        _ollama_client = OllamaClient()
    return _ollama_client


async def cleanup_ollama_client():
    """Cleanup global Ollama client."""
    global _ollama_client
    if _ollama_client:
        await _ollama_client.close()
        _ollama_client = None