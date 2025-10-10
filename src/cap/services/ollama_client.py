"""
Ollama client for interacting with ollama.
"""
import os
import logging
import json
import re
from typing import AsyncIterator, Optional, Any
import httpx
from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class OllamaClient:
    """Client for interacting with Ollama LLM service."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        llm_model: str = "cap-nl-sparql",
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
        self.llm_model = llm_model
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

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
            span.set_attribute("model", model)
            span.set_attribute("prompt_length", len(prompt))

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
                    json=request_data
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
                                span.set_attribute("completed", True)
                                break

                        except json.JSONDecodeError:
                            logger.warning(f"Failed to decode JSON: {line}")
                            continue

            except httpx.HTTPStatusError as e:
                span.set_attribute("error", str(e))
                logger.error(f"Ollama HTTP error: {e}")
                raise

            except Exception as e:
                span.set_attribute("error", str(e))
                logger.error(f"Ollama streaming error: {e}")
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
        natural_query: str,
        system_prompt: str=""
    ) -> str:
        """
        Convert natural language query to SPARQL.

        Args:
            natural_query: Natural language query
            system_prompt: System prompt for SPARQL generation

        Returns:
            Generated SPARQL query
        """
        with tracer.start_as_current_span("nl_to_sparql") as span:
            span.set_attribute("query", natural_query)

            sparql_response = await self.generate_complete(
                prompt=natural_query,
                model=self.llm_model,
                system_prompt=system_prompt,
                temperature=0.1  # Low temperature for more deterministic SPARQL
            )

            # Clean the SPARQL query
            cleaned_sparql = self._clean_sparql(sparql_response)
            span.set_attribute("sparql_length", len(cleaned_sparql))

            return cleaned_sparql

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

    async def contextualize_answer(
        self,
        user_query: str,
        sparql_query: str,
        sparql_results: dict[str, Any],
        system_prompt: str
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
            # Format the prompt with query and results
            prompt = f"""User Question: {user_query}

SPARQL Query Executed:
{sparql_query}

Query Results:
{json.dumps(sparql_results, indent=2)}

Based on the above information, provide a clear and helpful answer to the user's question."""

            span.set_attribute("prompt_length", len(prompt))

            async for chunk in self.generate_stream(
                prompt=prompt,
                model=self.llm_model,
                system_prompt=system_prompt,
                temperature=0.3  # Slightly higher for more natural language
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
            return response.status_code == 200
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