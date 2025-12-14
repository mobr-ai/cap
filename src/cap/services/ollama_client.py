"""
Ollama client for interacting with ollama.
"""
import os
import logging
import json
from datetime import datetime, timezone
from typing import AsyncIterator, Optional, Any, Union
import httpx
from opentelemetry import trace

from cap.util.vega_util import VegaUtil
from cap.util.sparql_util import detect_and_parse_sparql
from cap.rdf.cache.semantic_matcher import SemanticMatcher

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

def matches_keyword(low_uq: str, keywords):
    return any(
        form in low_uq
        for keyword in keywords
        for form in (keyword, f"{keyword}s", f"{keyword}es", f"{keyword}ies")
    )

class OllamaClient:
    """Client for interacting with Ollama LLM service."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        llm_model: str = None,
        timeout: float = 300.0
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
    def chart_prompt(self) -> str:
        """Get contextualization prompt (refreshed from env)."""
        return self._load_prompt(
            "CHART_PROMPT",
            "You are the Cardano Analytics Platform chart analyzer."
        )

    @property
    def contextualize_prompt(self) -> str:
        """Get contextualization prompt (refreshed from env)."""
        return self._load_prompt(
            "CONTEXTUALIZE_PROMPT",
            "Based on the query results, provide a clear and helpful answer."
        )

    async def _get_nl_client(self) -> httpx.AsyncClient:
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
            client = await self._get_nl_client()

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
                raise

            except Exception as e:
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

            client = await self._get_nl_client()

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

            logger.info(f"LLM-generated SPARQL: \n {sparql_response}")
            is_sequential, content = detect_and_parse_sparql(sparql_response, natural_query)
            if is_sequential:
                # For backward compatibility, return first query if sequential (or raise/log)
                logger.warning("Sequential SPARQL detected in single nl_to_sparql call; using first query")
                return content[0]['query'] if content else ""
            else:
                span.set_attribute("sparql_length", len(content))
                return content


    def _categorize_query(user_query: str, result_type: str) -> str:
        """
        Categorizes a natural language query into result types:
        - "bar_chart"
        - "pie_chart"
        - "table"
        - "single_value"
        """
        low_uq = user_query.lower().strip()

        if result_type != "multiple" and result_type != "single":
            return ""

        # Chart-related queries
        new_type = ""
        if result_type == "multiple" and matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["bar"]):
            new_type = "bar_chart"
        elif result_type == "single" and matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["pie"]):
            new_type = "pie_chart"
        elif result_type == "multiple" and matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["line"]):
            new_type = "line_chart"

        # Tabular or list queries
        elif matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["table"]):
            new_type = "table"

        return new_type


    async def contextualize_answer(
        self,
        user_query: str,
        sparql_query: str,
        sparql_results: Union[str, dict[str, Any]],
        kv_results: dict[str, Any],
        system_prompt: str = None,
        conversation_history: Optional[list[dict]] = None
    ) -> AsyncIterator[str]:
        """Generate contextualized answer using chat endpoint with conversation history."""

        with tracer.start_as_current_span("contextualized answer") as span:
            # Stream kv_results first if present
            result_type = ""
            if kv_results:
                try:
                    result_type = kv_results.get("result_type", "")
                    result_type = OllamaClient._categorize_query(user_query, result_type)
                    if result_type != "":
                        kv_results["result_type"] = result_type

                        # Convert to Vega format for chart types
                        if result_type in ["bar_chart", "pie_chart", "line_chart", "table"]:
                            vega_data = VegaUtil._convert_to_vega_format(
                                kv_results,
                                user_query,
                                sparql_query
                            )
                            columns = []
                            if kv_results.get("data"):
                                if isinstance(kv_results.get("data"), list):
                                    columns = list(kv_results["data"][0].keys())
                                elif isinstance(kv_results.get("data"), dict):
                                    columns = list(kv_results["data"].keys())

                            output_data = {
                                "result_type": result_type,
                                "data": vega_data,
                                "metadata": {
                                    "count": kv_results.get("count", 0),
                                    "columns": columns
                                }
                            }
                            kv_formatted = json.dumps(output_data, indent=2)
                            logger.debug(f"output_data: \n {kv_formatted}")
                        else:
                            kv_formatted = json.dumps(kv_results, indent=2)
                    else:
                        kv_formatted = json.dumps(kv_results, indent=2)

                    yield f"kv_results:{kv_formatted}\n\n"

                except Exception as e:
                    logger.warning(f"KV results formatting failed: {e}")
                    yield f"kv_results: {str(kv_results)}\n\n"

                yield f"_kv_results_end_\n\n"

            # Prepare context results
            context_res = ""
            try:
                if isinstance(sparql_results, str):
                    context_res = sparql_results
                    span.set_attribute("format", "string")
                elif sparql_results:
                    context_res = json.dumps(sparql_results, indent=2)
                    span.set_attribute("format", "dict")
                else:
                    context_res = ""
                    span.set_attribute("format", "empty")
            except Exception as e:
                logger.warning(f"Result formatting failed: {e}")
                context_res = str(sparql_results)

            # Build system message based on what data we have
            current_date = datetime.now(timezone.utc).date()
            system_content = f"Today is {current_date}.\n"
            temperature = 0.1

            if "chart" in result_type or "table" in result_type:
                # Chart/table analysis mode
                system_content += f"{self.chart_prompt}\n"
                system_content += "The system is showing an artifact to the user using the visualization data provided. "
                system_content += "Always write a SHORT insight about the data shown in the visualization."

            elif context_res:
                # Normal query with SPARQL results
                system_content += self.contextualize_prompt

            else:
                # No results available
                system_content += "If you do not know how to answer the user's question, say you do not know the answer. "
                system_content += "NEVER explain how to get results for the question. "
                system_content += "NEVER answer with a SPARQL query."

            # Prepare messages with history and all context
            messages = self._prepare_chat_messages(
                user_query=user_query,
                sparql_results=context_res,
                kv_results=kv_results,  # Pass kv_results here
                conversation_history=conversation_history,
                system_content=system_content
            )

            logger.debug(f"Chat messages prepared with {len(messages)} total messages")

            # Use chat endpoint instead of generate
            async for chunk in self.chat_stream(
                messages=messages,
                model=self.llm_model,
                temperature=temperature
            ):
                yield chunk


    def _prepare_chat_messages(
        self,
        user_query: str,
        sparql_results: str,
        kv_results: dict[str, Any],
        conversation_history: Optional[list[dict]] = None,
        system_content: str = ""
    ) -> list[dict]:
        """
        Prepare messages for chat API with 40K token limit.
        Estimates ~4 chars per token and caps at ~160K characters (40K tokens).
        """
        MAX_CONTEXT_CHARS = 160_000  # Conservative estimate for 40K tokens

        messages = []

        # System message (if provided)
        if system_content:
            messages.append({"role": "system", "content": system_content})

        # Build current message with all context
        current_message_parts = [user_query]

        # Add kv_results if present (important for chart/table analysis)
        if kv_results:
            try:
                kv_formatted = json.dumps(kv_results, indent=2)
                current_message_parts.append(f"\n\nVisualization Data:\n{kv_formatted}")
            except Exception as e:
                logger.warning(f"Failed to format kv_results: {e}")

        # Add SPARQL results if present
        if sparql_results:
            current_message_parts.append(f"\n\nKnowledge Graph Context:\n{sparql_results}")

        current_message = "\n".join(current_message_parts)

        # Add conversation history (most recent first after reversing)
        if conversation_history:
            reversed_history = list(reversed(conversation_history))
            kept_history = []
            current_size = len(system_content) + len(current_message)

            for msg in reversed_history:
                msg_size = len(msg.get("content", ""))
                if current_size + msg_size < MAX_CONTEXT_CHARS:
                    kept_history.append(msg)
                    current_size += msg_size
                else:
                    logger.info(f"Truncated conversation history at {len(kept_history)} messages due to context limit")
                    break

            # Reverse back to chronological order
            messages.extend(reversed(kept_history))

        messages.append({"role": "user", "content": current_message})

        return messages


    async def chat_stream(
        self,
        messages: list[dict],
        model: str,
        temperature: float = 0.1
    ) -> AsyncIterator[str]:
        """
        Generate streaming response using Ollama chat endpoint.

        Args:
            messages: List of message dicts with 'role' and 'content'
            model: Model name to use
            temperature: Sampling temperature

        Yields:
            Chunks of generated text
        """
        with tracer.start_as_current_span("ollama_chat_stream") as span:
            client = await self._get_nl_client()

            if messages and len(messages) > 1:
                logger.info(f"Query with context:\n   {messages}")
            else:
                logger.info(f"Query without context:\n   {messages}")

            request_data = {
                "model": model,
                "messages": messages,
                "stream": True,
                "options": {
                    "temperature": temperature
                }
            }

            try:
                async with client.stream(
                    "POST",
                    f"{self.base_url}/api/chat",
                    json=request_data,
                    timeout=None
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if not line:
                            continue

                        try:
                            chunk = json.loads(line)

                            # Chat endpoint uses 'message' instead of 'response'
                            if "message" in chunk and "content" in chunk["message"]:
                                yield chunk["message"]["content"]

                            if chunk.get("done", False):
                                break

                        except json.JSONDecodeError:
                            logger.warning(f"Failed to decode JSON: {line}")
                            continue

            except httpx.HTTPStatusError as e:
                logger.error(f"Ollama HTTP error: {e}")
                raise
            except Exception as e:
                logger.error(f"Ollama streaming error: {e}")
                raise


    async def health_check(self) -> bool:
        """
        Check if Ollama service is available.

        Returns:
            True if service is healthy, False otherwise
        """
        try:
            client = await self._get_nl_client()
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