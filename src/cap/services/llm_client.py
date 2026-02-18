"""
llm client for interacting with models
"""
import os
import logging
import json
from datetime import datetime, timezone
from typing import AsyncIterator, Optional, Any, Union
import httpx
from opentelemetry import trace

from cap.config import settings
from cap.util.str_util import get_file_content
from cap.util.vega_util import VegaUtil
from cap.util.cardano_scan import convert_sparql_results_to_links
from cap.util.sparql_util import detect_and_parse_sparql
from cap.services.msg_formatter import MessageFormatter
from cap.services.similarity_service import SimilarityService
from cap.rdf.cache.semantic_matcher import SemanticMatcher

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


MODEL_CONTEXT_CAP = settings.MODEL_CONTEXT_CAP * 1000
CHAR_PER_TOKEN = settings.CHAR_PER_TOKEN
MAX_CONTEXT_CHARS = CHAR_PER_TOKEN * MODEL_CONTEXT_CAP


def matches_keyword(low_uq: str, keywords):
    return any(
        form in low_uq
        for keyword in keywords
        for form in (keyword, f"{keyword}s", f"{keyword}es", f"{keyword}ies", f"{keyword}ing")
    )

class LLMClient:
    """Client for interacting with LLM service."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        llm_model: str = None,
        timeout: float = 300.0
    ):
        """
        Initialize llm client.

        Args:
            base_url: llm API base URL (default: http://localhost:8000)
            llm_model: Model for converting NL to SPARQL
            timeout: Request timeout in seconds
        """
        self.base_url = (base_url or os.getenv("LLM_BASE_URL", "http://localhost:8000")).rstrip("/")
        self.llm_model = (llm_model or os.getenv("LLM_MODEL_NAME", "Qwen/Qwen3-8B-AWQ"))
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
        """Get contextualization prompt for chart related queries (refreshed from env)."""
        return self._load_prompt(
            "CHART_PROMPT",
            "You are the Cardano Analytics Platform chart analyzer."
        )

    @property
    def ontology_prompt(self) -> str:
        """Add ontology to prompt (refreshed from env)."""
        if settings.LLM_ONTOLOGY_PATH != "":
            onto = get_file_content(settings.LLM_ONTOLOGY_PATH)
            return f"ALWAYS USE THIS ONTOLOGY:\n{onto}"

        return ""

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


    async def health_check(self) -> bool:
        """
        vLLM OpenAI-compatible health check.
        """
        try:
            client = await self._get_nl_client()

            r = await client.get(f"{self.base_url}/v1/models")
            r.raise_for_status()

            data = r.json()
            models = {m.get("id") for m in data.get("data", []) if isinstance(m, dict)}

            return (self.llm_model in models) if self.llm_model else True

        except Exception:
            return False


    async def generate_stream(
        self,
        prompt: str,
        model: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.1
    ) -> AsyncIterator[str]:
        """
        vLLM OpenAI-compatible streaming Chat Completions.
        Streams Server-Sent Events (SSE): lines start with 'data: ...' and end with 'data: [DONE]'.
        """
        client = await self._get_nl_client()

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        request_data = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": True,
        }

        async with client.stream(
            "POST",
            f"{self.base_url}/v1/chat/completions",
            json=request_data,
            timeout=None,
        ) as response:
            response.raise_for_status()

            async for line in response.aiter_lines():
                if not line:
                    continue

                # vLLM streams SSE: "data: {...}"
                if line.startswith("data: "):
                    payload = line[len("data: "):].strip()
                else:
                    continue

                if payload == "[DONE]":
                    break

                try:
                    chunk = json.loads(payload)
                except json.JSONDecodeError:
                    continue

                # OpenAI-style delta tokens
                delta = (
                    chunk.get("choices", [{}])[0]
                        .get("delta", {})
                        .get("content")
                )
                if delta:
                    yield delta


    async def generate_complete(
        self,
        prompt: str,
        model: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.1
    ) -> str:
        """
        vLLM OpenAI-compatible non-streaming Chat Completions.
        """
        client = await self._get_nl_client()

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        request_data = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": False,
        }

        response = await client.post(
            f"{self.base_url}/v1/chat/completions",
            json=request_data,
        )
        response.raise_for_status()

        data = response.json()
        return (
            data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
        )


    async def chat_stream(
        self,
        messages: list[dict],
        model: str,
        temperature: float = 0.1
    ) -> AsyncIterator[str]:
        client = await self._get_nl_client()

        request_data = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": True,
        }

        async with client.stream(
            "POST",
            f"{self.base_url}/v1/chat/completions",
            json=request_data,
            timeout=None,
        ) as response:
            response.raise_for_status()

            async for line in response.aiter_lines():
                if not line:
                    continue
                if not line.startswith("data: "):
                    continue

                payload = line[len("data: "):].strip()
                if payload == "[DONE]":
                    break

                try:
                    chunk = json.loads(payload)
                except json.JSONDecodeError:
                    continue

                delta = (
                    chunk.get("choices", [{}])[0]
                        .get("delta", {})
                        .get("content")
                )
                if delta:
                    yield delta


    async def nl_to_sparql(
        self,
        natural_query: str,
        conversation_history: list[dict] | None
    ) -> str:
        """Convert natural language query to SPARQL."""
        with tracer.start_as_current_span("nl_to_sparql") as span:
            span.set_attribute("query", natural_query)

            # Use fresh prompt from environment
            system_prompt = ""
            nl_prompt = f"""
                {self.nl_to_sparql_prompt}
                {self.ontology_prompt}

                User Question: {natural_query}

            """

            nl_prompt = await self._add_few_shot_learning(
                nl_query=natural_query,
                prompt=nl_prompt
            )

            # Prepare messages with history and all context
            nl_prompt = self._add_history(
                prompt=nl_prompt,
                conversation_history=conversation_history,
            )

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

    @staticmethod
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
        if result_type == "multiple":
            if matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["bar"]):
                new_type = "bar_chart"
            elif matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["line"]):
                new_type = "line_chart"
            elif matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["scatter"]):
                new_type = "scatter_chart"
            elif matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["bubble"]):
                new_type = "bubble_chart"
            elif matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["treemap"]):
                new_type = "treemap"
            elif matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["heatmap"]):
                new_type = "heatmap"

        elif result_type == "single" and matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["pie"]):
            new_type = "pie_chart"

        # Tabular or list queries
        if new_type == "" and matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["table"]):
            new_type = "table"

        return new_type

    @staticmethod
    def format_kv(user_query: str, sparql_query:str, kv_results: dict) -> str:
        result_type = kv_results["result_type"]
        result_type = LLMClient._categorize_query(user_query, result_type)
        if result_type != "":
            kv_results["result_type"] = result_type

            # Convert to Vega format for chart types
            if result_type in ["bar_chart", "pie_chart", "line_chart", "scatter_chart", "bubble_chart", "treemap", "heatmap", "table"]:
                vega_data = VegaUtil.convert_to_vega_format(
                    kv_results,
                    user_query,
                    sparql_query
                )

                # Determine columns based on chart type
                columns = []
                if kv_results.get("data"):
                    if isinstance(kv_results.get("data"), list):
                        columns = list(kv_results["data"][0].keys())
                    elif isinstance(kv_results.get("data"), dict):
                        columns = list(kv_results["data"].keys())

                # Check if we have series labels from vega conversion (for line charts)
                series_labels = vega_data.get("_series_labels")
                label_key = vega_data.get("_label_key")
                x_key = vega_data.get("_x_key")
                y_keys = vega_data.get("_y_keys", [])

                if series_labels and label_key:
                    # Build formatted columns: [x_label, y_label, ...series_labels]
                    formatted_columns = []

                    # Add x-axis label
                    if x_key:
                        formatted_columns.append(VegaUtil._format_column_name(x_key))

                    # Add y-axis labels (do we need this?)
                    for y_key in y_keys:
                        pass

                    # Add series labels (replacing the label_key column)
                    formatted_columns.extend(series_labels)
                else:
                    # Standard case: format all column names OR use metadata columns
                    metadata_columns = vega_data.get("_columns")
                    if metadata_columns:
                        formatted_columns = metadata_columns
                    else:
                        formatted_columns = [VegaUtil._format_column_name(col) for col in columns]

                # Remove internal metadata from vega_data
                vega_data = {k: v for k, v in vega_data.items() if not k.startswith("_")}

                output_data = {
                    "result_type": result_type,
                    "data": vega_data,
                    "metadata": {
                        "count": kv_results.get("count", 0),
                        "columns": formatted_columns
                    }
                }
                kv_formatted = json.dumps(output_data, indent=2)
                logger.info(f"output_data: \n {kv_formatted}")
            else:
                kv_formatted = json.dumps(kv_results, indent=2)
        else:
            kv_formatted = json.dumps(kv_results, indent=2)

        return kv_formatted, result_type


    async def generate_answer_with_context(
        self,
        user_query: str,
        sparql_query: str,
        sparql_results: Union[str, dict[str, Any]],
        kv_results: dict[str, Any],
        system_prompt: str = None,
        conversation_history: Optional[list[dict]] = None
    ) -> AsyncIterator[str]:
        """
        Generate contextualized answer based on SPARQL results.

        Args:
            user_query: Original natural language query
            sparql_query: SPARQL query that was executed
            sparql_results: Results from SPARQL execution (formatted string or raw dict)
            system_prompt: System prompt for answer generation

        Yields:
            Chunks of contextualized answer
        """
        with tracer.start_as_current_span("contextualized answer") as span:
            # Stream kv_results first if present
            result_type = ""
            if kv_results:
                try:
                    kv_formatted, result_type = LLMClient.format_kv(
                        user_query=user_query,
                        sparql_query=sparql_query,
                        kv_results=kv_results
                    )
                    logger.info(f"Sending data to feed widget: \n   {kv_formatted}")
                    yield f"kv_results: {kv_formatted}\n\n"

                except Exception as e:
                    logger.warning(f"KV results formatting failed: {e}")
                    yield f"kv_results: {str(kv_results)}\n\n"

                yield f"_kv_results_end_\n\n"

            context_res = ""
            try:
                # If results are already formatted as string, use directly
                if isinstance(sparql_results, str):
                    context_res = sparql_results
                    span.set_attribute("format", "string")
                # Otherwise, serialize dict to JSON
                elif sparql_results:
                    sparql_results = convert_sparql_results_to_links(sparql_results, sparql_query)
                    context_res = json.dumps(sparql_results, indent=2)
                    span.set_attribute("format", "dict")
                else:
                    context_res = ""
                    span.set_attribute("format", "empty")

            except Exception as e:
                logger.warning(f"Result formatting failed: {e}")
                context_res = str(sparql_results)

            current_date = f"Current utc date and time: {datetime.now(timezone.utc)}."
            current_his = None
            known_info = ""
            temperature = 0.1
            if "chart" in result_type or "table" in result_type:
                known_info = f"""
                {current_date}
                {self.chart_prompt}
                The system is showing an artifact to the user using the data below. Always write a SHORT insight about it.
                {kv_results}
                """

            elif context_res != "":
                known_info = f"""
                {current_date}
                This is the current value you MUST consider in your answer:
                {context_res}

                {self.contextualize_prompt}
                """
                current_his = conversation_history

            else:
                known_info = f"""
                    Answer with a text similar to the following message:
                    I do not have this information or I was not capable of retrieving it correctly.
                    We would appreciate it if you could specify here what you wanted to do as a feature and we will try to make your prompt work asap.
                    If you think this feature is already supported, try specifying the entire command in a unique prompt.
                """

            # Format the prompt with query and results
            prompt = f"""
                User Question: {user_query}

                {known_info}
            """

            # Prepare messages with history and all context
            prompt = self._add_history(
                prompt=prompt,
                conversation_history=current_his,
            )

            logger.info(f"Prompting LLM (truncated): \n{prompt[:1000] + ('...' if len(prompt) > 1000 else '')}")
            if (not sparql_results or len(sparql_results) == 0):
                logger.info(f" Sparql query returned empty: \n{sparql_query}")

            async for chunk in self.generate_stream(
                prompt=prompt,
                model=self.llm_model,
                system_prompt=system_prompt,
                temperature=temperature
            ):
                yield chunk

            # Yield SPARQL query as metadata after the response
            # if sparql_query:
            #     metadata = {
            #         "type": "metadata",
            #         "sparql_query": sparql_query
            #     }
            #     yield f"\n__METADATA__:{json.dumps(metadata)}"

    async def _add_few_shot_learning(self, nl_query: str, prompt:str) -> str:
        """Use similar queries as few-shot examples."""
        top_n = 5
        min_similarity = 0.0

        # Find similar cached queries and format as examples
        similar = await SimilarityService.find_similar_queries(
            nl_query=nl_query,
            top_n=top_n,
            min_similarity=min_similarity
        )

        messages = MessageFormatter.format_similar_queries_to_examples(
            similar_queries=similar,
            max_examples=top_n
        )

        return MessageFormatter.append_examples_to_prompt(
            examples=messages,
            existing_prompt=prompt
        )


    def _add_history(
        self,
        prompt: str,
        conversation_history: Optional[list[dict]] = None
    ) -> list[dict]:
        """
        Prepare messages for chat API with token limit.
        """

        history = []

        # Add conversation history (most recent first after reversing)
        if conversation_history:
            reversed_history = list(reversed(conversation_history))
            kept_history = []
            current_size = len(prompt)

            for msg in reversed_history:
                msg_size = len(msg.get("content", ""))
                if current_size + msg_size < MAX_CONTEXT_CHARS:
                    kept_history.append(msg)
                    current_size += msg_size
                else:
                    logger.info(f"Truncated conversation history at {len(kept_history)} messages due to context limit")
                    break

            # Reverse back to chronological order
            history = list(reversed(kept_history))

        # Format each message as "role: content"
        str_history = "\n".join([
            f"{msg.get('role', 'unknown')}: {msg.get('content', '')}"
            for msg in history
        ])

        return f"{prompt}\nPrevious messages:\n{str_history}" if str_history else prompt


# Global client instance
_llm_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Get or create global llm client instance."""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client


async def cleanup_llm_client():
    """Cleanup global llm client."""
    global _llm_client
    if _llm_client:
        await _llm_client.close()
        _llm_client = None