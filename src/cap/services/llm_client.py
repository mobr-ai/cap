"""
llm client for interacting with models
"""
import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator
from typing import Any

import httpx
from opentelemetry import trace

from cap.chains.cardano.canon.semantic_matcher import SemanticMatcher
from cap.config import settings
from cap.federated.federated_result_processor import format_kv
from cap.federated.models import FederatedQuery
from cap.federated.planner import FederatedPlanner
from cap.federated.sparql.sparql_result_processor import convert_results_to_explorer_links
from cap.services.intent.refer_classifier import ReferClassifier
from cap.services.intent.render_classifier import RenderClassifier
from cap.services.prompt_builder import PromptBuilder
from cap.services.similarity_service import SearchStrategy
from cap.services.vega.facade import VegaConverter
from cap.util.str_util import matches_keyword
from cap.util.tag_filter import TagFilter

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

LLM_MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))
LLM_RETRY_BASE_SECONDS = float(os.getenv("LLM_RETRY_BASE_SECONDS", "1.0"))
LLM_RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}

MODEL_CONTEXT_CAP = settings.MODEL_CONTEXT_CAP * 1000
CHAR_PER_TOKEN = settings.CHAR_PER_TOKEN
MAX_CONTEXT_CHARS = 18000 # CHAR_PER_TOKEN * MODEL_CONTEXT_CAP


class LLMClient:
    """Client for interacting with LLM service."""

    def __init__(
        self,
        base_url: str | None = None,
        llm_model: str | None = None,
        timeout: float = 120.0
    ):
        """
        Initialize llm client.

        Args:
            base_url: llm API base URL (default: http://localhost:8001)
            llm_model: Model for converting NL to SPARQL
            timeout: Request timeout in seconds
        """
        self.base_url = (base_url or os.getenv("LLM_BASE_URL", "https://api.openai.com")).rstrip("/")
        self.llm_model = (
            llm_model
            or os.getenv("LLM_MODEL_NAME")
            or os.getenv("OPENAI_MODEL")
            or "gpt-5.4"
        )
        self.api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY", "")
        self.timeout = timeout
        self.prompt_builder = PromptBuilder()

        self._client: httpx.AsyncClient | None = None

        self._refer_classifier = ReferClassifier(
            dataset_path=os.getenv(
                "REFER_CLASSIFIER_DATASET_PATH",
                "datasets/refer_classifier_examples.en.jsonl",
            )
        )
        self._render_classifier = RenderClassifier(
            dataset_path=os.getenv(
                "RENDER_CLASSIFIER_DATASET_PATH",
                "datasets/render_classifier_examples.en.jsonl",
            )
        )
        self._intent_warmup_lock = asyncio.Lock()
        self._intent_warmed_up = False

    async def warmup_intent_indices(self, force: bool = False) -> None:
        if self._intent_warmed_up and not force:
            return

        async with self._intent_warmup_lock:
            if self._intent_warmed_up and not force:
                return

            await self._refer_classifier.warmup()
            await self._render_classifier.warmup()
            self._intent_warmed_up = True

    async def _get_nl_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            timeout = httpx.Timeout(self.timeout, connect=10.0)
            headers = {}
            if self.api_key:
                logger.info("**** Using openai")
                headers["Authorization"] = f"Bearer {self.api_key}"

            self._client = httpx.AsyncClient(
                timeout=timeout,
                limits=httpx.Limits(
                    max_keepalive_connections=5,
                    max_connections=10,
                    keepalive_expiry=self.timeout
                ),
                headers=headers,
                http2=True  # Enable HTTP/2 for better performance
            )
        return self._client

    async def _close(self):
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


    @staticmethod
    def _is_retryable_http_error(exc: httpx.HTTPStatusError) -> bool:
        return exc.response.status_code in LLM_RETRY_STATUS_CODES


    @staticmethod
    def _retry_delay(attempt: int) -> float:
        return LLM_RETRY_BASE_SECONDS * (2 ** attempt)


    async def generate_stream(
        self,
        prompt: str,
        model: str,
        system_prompt: str | None = None,
        temperature: float = 0.1
    ) -> AsyncIterator[str]:
        """
        vLLM/OpenAI-compatible streaming Chat Completions.

        Retries transient provider failures before any content is emitted.
        This covers OpenAI/server-side 5xx errors, rate limits, request timeout,
        and temporary transport failures.
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

        last_exc: Exception | None = None

        for attempt in range(LLM_MAX_RETRIES + 1):
            emitted_content = False
            tf = TagFilter()
            tf.reset()

            try:
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

                        if not delta:
                            continue

                        safe = tf.push(delta)
                        if safe:
                            emitted_content = True
                            yield safe

                leftover = tf.flush()
                if leftover:
                    emitted_content = True
                    yield leftover

                return

            except httpx.HTTPStatusError as exc:
                last_exc = exc

                if emitted_content or not self._is_retryable_http_error(exc):
                    raise

                if attempt >= LLM_MAX_RETRIES:
                    raise

                delay = self._retry_delay(attempt)
                logger.warning(
                    "Retryable LLM HTTP error %s from %s; retrying attempt %s/%s after %.1fs",
                    exc.response.status_code,
                    exc.request.url,
                    attempt + 1,
                    LLM_MAX_RETRIES,
                    delay,
                )
                await asyncio.sleep(delay)

            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc

                if emitted_content:
                    raise

                if attempt >= LLM_MAX_RETRIES:
                    raise

                delay = self._retry_delay(attempt)
                logger.warning(
                    "Retryable LLM transport error: %s; retrying attempt %s/%s after %.1fs",
                    exc,
                    attempt + 1,
                    LLM_MAX_RETRIES,
                    delay,
                )
                await asyncio.sleep(delay)

        if last_exc:
            raise last_exc


    async def nl_to_federated_query(
        self,
        natural_query: str,
        conversation_history: list[dict[str, Any]] | None,
        use_ontology: bool = True,
        use_fewshot: bool = True,
        fewshot_strategy: SearchStrategy = SearchStrategy.auto,
        fewshot_top_n: int = -1,
        _eval_retrieved_out: list[dict[str, Any]] | None = None,
    ) -> tuple[FederatedQuery, Any]:

        ontology_block = self.prompt_builder.ontology_prompt if use_ontology else ""

        fewshot_block = ""
        if use_fewshot and fewshot_strategy != SearchStrategy.none:
            # Reuse existing retrieval, but now the returned assistant payload may be
            # SPARQL-only, SQL-only, or federated JSON.
            retrieved: list[dict[str, Any]] = []
            fewshot_block = await self.prompt_builder.build_fewshot_block(
                nl_query=natural_query,
                strategy=fewshot_strategy,
                top_n=fewshot_top_n,
                _eval_retrieved_out=retrieved,
            )

        refer_decision = await self._refer_classifier.classify(natural_query)

        planner = FederatedPlanner(self)
        federated_query = await planner.generate(
            natural_query=natural_query,
            conversation_history=conversation_history,
            ontology_block=ontology_block,
            fewshot_block=fewshot_block,
        )

        return federated_query, refer_decision


    @staticmethod
    def _categorize_query(user_query: str, result_type: str) -> str:
        low_uq = user_query.lower().strip()

        if result_type not in {"multiple", "single"}:
            return ""

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

        if not new_type and matches_keyword(low_uq, SemanticMatcher.CHART_GROUPS["table"]):
            new_type = "table"

        return new_type

    async def _classify_render_type(
        self,
        user_query: str,
        kv_results: dict[str, Any],
    ) -> str:
        current = LLMClient._categorize_query(user_query, kv_results["result_type"])
        if current:
            return current

        decision = await self._render_classifier.classify(user_query)
        if not decision.family:
            return ""

        if decision.family == "table":
            return "table"

        if decision.family == "text":
            return ""

        subtype_to_result = {
            "line": "line_chart",
            "bar": "bar_chart",
            "scatter": "scatter_chart",
            "bubble": "bubble_chart",
            "pie": "pie_chart",
            "heatmap": "heatmap",
            "treemap": "treemap",
        }
        return subtype_to_result.get(decision.chart_subtype or "", "")


    async def generate_answer_with_context(
        self,
        user_query: str,
        federated_query: FederatedQuery,
        formatted_results: str | dict[str, Any],
        kv_results: dict[str, Any],
        system_prompt: str | None = None,
        conversation_history: list[dict[str, Any]] | None = None
    ) -> AsyncIterator[str]:
        """
        Generate contextualized answer based on SPARQL results.
        """
        # Stream kv_results first if present
        result_type = ""
        serialized_query = federated_query.model_dump_json() if federated_query else ""
        if kv_results:
            try:
                result_type = federated_query.visualization_type if federated_query else ""
                if not result_type or result_type not in VegaConverter.known_types:
                    result_type = await self._classify_render_type(user_query, kv_results)

                kv_formatted, result_type = format_kv(
                    result_type=result_type,
                    user_query=user_query,
                    kv_results=kv_results
                )
                logger.info(f"Sending data to feed widget: \n   {kv_formatted}")
                yield f"kv_results: {kv_formatted}\n\n"

            except Exception as e:
                logger.warning(f"KV results formatting failed: {e}")
                yield f"kv_results: {str(kv_results)}\n\n"

            yield "_kv_results_end_\n\n"

        if isinstance(formatted_results, dict):
            try:
                formatted_results = convert_results_to_explorer_links(formatted_results, serialized_query)

            except Exception as e:
                logger.warning(f"Result formatting failed: {e}")
                formatted_results = str(formatted_results)

        prompt, temperature = self.prompt_builder.build_answer_prompt(
            user_query=user_query,
            formatted_results=formatted_results,
            result_type=result_type,
            kv_results=kv_results,
            conversation_history=conversation_history,
        )

        logger.info(f"Prompting LLM (truncated): \n{prompt[:1000] + ('...' if len(prompt) > 1000 else '')}")
        if (not formatted_results or len(formatted_results) == 0):
            logger.info(f" Federated query returned empty: \n{serialized_query}")

        try:
            async for chunk in self.generate_stream(
                prompt=prompt,
                model=self.llm_model,
                system_prompt=system_prompt,
                temperature=temperature
            ):
                yield chunk

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response else None

            if status_code == 429 and kv_results:
                logger.warning(
                    "OpenAI rate limit while generating final answer. "
                    "kv_results were already streamed, so skipping LLM text generation."
                )
                return

            raise


# Global client instance
_llm_client: LLMClient | None = None


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
        await _llm_client._close()
        _llm_client = None


