"""
llm client for interacting with models
"""
import asyncio
import os
import logging
import json
from datetime import datetime, timezone
from typing import AsyncIterator, Optional, Any, Union
import httpx
from opentelemetry import trace

from cap.config import settings
from cap.util.tag_filter import TagFilter
from cap.util.str_util import get_file_content
from cap.util.vega_util import VegaUtil
from cap.util.cardano_scan import convert_sparql_results_to_links
from cap.util.sparql_util import detect_and_parse_sparql
from cap.services.msg_formatter import MessageFormatter
from cap.services.similarity_service import SimilarityService, SearchStrategy
from cap.services.intent.context_assembler import ConversationContextAssembler
from cap.services.intent.refer_classifier import ReferClassifier
from cap.services.intent.render_classifier import RenderClassifier
from cap.rdf.cache.semantic_matcher import SemanticMatcher

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


MODEL_CONTEXT_CAP = settings.MODEL_CONTEXT_CAP * 1000
CHAR_PER_TOKEN = settings.CHAR_PER_TOKEN
MAX_CONTEXT_CHARS = 18000 # CHAR_PER_TOKEN * MODEL_CONTEXT_CAP


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
        timeout: float = 360.0
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
            or "gpt-4.1-mini"
        )
        self.api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY", "")
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

        self._context_assembler = ConversationContextAssembler()
        self._refer_classifier = ReferClassifier(
            dataset_path=os.getenv(
                "REFER_CLASSIFIER_DATASET_PATH",
                "./datasets/refer_classifier_examples.en.jsonl",
            )
        )
        self._render_classifier = RenderClassifier(
            dataset_path=os.getenv(
                "RENDER_CLASSIFIER_DATASET_PATH",
                "./datasets/render_classifier_examples.en.jsonl",
            )
        )
        self._intent_warmup_lock = asyncio.Lock()
        self._intent_warmed_up = False

    def _load_prompt(self, env_key: str, default: str = "") -> str:
        """Load prompt from environment, refreshed on each call."""
        return os.getenv(env_key, default)

    async def warmup_intent_indices(self, force: bool = False) -> None:
        if self._intent_warmed_up and not force:
            return

        async with self._intent_warmup_lock:
            if self._intent_warmed_up and not force:
                return

            await self._refer_classifier.warmup()
            await self._render_classifier.warmup()
            self._intent_warmed_up = True

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
            timeout = httpx.Timeout(self.timeout, connect=10.0)
            headers = {}
            if self.api_key:
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

        tf = TagFilter()
        tf.reset()

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
                    yield safe

        leftover = tf.flush()
        if leftover:
            yield leftover


    async def nl_to_sparql(
        self,
        natural_query: str,
        conversation_history: list[dict] | None,
        use_ontology: bool = True,
        use_fewshot: bool = True,
        fewshot_strategy: SearchStrategy = SearchStrategy.auto,
        fewshot_top_n: int = 3,
        _eval_retrieved_out: list[dict] | None = None,
    ) -> str:
        with tracer.start_as_current_span("nl_to_sparql") as span:
            span.set_attribute("query", natural_query)

            ontology_block = self.ontology_prompt if use_ontology else ""

            refer_decision = await self._refer_classifier.classify(natural_query)
            span.set_attribute("refer_label", refer_decision.label)
            span.set_attribute("refer_confidence", refer_decision.confidence)

            assembled_history = await self._context_assembler.assemble(
                current_query=natural_query,
                conversation_history=conversation_history,
                refer_decision=refer_decision,
            )

            nl_prompt = f"""
{self.nl_to_sparql_prompt}
{ontology_block}

User Question: {natural_query}
""".strip()

            if use_fewshot and fewshot_strategy != SearchStrategy.none:
                nl_prompt = await self._add_few_shot_learning(
                    nl_query=natural_query,
                    prompt=nl_prompt,
                    strategy=fewshot_strategy,
                    top_n=fewshot_top_n,
                    _eval_retrieved_out=_eval_retrieved_out,
                )

            if assembled_history:
                nl_prompt = self._add_history(
                    prompt=nl_prompt,
                    conversation_history=assembled_history,
                )

            logger.info(
                "LLM is generating SPARQL - prompt size=%s refer=%s history_items=%s",
                len(nl_prompt),
                refer_decision.label,
                len(assembled_history),
            )

            chunks = []
            async for chunk in self.generate_stream(
                prompt=nl_prompt,
                model=self.llm_model,
                system_prompt="",
                temperature=0.0,
            ):
                chunks.append(chunk)

            sparql_response = "".join(chunks)
            if not sparql_response.strip():
                logger.warning("Empty SPARQL response for query '%s'", natural_query)
                return ""

            is_sequential, content = detect_and_parse_sparql(sparql_response, natural_query)
            if is_sequential:
                logger.warning("Sequential SPARQL detected in single nl_to_sparql call; using first query")
                return content[0]["query"] if content else ""

            span.set_attribute("sparql_length", len(content))
            return content

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


    async def format_kv(self, user_query: str, sparql_query: str, kv_results: dict) -> tuple[str, str]:
        result_type = await self._classify_render_type(user_query, kv_results)

        if result_type:
            kv_results["result_type"] = result_type

            if result_type in {
                "bar_chart",
                "pie_chart",
                "line_chart",
                "scatter_chart",
                "bubble_chart",
                "treemap",
                "heatmap",
                "table",
            }:
                vega_data = VegaUtil.convert_to_vega_format(
                    kv_results,
                    user_query,
                    sparql_query,
                )

                columns = []
                if kv_results.get("data"):
                    if isinstance(kv_results["data"], list):
                        columns = list(kv_results["data"][0].keys())
                    elif isinstance(kv_results["data"], dict):
                        columns = list(kv_results["data"].keys())

                metadata_columns = vega_data.get("_columns")
                formatted_columns = (
                    metadata_columns
                    if metadata_columns
                    else [VegaUtil._format_column_name(col) for col in columns]
                )

                vega_data = {k: v for k, v in vega_data.items() if not k.startswith("_")}

                output_data = {
                    "result_type": result_type,
                    "data": vega_data,
                    "metadata": {
                        "count": kv_results.get("count", 0),
                        "columns": formatted_columns,
                    },
                }
                return json.dumps(output_data, indent=2), result_type

        return json.dumps(kv_results, indent=2), result_type


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
                    kv_formatted, result_type = await self.format_kv(
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


    async def _add_few_shot_learning(
        self,
        nl_query: str,
        prompt: str,
        strategy: SearchStrategy = SearchStrategy.auto,
        top_n: int = 3,
        min_similarity: float = 0.0,
        _eval_retrieved_out: list[dict] | None = None,
    ) -> str:
        """Use similar queries as few-shot examples."""

        similar = await SimilarityService.find_similar_queries(
            strategy=strategy,
            nl_query=nl_query,
            top_n=top_n,
            min_similarity=min_similarity,
        )

        if _eval_retrieved_out is not None:
            _eval_retrieved_out.extend(similar)

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
        await _llm_client._close()
        _llm_client = None