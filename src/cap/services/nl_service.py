"""
Natural language query API endpoint using LLM.
Multi-stage pipeline: NL -> FederatedQuery -> Execute -> Contextualize -> Stream
"""
import re
import logging
import time
from typing import Any

from cap.services.agentic.graph import build_agentic_query_graph
from cap.services.llm_client import get_llm_client
from cap.services.metrics_service import MetricsService
from cap.services.redis_nl_client import get_redis_nl_client
from cap.util.json_util import json_safe
from cap.util.status_message import StatusMessage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Streaming helpers (NO mid-word splits, NO whitespace "help")
# ---------------------------------------------------------------------
# NOTE:
# Upstream/proxies sometimes append "data: [DONE]" directly onto the end of a
# data payload without a newline, e.g. "...smart contracts.data: [DONE]".
# Do NOT rely on word-boundary before "data:" because chunking/punctuation can
# make it inconsistent.
_INLINE_DONE_RE = re.compile(r"(?:data:\s*)?\[DONE\]")


def split_inline_done(payload: str) -> tuple[str, bool]:
    """
    Upstream streams (or proxies) may accidentally concatenate `data: [DONE]`
    onto the end of a normal payload without a newline, e.g.:
      ".... smart contracts.data: [DONE]"

    This function detects an inline DONE marker and splits it off so we can:
      - emit the preceding text (without leaking `data:` into UI),
      - then emit a clean DONE event, and stop.
    """
    if not payload:
        return payload, False

    m = _INLINE_DONE_RE.search(payload)
    if not m:
        return payload, False

    before = payload[: m.start()]
    # If the broken concat left a trailing "data:" token at the end of the text,
    # strip it (preserve everything else).
    before = re.sub(r"(?:\s*data:\s*)$", "", before)
    return before, True

def strip_any_done_markers(text: str) -> tuple[str, bool]:
    before, hit = split_inline_done(text)
    return before, hit

def sse_line(text: str) -> bytes:
    # Keep protocol simple and standard
    return (str(text) + "\n").encode("utf-8")

def sse_data(payload: str) -> bytes:
    # Standard SSE framing with a single space after colon
    # (frontend removes only ONE optional space after "data:")
    return ("data: " + str(payload) + "\n").encode("utf-8")

def iter_word_safe_chunks(text: str, max_len: int = 96):
    r"""
    Yield chunks without splitting inside words.

    Consumes tokens as: non-space + trailing whitespace (\\S+\\s*).
    Preserves spaces exactly; avoids 'thiswould' / 'mint ed' regressions
    caused by fixed-width slicing or trimming.
    """
    if not text:
        return
    if max_len <= 0:
        yield text
        return

    buf = ""
    for m in re.finditer(r"\S+\s*", text):
        tok = m.group(0)

        # hard-split only if a single token is enormous (rare)
        if len(tok) > max_len:
            if buf:
                yield buf
                buf = ""
            for i in range(0, len(tok), max_len):
                yield tok[i : i + max_len]
            continue

        if buf and (len(buf) + len(tok) > max_len):
            yield buf
            buf = tok
        else:
            buf += tok

    if buf:
        yield buf

def is_billable_assistant_text(text: str) -> bool:
    value = (text or "").strip()
    if not value:
        return False

    lowered = value.lower()

    non_billable_markers = (
        "error:",
        "error generating answer",
        "client error",
        "server error",
        "http error",
        "unauthorized",
        "for more information check:",
        "failed to generate",
        "failed to execute",
    )

    return not any(marker in lowered for marker in non_billable_markers)



def parse_sse_payload_from_line(line: str) -> str:
    """
    Convert an SSE text line to its payload.

    - For "data:" lines, remove the SSE delimiter and ONE optional space.
    - For other lines (status/kv markers), payload is the raw line.
    """
    if line.startswith("data:"):
        payload = line[5:]
        if payload.startswith(" "):
            payload = payload[1:]
        return payload
    return line


async def query_with_stream_response(
    query,
    context,
    db=None,
    user=None,
    conversation_history=None,
    final_state_out: dict[str, Any] | None = None,
    request_source: str = "cap_web",
    telegram_account_id: int | None = None,
    telegram_user_id: int | None = None,
    telegram_chat_id: int | None = None,
):
    start_time = time.time()
    final_state = {}

    try:
        yield StatusMessage.processing_query()

        llm_client = get_llm_client()
        redis_client = get_redis_nl_client()

        user_query = query
        if context:
            user_query = f"{context}\n\n{query}"

        graph = build_agentic_query_graph(
            llm_client=llm_client,
            redis_client=redis_client,
        )

        initial_state = {
            "user_query": user_query,
            "context": context,
            "conversation_history": conversation_history,
            "retry_count": 0,
            "max_retries": 2,
        }

        final_state = {}

        async for mode, payload in graph.astream(
            initial_state,
            stream_mode=["updates", "custom"],
        ):
            if mode == "custom":
                if isinstance(payload, dict) and payload.get("type") == "answer_chunk":
                    yield payload["content"]
                elif isinstance(payload, dict) and payload.get("type") == "status":
                    yield payload["content"]
                continue

            update = payload
            for step_name, step_state in update.items():
                if isinstance(step_state, dict):
                    final_state.update(step_state)

                if step_name == "critic" and final_state.get("federated_query") is None:
                    yield StatusMessage.retry_query(final_state.get("retry_count", 0))

        yield StatusMessage.data_done()

    except Exception as exc:
        logger.error("Agentic pipeline error: %s", exc, exc_info=True)
        yield StatusMessage.error(f"Unexpected error: {exc}")
        yield StatusMessage.data_done()

    finally:
        total_latency_ms = int((time.time() - start_time) * 1000)

        if final_state_out is not None:
            final_state_out.clear()
            final_state_out.update(final_state or {})

        try:
            federated_query = final_state.get("federated_query")
            execution_result = final_state.get("execution_result")
            user_id = user.user_id if user else None

            sparql_query = federated_query.sparql if federated_query else ""
            sql_query = federated_query.sql if federated_query else ""
            query_source = (
                federated_query.source.value
                if federated_query and federated_query.source
                else None
            )

            MetricsService.record_query_metrics(
                db=db,
                nl_query=query,
                normalized_query=final_state.get("normalized_query", ""),
                sparql_query=sparql_query,
                sql_query=sql_query,
                query_source=query_source,
                kv_results=json_safe(final_state.get("kv_results")),
                is_sequential=False,
                sparql_valid=bool(sparql_query and final_state.get("query_valid")),
                sql_valid=bool(sql_query and final_state.get("query_valid")),
                query_succeeded=bool(execution_result and execution_result.has_data),
                llm_latency_ms=0,
                sparql_latency_ms=0,
                sql_latency_ms=0,
                total_latency_ms=total_latency_ms,
                user_id=user_id,
                telegram_account_id=telegram_account_id,
                telegram_user_id=telegram_user_id,
                telegram_chat_id=telegram_chat_id,
                request_source=request_source,
                error_message=final_state.get("error"),
            )
        except Exception as metrics_error:
            if db:
                db.rollback()
            logger.error(f"Failed to record metrics: {metrics_error}")
