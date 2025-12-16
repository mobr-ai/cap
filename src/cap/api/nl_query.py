# nl_query.py
"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
import re
from typing import Optional, AsyncGenerator

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from opentelemetry import trace

from cap.database.session import get_db
from cap.database.model import User, ConversationMessage
from cap.core.auth_dependencies import get_current_user_unconfirmed
from cap.services.nl_service import query_with_stream_response
from cap.services.redis_nl_client import get_redis_nl_client
from cap.services.ollama_client import get_ollama_client
from cap.services.conversation_persistence import (
    start_conversation_and_persist_user,
    persist_assistant_message_and_touch,
    persist_conversation_artifact_from_raw_kv,
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
router = APIRouter(prefix="/api/v1/nl", tags=["llm"])


# ---------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------

class NLQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000)
    context: Optional[str] = None
    conversation_id: Optional[int] = Field(
        None,
        description="Existing conversation id (omit/null to start a new one)",
    )


# ---------------------------------------------------------------------
# Streaming helpers (NO mid-word splits, NO whitespace "help")
# ---------------------------------------------------------------------

def sse_line(text: str) -> bytes:
    # Keep protocol simple and standard
    return (str(text) + "\n").encode("utf-8")

def sse_data(payload: str) -> bytes:
    # Standard SSE framing with a single space after colon
    # (frontend removes only ONE optional space after "data:")
    return ("data: " + str(payload) + "\n").encode("utf-8")

def iter_word_safe_chunks(text: str, max_len: int = 96):
    """
    Yield chunks without splitting inside words.

    Consumes tokens as: non-space + trailing whitespace (\S+\s*).
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


# ---------------------------------------------------------------------
# Top queries
# ---------------------------------------------------------------------

@router.get("/queries/top")
async def get_top_queries(limit: int = 5):
    with tracer.start_as_current_span("get_top_queries") as span:
        span.set_attribute("limit", limit)
        try:
            redis_client = get_redis_nl_client()
            popular_queries = await redis_client.get_popular_queries(limit=limit)
            return {
                "top_queries": [
                    {
                        "rank": idx + 1,
                        "query": q["original_query"],
                        "normalized_query": q["normalized_query"],
                        "frequency": q["count"],
                    }
                    for idx, q in enumerate(popular_queries)
                ]
            }
        except Exception as e:
            logger.error(f"Error fetching top queries: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------
# NL query endpoint
# ---------------------------------------------------------------------

@router.post("/query")
async def natural_language_query(
    request: NLQueryRequest,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_unconfirmed),
):
    """
    Process natural language query with streaming + unified conversation persistence.

    Key rules:
    - Forward status / kv blocks without touching payload spacing.
    - Re-chunk assistant "data:" output on word boundaries (no fixed slicing).
    - Persist assistant text only once at the end (normalized once).
    """
    with tracer.start_as_current_span("nl_query_pipeline") as span:
        span.set_attribute("query", request.query)

        # 1) Conversation + user message
        persist = current_user is not None
        convo = None
        user_msg = None

        if persist:
            convo, user_msg = start_conversation_and_persist_user(
                db=db,
                user=current_user,
                conversation_id=request.conversation_id,
                query=request.query,
                nl_query_id=None,
            )

        conversation_id = convo.id if convo else None
        user_message_id = user_msg.id if user_msg else None

        conversation_history = []
        if convo:
            # Get messages ordered by creation time
            messages = (
                db.query(ConversationMessage)
                .filter(ConversationMessage.conversation_id == convo.id)
                .order_by(ConversationMessage.created_at.asc())
                .all()
            )

            # Build history, excluding the just-added user message
            for msg in messages:
                if msg.id != user_message_id:  # Don't include the current message
                    conversation_history.append({
                        "role": msg.role,
                        "content": msg.content
                    })

        # -----------------------------------------------------------------
        # 2) Stream + persist artifacts + assistant message
        # -----------------------------------------------------------------

        async def stream_and_persist() -> AsyncGenerator[bytes, None]:
            collecting_kv = False
            kv_buffer: list[str] = []

            # raw assistant text (exact, before storage normalization)
            assistant_buf: list[str] = []

            # word-safe streaming buffer
            pending_text = ""

            async def flush_pending(force: bool = False):
                nonlocal pending_text
                if not pending_text:
                    return
                # If not force, only flush when buffer is "big enough"
                if not force and len(pending_text) < 192:
                    return

                for chunk in iter_word_safe_chunks(pending_text, max_len=96):
                    # emit and persist exactly what we send
                    yield sse_data(chunk)
                    assistant_buf.append(chunk)

                pending_text = ""

            async for chunk in query_with_stream_response(
                request.query,
                request.context,
                db,
                current_user,
                conversation_history=conversation_history,
            ):
                # Decode chunk to inspect SSE framing
                if isinstance(chunk, (bytes, bytearray)):
                    text = chunk.decode("utf-8", errors="ignore")
                else:
                    text = str(chunk)

                # query_with_stream_response may return partial lines; we need a small carry buffer
                # to safely split into full lines.
                # We'll handle this by accumulating into a local buffer per-iteration.
                # (Keep it inside generator state.)
                if not hasattr(stream_and_persist, "_carry"):
                    setattr(stream_and_persist, "_carry", "")
                carry = getattr(stream_and_persist, "_carry")
                carry += text
                lines = carry.splitlines(keepends=False)

                # If the chunk did not end in a newline, last element is incomplete.
                # Keep it in carry for next iteration.
                if carry and not carry.endswith("\n") and not carry.endswith("\r\n"):
                    setattr(stream_and_persist, "_carry", lines.pop() if lines else carry)
                else:
                    setattr(stream_and_persist, "_carry", "")

                for raw_line in lines:
                    line = raw_line.rstrip("\r")
                    payload = parse_sse_payload_from_line(line)
                    chk = payload.strip()

                    # empty keep-alives
                    if chk == "":
                        if collecting_kv:
                            kv_buffer.append("")
                        continue

                    # DONE handling: flush assistant pending and pass through DONE exactly once
                    if chk in ("[DONE]", "data:[DONE]", "data: [DONE]"):
                        async for out in flush_pending(force=True):
                            yield out
                        # pass through a canonical done line
                        yield sse_data("[DONE]")
                        continue

                    # status lines: forward as-is (normalize only framing, not spaces inside)
                    if chk.startswith("status:"):
                        # preserve original line style (if it's already "status: ...")
                        yield sse_line(line)
                        continue

                    # kv_results start marker (may arrive as its own line OR prefixed)
                    if chk.startswith("kv_results:"):
                        # flush any assistant text before switching modes
                        async for out in flush_pending(force=True):
                            yield out

                        collecting_kv = True
                        kv_buffer.clear()

                        # Forward kv start marker in the simplest form:
                        yield sse_line("kv_results:")

                        # If the same line has json content after kv_results:
                        rest = chk[len("kv_results:"):].strip()
                        if rest and rest != "_kv_results_end_":
                            kv_buffer.append(rest)
                            yield sse_line(rest)

                        # If end marker was on same line
                        if "_kv_results_end_" in chk:
                            collecting_kv = False
                            # forward end marker
                            yield sse_line("_kv_results_end_")
                            if persist and convo is not None:
                                try:
                                    raw_kv_payload = "\n".join(kv_buffer).strip()
                                    persist_conversation_artifact_from_raw_kv(
                                        db=db,
                                        conversation=convo,
                                        conversation_message_id=user_message_id,
                                        nl_query_id=None,
                                        raw_kv_payload=raw_kv_payload,
                                    )
                                except Exception as e:
                                    db.rollback()
                                    logger.error(f"Failed to persist conversation artifact: {e}")
                            kv_buffer.clear()
                        continue

                    if collecting_kv:
                        # end marker for kv
                        if "_kv_results_end_" in chk:
                            collecting_kv = False
                            yield sse_line("_kv_results_end_")
                            if persist and convo is not None:
                                try:
                                    raw_kv_payload = "\n".join(kv_buffer).strip()
                                    persist_conversation_artifact_from_raw_kv(
                                        db=db,
                                        conversation=convo,
                                        conversation_message_id=user_message_id,
                                        nl_query_id=None,
                                        raw_kv_payload=raw_kv_payload,
                                    )
                                except Exception as e:
                                    db.rollback()
                                    logger.error(f"Failed to persist conversation artifact: {e}")
                            kv_buffer.clear()
                        else:
                            # Keep the kv payload line exactly (do NOT strip)
                            kv_buffer.append(payload)
                            yield sse_line(payload)
                        continue

                    # Normal assistant content:
                    # Buffer and re-emit on word boundaries (no trimming, no collapsing)
                    pending_text += payload

                    async for out in flush_pending(force=False):
                        yield out

            # Stream ended without explicit DONE:
            async for out in flush_pending(force=True):
                yield out

            # 3) Persist assistant message (normalize once, at end)
            if persist and convo is not None:
                assistant_text = "".join(assistant_buf)
                if assistant_text:
                    try:
                        persist_assistant_message_and_touch(
                            db=db,
                            conversation=convo,
                            content=assistant_text,
                            nl_query_id=None,
                        )
                        db.commit()
                    except Exception as e:
                        db.rollback()
                        logger.error(f"Failed to persist assistant message: {e}")

        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Expose-Headers": "X-Conversation-Id, X-User-Message-Id",
        }
        if conversation_id is not None:
            headers["X-Conversation-Id"] = str(conversation_id)
        if user_message_id is not None:
            headers["X-User-Message-Id"] = str(user_message_id)

        return StreamingResponse(
            stream_and_persist(),
            media_type="text/event-stream; charset=utf-8",
            headers=headers,
        )


# ---------------------------------------------------------------------
# Health / cache endpoints
# ---------------------------------------------------------------------

@router.get("/health")
async def health_check():
    try:
        ollama = get_ollama_client()
        healthy = await ollama.health_check()
        return {
            "status": "healthy" if healthy else "unhealthy",
            "service": "ollama",
            "models": {"llm_model": ollama.llm_model},
        }
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {"status": "error", "service": "ollama", "error": str(e)}


@router.get("/cache/stats")
async def get_cache_stats():
    try:
        redis_client = get_redis_nl_client()
        popular_queries = await redis_client.get_popular_queries(limit=10)
        return {
            "popular_queries": [
                {"query": q, "count": count} for q, count in popular_queries
            ]
        }
    except Exception as e:
        logger.error(f"Cache stats error: {e}")
        return {"error": str(e)}
