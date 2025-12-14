"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
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
from cap.services.ollama_client import get_ollama_client
from cap.services.redis_nl_client import get_redis_nl_client
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
    """

    with tracer.start_as_current_span("nl_query_pipeline") as span:
        span.set_attribute("query", request.query)

        # -----------------------------------------------------------------
        # 1) Conversation + user message
        # -----------------------------------------------------------------

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
            assistant_parts: list[str] = []

            async for chunk in query_with_stream_response(
                request.query,
                request.context,
                db,
                current_user,
                conversation_history=conversation_history,
            ):
                # Forward chunk to client verbatim
                yield chunk

                # Decode for parsing only
                if isinstance(chunk, (bytes, bytearray)):
                    text = chunk.decode("utf-8", errors="ignore")
                else:
                    text = str(chunk)

                for raw_line in text.splitlines():
                    line = raw_line

                    # Strip SSE prefix for parsing
                    if line.startswith("data: "):
                        payload = line[6:]
                    else:
                        payload = line

                    stripped = payload.strip()
                    if not stripped:
                        continue

                    # Ignore protocol noise
                    if stripped in ("[DONE]", "data: [DONE]"):
                        continue
                    if stripped.startswith("status:"):
                        continue

                    # ---------------------------------------------------------
                    # kv_results handling (PERSIST AT END MARKER)
                    # ---------------------------------------------------------

                    if stripped.startswith("kv_results:"):
                        collecting_kv = True
                        kv_buffer.clear()

                        payload = stripped[len("kv_results:"):].strip()
                        if payload and payload != "_kv_results_end_":
                            kv_buffer.append(payload)

                        if "_kv_results_end_" in stripped:
                            collecting_kv = False

                            try:
                                persist_conversation_artifact_from_raw_kv(
                                    db=db,
                                    conversation=convo,
                                    conversation_message_id=user_message_id,
                                    nl_query_id=None,
                                    raw_kv_payload="\n".join(kv_buffer).strip(),
                                )
                            except Exception as e:
                                db.rollback()
                                logger.error(
                                    f"Failed to persist conversation artifact: {e}"
                                )

                            kv_buffer.clear()
                        continue

                    if collecting_kv:
                        if "_kv_results_end_" in stripped:
                            collecting_kv = False

                            try:
                                persist_conversation_artifact_from_raw_kv(
                                    db=db,
                                    conversation=convo,
                                    conversation_message_id=user_message_id,
                                    nl_query_id=None,
                                    raw_kv_payload="\n".join(kv_buffer).strip(),
                                )
                            except Exception as e:
                                db.rollback()
                                logger.error(
                                    f"Failed to persist conversation artifact: {e}"
                                )

                            kv_buffer.clear()
                        else:
                            kv_buffer.append(stripped)
                        continue

                    # ---------------------------------------------------------
                    # Normal assistant content (KV excluded)
                    # ---------------------------------------------------------

                    assistant_parts.append(stripped)

            # -----------------------------------------------------------------
            # 3) Persist assistant message
            # -----------------------------------------------------------------

            assistant_text = " ".join(assistant_parts).strip()
            if assistant_text and convo is not None:
                try:
                    persist_assistant_message_and_touch(
                        db=db,
                        conversation=convo,
                        content=assistant_text,
                        nl_query_id=None,
                    )
                except Exception as e:
                    db.rollback()
                    logger.error(f"Failed to persist assistant message: {e}")

        # -----------------------------------------------------------------
        # 4) Streaming response
        # -----------------------------------------------------------------

        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        if conversation_id is not None:
            headers["X-Conversation-Id"] = str(conversation_id)
        if user_message_id is not None:
            headers["X-User-Message-Id"] = str(user_message_id)

        return StreamingResponse(
            stream_and_persist(),
            media_type="text/event-stream",
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
