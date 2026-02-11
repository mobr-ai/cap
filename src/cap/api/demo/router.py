# cap/api/demo/router.py
from __future__ import annotations

import json
import logging
import time
from typing import AsyncGenerator, Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import User, QueryMetrics
from cap.services.metrics_service import MetricsService
from cap.services.conversation_persistence import (
    start_conversation_and_persist_user,
    persist_assistant_message_and_touch,
    persist_conversation_artifact_from_raw_kv,
)

from .schemas import DemoQueryRequest, BreakSSEMode
from .auth import get_optional_user
from .sse import NL_TOKEN, DONE_SSE, iter_sse_markdown_events
from .scenes import pick_scene

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/demo", tags=["demo"])


def _truthy(v) -> bool:
    if v is True:
        return True
    if v is False or v is None:
        return False
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y", "on")
    return bool(v)


def _scene_break_mode(req: DemoQueryRequest, scene: Optional[dict]) -> BreakSSEMode:
    if req.break_sse_mode in ("concat_payload", "concat_raw"):
        return req.break_sse_mode

    if not scene:
        return None

    mode = scene.get("break_sse_mode")
    if mode in ("concat_payload", "concat_raw"):
        return mode

    if _truthy(scene.get("break_sse")):
        return "concat_raw"

    return None


def _find_recent_demo_query_metrics_id(db: Session, user_id: Optional[int], nl_query: str) -> Optional[int]:
    if not user_id:
        return None
    q = (
        db.query(QueryMetrics)
        .filter(QueryMetrics.user_id == user_id, QueryMetrics.nl_query == nl_query)
        .order_by(QueryMetrics.created_at.desc(), QueryMetrics.id.desc())
        .first()
    )
    return int(q.id) if q and q.id is not None else None


@router.post("/nl/query")
async def demo_nl_query(
    req: DemoQueryRequest,
    request: Request,
    db: Session = Depends(get_db),
    user: Optional[User] = Depends(get_optional_user),
):
    scene = pick_scene(req.query)

    conversation = None
    user_msg = None
    persist = user is not None
    kv_results_dict = None
    deferred_raw_kv: Optional[str] = None

    if persist:
        conversation, user_msg = start_conversation_and_persist_user(
            db=db,
            user=user,
            conversation_id=req.conversation_id,
            query=req.query,
            nl_query_id=None,
        )

    conversation_id = conversation.id if conversation else None
    user_message_id = user_msg.id if user_msg else None

    assistant_text = (scene or {}).get("assistant_text") or (
        "This is a demo response. Try: "
        "'List the latest 5 blocks.' or "
        "'Show the last 5 proposals.' or "
        "'Monthly multi assets created in 2021.'"
    )

    t0 = time.perf_counter()
    break_mode = _scene_break_mode(req, scene)

    delay_ms = req.delay_ms if req.delay_ms is not None else 0

    def _sleep(ms: int):
        if ms and ms > 0:
            time.sleep(ms / 1000.0)

    async def stream_demo() -> AsyncGenerator[bytes, None]:
        nonlocal kv_results_dict, deferred_raw_kv

        yield b"status: Planning...\n"
        _sleep(delay_ms)

        yield b"status: Querying knowledge graph...\n"
        _sleep(delay_ms)

        # KV block (defer persistence until qid exists)
        if scene and scene.get("kv"):
            yield b"kv_results:\n"
            raw_kv = json.dumps(scene["kv"])
            yield (raw_kv + "\n").encode("utf-8")
            yield b"_kv_results_end_\n"
            _sleep(delay_ms)

            deferred_raw_kv = raw_kv
            kv_results_dict = scene["kv"]

        yield b"status: Writing answer...\n"
        _sleep(delay_ms)

        payloads = list(iter_sse_markdown_events(assistant_text or "", max_len=96))

        if break_mode and payloads:
            carrier_idx = None
            for i in range(len(payloads) - 1, -1, -1):
                p = payloads[i]
                if p and p != NL_TOKEN:
                    carrier_idx = i
                    break

            if carrier_idx is None:
                for payload in payloads:
                    yield f"data: {payload}\n".encode("utf-8")
            else:
                for i, payload in enumerate(payloads):
                    if i == carrier_idx:
                        continue
                    yield f"data: {payload}\n".encode("utf-8")
                    _sleep(max(0, int(delay_ms / 3)))

                carrier = payloads[carrier_idx]

                if break_mode == "concat_payload":
                    yield f"data: {carrier}{DONE_SSE}\n".encode("utf-8")
                else:
                    yield f"{carrier}{DONE_SSE}\n".encode("utf-8")
        else:
            for payload in payloads:
                yield f"data: {payload}\n".encode("utf-8")

        # Metrics (best-effort)
        qid: Optional[int] = None
        try:
            total_ms = int((time.perf_counter() - t0) * 1000)

            MetricsService.record_query_metrics(
                db=db,
                nl_query=req.query,
                normalized_query=(req.query or "").strip().lower(),
                sparql_query="-- demo endpoint (no SPARQL) --",
                kv_results=kv_results_dict,
                is_sequential=False,
                sparql_valid=True,
                query_succeeded=True,
                llm_latency_ms=0,
                sparql_latency_ms=0,
                total_latency_ms=total_ms,
                user_id=(user.user_id if user else None),
                error_message=None,
            )
            db.commit()

            qid = _find_recent_demo_query_metrics_id(db, (user.user_id if user else None), req.query)
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to record demo query metrics: {e}")

        # Persist messages/artifacts with proper nl_query_id
        if persist and conversation is not None:
            try:
                if qid is not None and user_msg is not None:
                    try:
                        user_msg.nl_query_id = qid
                        db.add(user_msg)
                    except Exception:
                        pass

                if deferred_raw_kv and user_message_id is not None:
                    persist_conversation_artifact_from_raw_kv(
                        db=db,
                        conversation=conversation,
                        conversation_message_id=user_message_id,
                        nl_query_id=qid,
                        raw_kv_payload=deferred_raw_kv,
                    )

                persist_assistant_message_and_touch(
                    db=db,
                    conversation=conversation,
                    content=assistant_text or "",
                    nl_query_id=qid,
                )

                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"Failed to persist demo messages/artifacts: {e}")

        if not break_mode:
            yield b"data: [DONE]\n"

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
        stream_demo(),
        media_type="text/event-stream; charset=utf-8",
        headers=headers,
    )
