import json
import logging
from typing import AsyncGenerator, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import Conversation, User
from cap.core.auth_dependencies import (
    bearer_scheme,
    _extract_token,
    _decode,
    _extract_user_id,
)
from cap.services.conversation_persistence import (
    start_conversation_and_persist_user,
    persist_assistant_message_and_touch,
    persist_conversation_artifact_from_raw_kv,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/demo", tags=["demo"])


# ---------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------

class DemoQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000)
    conversation_id: Optional[int] = None


# ---------------------------------------------------------------------------
# Demo scenes (UNCHANGED)
# ---------------------------------------------------------------------------
# IMPORTANT: These payloads MUST match what the frontend expects:
# - Tables: kv.data.values = [{ "key": <colName>, "values": [...] }, ...]
# - Charts: kv.metadata.columns = [xField, yField, (optional) cField]
#          kv.data.values = [{ [xField]: ..., [yField]: ..., [cField]: ... }, ...]
DEMO_SCENES = {
    "latest_5_blocks": {
        "match": "list the latest 5 blocks",
        "kv": {
            "result_type": "table",
            "data": {
                "values": [
                    {
                        "key": "blockNumber",
                        "values": ["5979789", "5979788", "5979787", "5979786", "5979785"],
                    },
                    {
                        "key": "slotNumber",
                        "values": ["34724642", "34724623", "34724618", "34724610", "34724609"],
                    },
                    {"key": "epochNumber", "values": ["80", "80", "80", "80", "80"]},
                    {
                        "key": "timestamp",
                        "values": [
                            "2021-07-14T19:28:53Z",
                            "2021-07-14T19:28:34Z",
                            "2021-07-14T19:28:29Z",
                            "2021-07-14T19:28:21Z",
                            "2021-07-14T19:28:20Z",
                        ],
                    },
                    {"key": "blockSize", "values": ["2518", "1197", "573", "3", "18899"]},
                    {
                        "key": "hash",
                        "values": [
                            "7ebff2ab745f908ff0d06cea3830c1b1c6020045c4a751e1f223e9a091fd881b",
                            "7b607fdbc570eb0375d3928a945b92bf25fa7ff38cc14a4545c2086238431f47",
                            "565607225351bdf21376f0a4f9cbbce90ca953102778f96c69549a9f985053d4",
                            "490c064b83d37fbc648db61bac223fdad37f1831f01b90b49fda0359c0aad543",
                            "b223bdabc24fa85b94c33db9c6ac382f022ff06a40851da0ced5c1a430301854",
                        ],
                    },
                    {"key": "transactionCount", "values": ["7", "1", "2", "0", "27"]},
                ]
            },
            "metadata": {
                "count": 5,
                "columns": [
                    "blockNumber",
                    "slotNumber",
                    "epochNumber",
                    "timestamp",
                    "blockSize",
                    "hash",
                    "transactionCount",
                ],
            },
        },
        "assistant_text": (
            "Here are the latest 5 blocks. In a production deployment, this table would be "
            "generated from on-chain block headers and enriched with derived fields."
        ),
    },
    "monthly_multiassets_2021": {
        "match": "monthly multi assets created in 2021",
        "kv": {
            "result_type": "bar_chart",
            "metadata": {"columns": ["yearMonth", "deployments"]},
            "data": {
                "values": [
                    {"yearMonth": "2021-01", "deployments": 120},
                    {"yearMonth": "2021-02", "deployments": 220},
                    {"yearMonth": "2021-03", "deployments": 540},
                    {"yearMonth": "2021-04", "deployments": 610},
                    {"yearMonth": "2021-05", "deployments": 430},
                    {"yearMonth": "2021-06", "deployments": 720},
                    {"yearMonth": "2021-07", "deployments": 980},
                    {"yearMonth": "2021-08", "deployments": 860},
                    {"yearMonth": "2021-09", "deployments": 650},
                    {"yearMonth": "2021-10", "deployments": 770},
                    {"yearMonth": "2021-11", "deployments": 690},
                    {"yearMonth": "2021-12", "deployments": 910},
                ]
            },
        },
        "assistant_text": (
            "This bar chart shows a demo monthly count of native assets created in 2021. "
            "In production, this would be computed from minting policies and asset creation events."
        ),
    },
    "top_1pct_ada_supply": {
        "match": "top 1% ada holders",
        "kv": {
            "result_type": "pie_chart",
            "data": {
                "values": [
                    {"label": "Top 1%", "value": 56.93},
                    {"label": "Other 99%", "value": 43.07},
                ]
            },
        },
        "assistant_text": (
            "The pie chart indicates the top 1% of ADA holders control a significant share of supply "
            "in this demo dataset. In production, this is computed from stake distribution and address clustering."
        ),
    },
    "monthly_tx_and_outputs": {
        "match": "monthly number of transactions and outputs",
        "kv": {
            "result_type": "line_chart",
            "metadata": {"columns": ["yearMonth", "txCount", "outputCount"]},
            "data": {
                "values": [
                    {"yearMonth": "2021-01", "txCount": 1100000, "outputCount": 3100000},
                    {"yearMonth": "2021-02", "txCount": 1200000, "outputCount": 3300000},
                    {"yearMonth": "2021-03", "txCount": 1350000, "outputCount": 3650000},
                    {"yearMonth": "2021-04", "txCount": 1500000, "outputCount": 4000000},
                    {"yearMonth": "2021-05", "txCount": 1420000, "outputCount": 3920000},
                    {"yearMonth": "2021-06", "txCount": 1600000, "outputCount": 4300000},
                    {"yearMonth": "2021-07", "txCount": 1750000, "outputCount": 4700000},
                    {"yearMonth": "2021-08", "txCount": 1680000, "outputCount": 4550000},
                    {"yearMonth": "2021-09", "txCount": 1550000, "outputCount": 4200000},
                    {"yearMonth": "2021-10", "txCount": 1620000, "outputCount": 4380000},
                    {"yearMonth": "2021-11", "txCount": 1580000, "outputCount": 4320000},
                    {"yearMonth": "2021-12", "txCount": 1700000, "outputCount": 4600000},
                ]
            },
        },
        "assistant_text": (
            "This line chart shows a demo monthly series for transactions and outputs. "
            "In production, these would be computed from transaction bodies and UTxO outputs per period."
        ),
    },
}


def pick_scene(query: str):
    q = (query or "").strip().lower()
    for scene in DEMO_SCENES.values():
        if scene["match"] in q:
            return scene
    return None


# ---------------------------------------------------------------------
# Authentication (best-effort)
# ---------------------------------------------------------------------

def get_optional_user(
    request: Request,
    db: Session = Depends(get_db),
    creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> Optional[User]:
    token = _extract_token(request, creds)
    if not token:
        return None

    try:
        payload = _decode(token)
        user_id = _extract_user_id(payload)
        return db.get(User, user_id)
    except Exception:
        return None


# ---------------------------------------------------------------------
# Demo NL endpoint
# ---------------------------------------------------------------------

@router.post("/nl/query")
async def demo_nl_query(
    req: DemoQueryRequest,
    request: Request,
    db: Session = Depends(get_db),
    user: Optional[User] = Depends(get_optional_user),
):
    scene = pick_scene(req.query)

    # -------------------------------------------------------------
    # 1) Conversation + user message (only if authenticated)
    # -------------------------------------------------------------

    conversation = None
    user_msg = None
    persist = user is not None

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
        "'Plot a bar chart showing monthly multi assets created in 2021.'"
    )

    # -------------------------------------------------------------
    # 2) Streaming logic (MATCHES REAL NL ENDPOINT)
    # -------------------------------------------------------------

    async def stream_demo() -> AsyncGenerator[bytes, None]:
        # Status messages
        yield b"status: Planning...\n"
        yield b"status: Querying knowledge graph...\n"

        # Emit KV block
        if scene and scene.get("kv"):
            yield b"kv_results:\n"
            yield (json.dumps(scene["kv"]) + "\n").encode("utf-8")
            yield b"_kv_results_end_\n"

            if persist and conversation is not None:
                try:
                    persist_conversation_artifact_from_raw_kv(
                        db=db,
                        conversation=conversation,
                        conversation_message_id=user_message_id,
                        nl_query_id=None,
                        raw_kv_payload=json.dumps(scene["kv"]),
                    )
                except Exception as e:
                    db.rollback()
                    logger.error(f"Failed to persist demo artifact: {e}")

        # Assistant text (streamed word by word)
        yield b"status: Writing answer...\n"
        for part in assistant_text.split(" "):
            yield (part + " ").encode("utf-8")

        yield b"\n"

        # Persist assistant message
        if persist and conversation is not None:
            try:
                persist_assistant_message_and_touch(
                    db=db,
                    conversation=conversation,
                    content=assistant_text,
                    nl_query_id=None,
                )
            except Exception as e:
                db.rollback()
                logger.error(f"Failed to persist demo assistant message: {e}")

    # -------------------------------------------------------------
    # 3) Headers (IDENTICAL to real NL endpoint)
    # -------------------------------------------------------------

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
        stream_demo(),
        media_type="text/event-stream",
        headers=headers,
    )
