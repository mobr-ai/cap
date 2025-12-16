# demo_nl.py
import json
import logging
import re
from typing import AsyncGenerator, Optional, Iterator

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import User
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

NL_TOKEN = "__NL__"

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
    "current_trends_spacing_regression": {
        "match": "current trends",
        "assistant_text": (
            "The data shows a peak in activity on December 11th, with the highest number of NFTs minted "
            "(1,271) and accounts created (6,473). However, all metrics sharply declined afterward, with "
            "scripts deployed dropping to 5 on December 15th and multi-assets created falling to 1."
        ),
        "kv": {
            "result_type": "line_chart",
            "data": {
                "values": [
                    {"x": "2025-12-09", "y": "95", "c": 0},
                    {"x": "2025-12-09", "y": "84", "c": 1},
                    {"x": "2025-12-09", "y": "438", "c": 2},
                    {"x": "2025-12-09", "y": "7161", "c": 3},
                    {"x": "2025-12-10", "y": "51", "c": 0},
                    {"x": "2025-12-10", "y": "24", "c": 1},
                    {"x": "2025-12-10", "y": "609", "c": 2},
                    {"x": "2025-12-10", "y": "5806", "c": 3},
                    {"x": "2025-12-11", "y": "24", "c": 0},
                    {"x": "2025-12-11", "y": "17", "c": 1},
                    {"x": "2025-12-11", "y": "1271", "c": 2},
                    {"x": "2025-12-11", "y": "6473", "c": 3},
                    {"x": "2025-12-12", "y": "27", "c": 0},
                    {"x": "2025-12-12", "y": "37", "c": 1},
                    {"x": "2025-12-12", "y": "1385", "c": 2},
                    {"x": "2025-12-12", "y": "3965", "c": 3},
                    {"x": "2025-12-13", "y": "22", "c": 0},
                    {"x": "2025-12-13", "y": "36", "c": 1},
                    {"x": "2025-12-13", "y": "543", "c": 2},
                    {"x": "2025-12-13", "y": "3199", "c": 3},
                    {"x": "2025-12-14", "y": "41", "c": 0},
                    {"x": "2025-12-14", "y": "38", "c": 1},
                    {"x": "2025-12-14", "y": "453", "c": 2},
                    {"x": "2025-12-14", "y": "3759", "c": 3},
                    {"x": "2025-12-15", "y": "5", "c": 0},
                    {"x": "2025-12-15", "y": "1", "c": 1},
                    {"x": "2025-12-15", "y": "40", "c": 2},
                    {"x": "2025-12-15", "y": "288", "c": 3},
                ]
            },
            "metadata": {
                "count": 7,
                "columns": [
                    "date",
                    "scriptsDeployed",
                    "multiAssetsCreated",
                    "nftsMinted",
                    "accountsCreated",
                ],
            },
        },
        "kv_type": "line",
        "artifact_type": "chart",
    },
    "markdown_only_formatting": {
        "match": "markdown formatting test",
        "assistant_text": (
            "# Markdown formatting smoke test\n\n"
            "This response has **no kv artifact** — it is *pure markdown text*.\n\n"
            "## Links\n"
            "- External: https://cardano.org\n"
            "- Explorer example: https://cardanoscan.io/\n\n"
            "## Lists\n"
            "1. Ordered item one\n"
            "2. Ordered item two\n"
            "   - Nested bullet A\n"
            "   - Nested bullet B\n\n"
            "## Code block\n"
            "```bash\n"
            "curl -s http://localhost:8000/api/v1/demo/nl/query \\\n"
            "  -H \"Authorization: Bearer <TOKEN>\" \\\n"
            "  -H \"Content-Type: application/json\" \\\n"
            "  -d '{\"query\":\"markdown formatting test\"}'\n"
            "```\n\n"
            "Inline code: `SELECT 1;`\n\n"
            "## Math\n"
            "Inline: $E=mc^2$  \n"
            "Display:\n"
            "$$\n"
            "\\sum_{i=1}^{n} i = \\frac{n(n+1)}{2}\n"
            "$$\n\n"
            "## Table\n"
            "| Metric | Value |\n"
            "| --- | ---: |\n"
            "| blocks | 5 |\n"
            "| txs | 27 |\n\n"
            "> Blockquote: this should render as a quote.\n"
        ),
    },
    "last_5_proposals": {
        "match": "show the last 5 proposals",
        "kv": {
            "result_type": "table",
            "data": {
                "values": [
                    {
                        "key": "proposalTxHash",
                        "values": [
                            "f8393f1ff814d3d52336a97712361fed933d9ef9e8d0909e1d31536a549fd22f",
                            "d16dffbae9d86a73cb343506e6712d79c278096dc25e8ba6900eb24522726bba",
                            "8f54d021c6e6fcdd5a4908f10a7b092fa31cd94db2e809f2e06d7ffa4d78773d",
                            "3285b7fd0da16d21e0b8f8910c37f77e17a57cfff8f513df4baf692954801088",
                            "03f671791fd97011f30e4d6b76c9a91f4f6bcfb60ee37e5399b9545bb3f2757a",
                        ],
                    },
                    {
                        "key": "proposalUrl",
                        "values": [
                            "<a href=\"https://ipfs.io/ipfs/bafkreiecqskxkmakkrzrs2xs2olh5jcwbuz5qr5gesp6merwcaydcaojiq\" target=\"_blank\">ipfs://bafkreiecqskxkmakkrzrs2xs2olh5jcwbuz5qr5gesp6merwcaydcaojiq</a>",
                            "<a href=\"https://ipfs.io/ipfs/Qmeme8EWugVPQeVghpqB53nvG5U4VT9zy3Ta545fEJPnqL\" target=\"_blank\">ipfs://Qmeme8EWugVPQeVghpqB53nvG5U4VT9zy3Ta545fEJPnqL</a>",
                            "<a href=\"https://ipfs.io/ipfs/bafkreicbxui5lbdrgcpjwhlti3rqkxfnd3vveiinkcu2zak5bny435w4yq\" target=\"_blank\">ipfs://bafkreicbxui5lbdrgcpjwhlti3rqkxfnd3vveiinkcu2zak5bny435w4yq</a>",
                            "<a href=\"https://most-brass-sun.quicknode-ipfs.com/ipfs/QmR7khTUdWyQFdNvyXDsuyZLUNsdfm7Ejo9wKfKdRE3ReG\" target=\"_blank\">https://most-brass-sun.quicknode-ipfs.com/ipfs/QmR7khTUdWyQFdNvyXDsuyZLUNsdfm7Ejo9wKfKdRE3ReG</a>",
                            "<a href=\"https://ipfs.io/ipfs/bafkreidl43ghacdpczaims63glq5kepaa63d63cr5mrpznv56jdm7e2eny\" target=\"_blank\">ipfs://bafkreidl43ghacdpczaims63glq5kepaa63d63cr5mrpznv56jdm7e2eny</a>",
                        ],
                    },
                    {"key": "voteCount", "values": ["27", "144", "276", "217", "226"]},
                    {"key": "yesCount", "values": ["26", "140", "259", "186", "160"]},
                    {"key": "noCount", "values": ["1", "4", "10", "16", "42"]},
                    {"key": "abstainCount", "values": ["0", "0", "7", "15", "24"]},
                    {
                        "key": "proposalTimestamp",
                        "values": [
                            "2025-12-08T22:34:44Z",
                            "2025-11-30T20:13:21Z",
                            "2025-11-27T19:50:18Z",
                            "2025-10-24T07:07:56Z",
                            "2025-10-23T15:59:15Z",
                        ],
                    },
                ]
            },
            "metadata": {
                "count": 5,
                "columns": [
                    "proposalTxHash",
                    "proposalUrl",
                    "voteCount",
                    "yesCount",
                    "noCount",
                    "abstainCount",
                    "proposalTimestamp",
                ],
            },
        },
        "assistant_text": (
            "Here are the last 5 governance proposals (demo dataset) including their IPFS URLs."
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
# SSE-safe text streaming helpers
# ---------------------------------------------------------------------

def _iter_safe_line_chunks(line: str, max_len: int = 96) -> Iterator[str]:
    """
    Chunk a single line (NO '\n' inside) into <= max_len pieces.
    Avoid emitting chunks ending in space/tab (frontend trims them).
    """
    if line is None:
        return
    s = str(line)
    if s == "":
        return
    max_len = max(16, int(max_len))

    i = 0
    carry = ""
    while i < len(s):
        take = s[i : i + max_len]
        i += max_len

        take = carry + take
        carry = ""

        # move trailing spaces/tabs to carry so they aren't lost by frontend trim
        m = re.search(r"[ \t]+$", take)
        if m:
            ws = m.group(0)
            take = take[: -len(ws)]
            carry = ws + carry

        if take:
            yield take

    # if we only have carry left at the end, drop it (frontend would drop anyway)


def iter_sse_markdown_events(text: str, max_len: int = 96) -> Iterator[str]:
    """
    Produce a stream of 'data:' payload strings.
    We emit NL_TOKEN as its own payload for every newline in the original text.
    """
    if not text:
        return
    raw = str(text).replace("\r\n", "\n").replace("\r", "\n")

    # split keeping line boundaries
    lines = raw.split("\n")

    for idx, line in enumerate(lines):
        # emit the line content (may be empty)
        for chunk in _iter_safe_line_chunks(line, max_len=max_len):
            yield chunk

        # after every line except the last, emit newline token (including blank lines)
        if idx < len(lines) - 1:
            yield NL_TOKEN


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
        "'Show the last 5 proposals.' or "
        "'Monthly multi assets created in 2021.'"
    )

    async def stream_demo() -> AsyncGenerator[bytes, None]:
        yield b"status: Planning...\n"
        yield b"status: Querying knowledge graph...\n"

        # KV block
        if scene and scene.get("kv"):
            yield b"kv_results:\n"
            raw_kv = json.dumps(scene["kv"])
            yield (raw_kv + "\n").encode("utf-8")
            yield b"_kv_results_end_\n"

            if persist and conversation is not None:
                try:
                    persist_conversation_artifact_from_raw_kv(
                        db=db,
                        conversation=conversation,
                        conversation_message_id=user_message_id,
                        nl_query_id=None,
                        raw_kv_payload=raw_kv,
                    )
                except Exception as e:
                    db.rollback()
                    logger.error(f"Failed to persist demo artifact: {e}")

        yield b"status: Writing answer...\n"

        # Stream markdown as SSE events, using NL_TOKEN for newlines
        for payload in iter_sse_markdown_events(assistant_text or "", max_len=96):
            yield f"data: {payload}\n".encode("utf-8")

        # Persist assistant message BEFORE done
        if persist and conversation is not None:
            try:
                persist_assistant_message_and_touch(
                    db=db,
                    conversation=conversation,
                    content=assistant_text or "",
                    nl_query_id=None,
                )
                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"Failed to persist demo assistant message: {e}")

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
