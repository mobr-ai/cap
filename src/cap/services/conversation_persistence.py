# cap/services/conversation_persistence.py

from __future__ import annotations
import hashlib
import json, re
from datetime import datetime
from typing import Optional, Tuple, Any, Dict, List
from fastapi import HTTPException
from sqlalchemy.orm import Session

from cap.database.model import Conversation, ConversationMessage, User, ConversationArtifact

_WS_RE = re.compile(r"\s+")
_SPACE_BEFORE_PUNCT_RE = re.compile(r"\s+([,.;:!?])")
_DIGIT_SPACES_RE = re.compile(r"(\d)\s+(?=\d)")
_ORDINAL_RE = re.compile(r"(\d)\s+(st|nd|rd|th)\b", re.IGNORECASE)
_SUFFIX_RE = re.compile(r"\b([A-Za-z]{3,})\s+(ed|ing|ly|er|ers|est|s|es)\b")
_HYPHEN_RE = re.compile(r"\b(\w+)\s*-\s*(\w+)\b")

_CAPS_SPLIT_RE = re.compile(r"\b([A-Z])\s+([A-Z]{2,16})\b")
_CAMEL_SPLIT_RE = re.compile(r"\b([A-Z][a-z]{1,3})\s+([A-Z][a-z]{1,3})\b")
_LOWER_UPPER_RE = re.compile(r"\b([a-z])\s+([A-Z][a-z]{2,})\b")
_PAREN_OPEN_RE = re.compile(r"\(\s+")
_PAREN_CLOSE_RE = re.compile(r"\s+\)")
_COMMA_IN_NUMBER_RE = re.compile(r"(\d)\s*,\s*(\d)")
_DECIMAL_RE = re.compile(r"(\d)\s*\.\s*(\d)")
_ACRONYM_PLURAL_RE = re.compile(r"\b([A-Z]{2,10})\s+(s|es)\b")
_ACRONYM_DESPACE_RE = re.compile(r"\b([A-Z])\s+(?=[A-Z])")


def _artifact_hash(payload: Dict[str, Any]) -> str:
    # stable JSON encoding
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _normalize_kv_type(result_type: Optional[str]) -> Optional[str]:
    s = (result_type or "").strip().lower()
    if not s:
        return None
    if s == "table":
        return "table"
    if s == "pie_chart":
        return "pie"
    if s == "bar_chart":
        return "bar"
    if s == "line_chart":
        return "line"
    if s.endswith("_chart"):
        s = s[: -len("_chart")]
    return s


def persist_conversation_artifact_from_raw_kv(
    db: Session,
    conversation: Conversation,
    raw_kv_payload: str,
    nl_query_id: Optional[int] = None,
    conversation_message_id: Optional[int] = None,
) -> Optional[ConversationArtifact]:
    """
    Parse raw kv_results JSON string and persist as ConversationArtifact.

    Stores the full kv payload under config={"kv": <parsed kv json>}
    so the frontend can reconstruct charts/tables reliably.
    """
    if not conversation:
        return None

    raw = (raw_kv_payload or "").strip()
    if not raw:
        return None

    # Some streams may include prefix "kv_results:" — tolerate it.
    if raw.startswith("kv_results:"):
        raw = raw[len("kv_results:") :].strip()

    # Try JSON parse (with a small rescue if extra text exists)
    kv: Optional[Dict[str, Any]] = None
    try:
        kv = json.loads(raw)
    except Exception:
        # rescue: find first {...} block
        import re

        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            kv = json.loads(m.group(0))

    if not isinstance(kv, dict):
        return None

    result_type = kv.get("result_type") or kv.get("resultType")
    kv_type = _normalize_kv_type(result_type)

    # Determine artifact_type: table vs chart
    artifact_type = "table" if kv_type == "table" else "chart"

    # Persist full kv payload for deterministic re-rendering
    config: Dict[str, Any] = {"kv": kv}

    return persist_conversation_artifact(
        db=db,
        conversation=conversation,
        artifact_type=artifact_type,
        kv_type=kv_type,
        config=config,
        nl_query_id=nl_query_id,
        conversation_message_id=conversation_message_id,
    )


def persist_conversation_artifact(
    db: Session,
    conversation: Conversation,
    artifact_type: str,
    config: Dict[str, Any],
    kv_type: Optional[str] = None,
    nl_query_id: Optional[int] = None,
    conversation_message_id: Optional[int] = None,
) -> Optional[ConversationArtifact]:
    if not conversation:
        return None

    payload_for_hash = {
        "artifact_type": artifact_type,
        "kv_type": kv_type,
        "config": config,
        "conversation_message_id": conversation_message_id,  # <- important
    }
    h = _artifact_hash(payload_for_hash)

    # idempotent upsert by (conversation_id, artifact_hash)
    existing = (
        db.query(ConversationArtifact)
        .filter(
            ConversationArtifact.conversation_id == conversation.id,
            ConversationArtifact.artifact_hash == h,
        )
        .first()
    )
    if existing:
        return existing

    a = ConversationArtifact(
        conversation_id=conversation.id,
        nl_query_id=nl_query_id,
        conversation_message_id=conversation_message_id,
        artifact_type=artifact_type,
        kv_type=kv_type,
        config=config,
        artifact_hash=h,
    )
    db.add(a)

    # touch conversation so it bumps correctly in UI
    conversation.updated_at = datetime.utcnow()
    db.add(conversation)

    db.commit()
    db.refresh(a)
    return a


def list_conversation_artifacts(
    db: Session,
    conversation_id: int,
) -> List[ConversationArtifact]:
    return (
        db.query(ConversationArtifact)
        .filter(ConversationArtifact.conversation_id == conversation_id)
        .order_by(ConversationArtifact.created_at.asc(), ConversationArtifact.id.asc())
        .all()
    )


def _title_from_query(query: str) -> Optional[str]:
    title = (query or "").strip()
    if not title:
        return None
    if len(title) > 80:
        title = title[:77] + "..."
    return title


def normalize_assistant_content(text: str) -> str:
    if not text:
        return ""

    t = str(text)

    # collapse whitespace early
    t = _WS_RE.sub(" ", t).strip()

    # remove spaces before punctuation
    t = _SPACE_BEFORE_PUNCT_RE.sub(r"\1", t)

    # fix parentheses spacing: "( 1, 271 )" -> "(1, 271)"
    t = _PAREN_OPEN_RE.sub("(", t)
    t = _PAREN_CLOSE_RE.sub(")", t)

    # join digit runs: "2 0 2 5" -> "2025"
    t = _DIGIT_SPACES_RE.sub(r"\1", t)

    # ordinal join: "11 th" -> "11th"
    t = _ORDINAL_RE.sub(r"\1\2", t)

    # comma/decimal inside numbers: "1, 271" -> "1,271" ; "3 . 14" -> "3.14"
    t = _COMMA_IN_NUMBER_RE.sub(r"\1,\2", t)
    t = _DECIMAL_RE.sub(r"\1.\2", t)

    # suffix join: "mint ed" -> "minted"
    t = _SUFFIX_RE.sub(r"\1\2", t)

    # join consecutive spaced capitals: "N FTs" -> "NFTs", "N F T s" -> "NFT s" (then plural rule fixes)
    t = _ACRONYM_DESPACE_RE.sub(r"\1", t)

    # acronym plural: "NFT s" -> "NFTs"
    t = _ACRONYM_PLURAL_RE.sub(r"\1\2", t)

    # normalize hyphen spacing: "early -stage" -> "early-stage"
    t = _HYPHEN_RE.sub(r"\1-\2", t)

    # join common split patterns; multiple passes helps
    stop_left = {
        "The", "This", "That", "These", "Those",
        "A", "An",
        "In", "On", "At", "As", "For", "From", "To", "By", "Of", "With",
        "And", "Or", "But", "So", "Yet",
        "It", "Its", "Is", "Are", "Was", "Were",
        "However", "While", "When", "Where", "What", "Why", "How",
    }

    for _ in range(2):
        t = _ACRONYM_DESPACE_RE.sub(r"\1", t)
        t = _CAPS_SPLIT_RE.sub(r"\1\2", t)     # "N IGHT" -> "NIGHT"
        t = _CAMEL_SPLIT_RE.sub(r"\1\2", t)    # "De Fi" -> "DeFi"
        t = _LOWER_UPPER_RE.sub(r"\1\2", t)    # "d Apps" -> "dApps"

        # Safer "Card ano" join:
        # only join when the right chunk is a small suffix (2-5 chars)
        # and the left chunk is not a common stopword like "The".
        t = re.sub(
            r"\b([A-Z][a-z]{2,10})\s+([a-z]{2,5})\b",
            lambda m: (m.group(1) + m.group(2)) if (m.group(1) not in stop_left) else m.group(0),
            t,
        )

    return t.strip()


def get_or_create_conversation(
    db: Session,
    user: User,
    conversation_id: Optional[int],
    query_for_title: str,
) -> Conversation:
    """
    Returns an existing conversation (if conversation_id provided and owned by user),
    otherwise creates a new conversation with title from query snippet.
    """
    if conversation_id is not None:
        convo = (
            db.query(Conversation)
            .filter(Conversation.id == conversation_id, Conversation.user_id == user.user_id)
            .first()
        )
        if not convo:
            raise HTTPException(status_code=404, detail="Conversation not found")
        return convo

    convo = Conversation(
        user_id=user.user_id,
        title=_title_from_query(query_for_title),
    )
    db.add(convo)
    db.commit()
    db.refresh(convo)
    return convo


def persist_user_message(
    db: Session,
    conversation_id: int,
    user_id: int,
    content: str,
    nl_query_id: Optional[int] = None,
) -> ConversationMessage:
    msg = ConversationMessage(
        conversation_id=conversation_id,
        user_id=user_id,
        role="user",
        content=content,
        nl_query_id=nl_query_id,
    )
    db.add(msg)
    db.commit()
    db.refresh(msg)
    return msg


def persist_assistant_message_and_touch(
    db: Session,
    conversation: Conversation,
    content: str,
    nl_query_id: Optional[int] = None,
) -> Optional[ConversationMessage]:
    text = normalize_assistant_content((content or "").strip())
    if not text:
        return None

    msg = ConversationMessage(
        conversation_id=conversation.id,
        user_id=None,
        role="assistant",
        content=text,
        nl_query_id=nl_query_id,
    )
    db.add(msg)

    conversation.updated_at = datetime.utcnow()
    db.add(conversation)

    db.commit()
    db.refresh(msg)
    return msg


def start_conversation_and_persist_user(
    db: Session,
    user: Optional[User],
    conversation_id: Optional[int],
    query: str,
    nl_query_id: Optional[int] = None,
) -> Tuple[Optional[Conversation], Optional[ConversationMessage]]:
    """
    Convenience wrapper.
    If user is None: returns (None, None) (no persistence).
    Otherwise: ensures conversation exists, persists user message, returns both.
    """
    if user is None:
        return None, None

    convo = get_or_create_conversation(db, user, conversation_id, query_for_title=query)
    user_msg = persist_user_message(
        db,
        conversation_id=convo.id,
        user_id=user.user_id,
        content=query,
        nl_query_id=nl_query_id,
    )
    return convo, user_msg
