# cap/services/conversation_persistence.py

from __future__ import annotations
import hashlib
import json, re
from datetime import datetime
from typing import Optional, Tuple, Any, Dict, List
from fastapi import HTTPException
from sqlalchemy.orm import Session

from cap.database.model import Conversation, ConversationMessage, User, ConversationArtifact

_WS_RE = re.compile(r"[ \t]+")
_SPACE_BEFORE_PUNCT_RE = re.compile(r"[ \t]+([,.;:!?])")
# do NOT touch markdown-required leading indentation for code blocks/quotes/lists
_MARKDOWN_PREFIX_RE = re.compile(r"^(\s{0,3}(?:```|~~~|>|\* |- |\d+\. ))")

# Split on fenced code blocks and inline code, keep delimiters
_CODE_SPLIT_RE = re.compile(r"(```[\s\S]*?```|`[^`]*`)")

def _artifact_hash(payload: Dict[str, Any]) -> str:
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
    if not conversation:
        return None

    raw = (raw_kv_payload or "").strip()
    if not raw:
        return None

    if raw.startswith("kv_results:"):
        raw = raw[len("kv_results:") :].strip()

    kv: Optional[Dict[str, Any]] = None
    try:
        kv = json.loads(raw)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            kv = json.loads(m.group(0))

    if not isinstance(kv, dict):
        return None

    result_type = kv.get("result_type") or kv.get("resultType")
    kv_type = _normalize_kv_type(result_type)
    artifact_type = "table" if kv_type == "table" else "chart"
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
        "conversation_message_id": conversation_message_id,
    }
    h = _artifact_hash(payload_for_hash)

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

def _repair_word_glue_unicode(text: str) -> str:
    """
    Language-agnostic "de-glue" pass for prose:
      - inserts a space between: lower-case letter + Upper-case letter (when it looks like a word start)
      - inserts a space between: letter + digit (when likely prose, e.g. "Dec15th" -> "Dec 15th")
    Uses Unicode-aware str.islower/isupper/isalpha/isdigit.

    IMPORTANT: run only on non-code markdown segments (caller must split).
    """
    if not text:
        return ""

    out: list[str] = []
    n = len(text)

    for i, ch in enumerate(text):
        prev = text[i - 1] if i > 0 else ""
        nxt = text[i + 1] if i + 1 < n else ""

        # Rule A: lower + Upper + lower => insert space before Upper (word boundary)
        # Example: "inOctober" -> "in October"
        if ch and prev:
            if prev.isalpha() and prev.islower() and ch.isalpha() and ch.isupper():
                # be conservative: only if next is a lowercase letter (likely "WordStart")
                if nxt and nxt.isalpha() and nxt.islower():
                    # avoid double spaces
                    if out and out[-1] != " ":
                        out.append(" ")

        # Rule B: letter + digit => insert space before digit (prose boundary)
        # Example: "December15th" -> "December 15th"
        if ch and prev:
            if prev.isalpha() and ch.isdigit():
                if out and out[-1] != " ":
                    out.append(" ")

        out.append(ch)

    return "".join(out)

def _repair_word_glue_markdown(md: str) -> str:
    """
    Apply _repair_word_glue_unicode() outside of fenced code blocks and inline code.
    """
    if not md:
        return ""

    parts = _CODE_SPLIT_RE.split(md)
    fixed: list[str] = []

    for p in parts:
        if not p:
            continue
        if p.startswith("```") or (p.startswith("`") and p.endswith("`")):
            fixed.append(p)
        else:
            fixed.append(_repair_word_glue_unicode(p))

    return "".join(fixed)

def normalize_assistant_content(text: str) -> str:
    """
    Markdown-safe normalization for storage:
      - preserves newlines
      - collapses runs of spaces/tabs *within a line* (except markdown-indented lines)
      - removes spaces before punctuation (within a line)
      - collapses 3+ blank lines to 2
      - then applies a language-agnostic "word glue" repair outside code spans
    """
    if not text:
        return ""

    src = str(text).replace("\r\n", "\n").replace("\r", "\n")

    out_lines = []
    for line in src.split("\n"):
        if _MARKDOWN_PREFIX_RE.match(line):
            out_lines.append(line.rstrip())
            continue

        t = _WS_RE.sub(" ", line).rstrip()
        t = _SPACE_BEFORE_PUNCT_RE.sub(r"\1", t)
        out_lines.append(t)

    normalized = "\n".join(out_lines)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)

    # Language-agnostic de-glue pass (do NOT touch code spans)
    normalized = _repair_word_glue_markdown(normalized)

    return normalized.strip()

def get_or_create_conversation(
    db: Session,
    user: User,
    conversation_id: Optional[int],
    query_for_title: str,
) -> Conversation:
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
) -> ConversationMessage:
    safe = normalize_assistant_content(content or "")

    msg = ConversationMessage(
        conversation_id=conversation.id,
        user_id=None,
        role="assistant",
        content=safe,
        nl_query_id=nl_query_id,
    )
    db.add(msg)

    conversation.updated_at = datetime.utcnow()
    db.add(conversation)
    return msg

def start_conversation_and_persist_user(
    db: Session,
    user: Optional[User],
    conversation_id: Optional[int],
    query: str,
    nl_query_id: Optional[int] = None,
) -> Tuple[Optional[Conversation], Optional[ConversationMessage]]:
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
