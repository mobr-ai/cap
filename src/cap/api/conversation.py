# cap/api/conversation.py

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import Conversation, ConversationMessage
from cap.services.conversation_persistence import list_conversation_artifacts
from cap.core.auth_dependencies import get_current_user

router = APIRouter(
    prefix="/api/v1/conversations",
    tags=["conversations"],
)


# ---------- Schemas ----------

class ConversationSummary(BaseModel):
    id: int
    title: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    last_message_preview: Optional[str] = None

    model_config = {"from_attributes": True}


class ConversationCreate(BaseModel):
    title: Optional[str] = Field(None, max_length=255)


class ConversationMessageOut(BaseModel):
    id: int
    conversation_id: int
    user_id: Optional[int]
    role: str
    content: str
    nl_query_id: Optional[int]
    created_at: datetime

    model_config = {"from_attributes": True}


class ConversationMessageCreate(BaseModel):
    role: str = Field(..., pattern="^(user|assistant|system)$")
    content: str = Field(..., min_length=1)
    nl_query_id: Optional[int] = None


class ConversationArtifactOut(BaseModel):
    id: int
    conversation_id: int
    nl_query_id: Optional[int] = None
    conversation_message_id: Optional[int] = None
    artifact_type: str
    kv_type: Optional[str] = None
    config: dict
    artifact_hash: str
    created_at: datetime

    model_config = {"from_attributes": True}


class ConversationArtifactCreate(BaseModel):
    artifact_type: str = Field(..., pattern="^(chart|table)$")
    kv_type: Optional[str] = None
    config: dict
    nl_query_id: Optional[int] = None
    conversation_message_id: Optional[int] = None


class ConversationDetail(BaseModel):
    id: int
    title: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    messages: List[ConversationMessageOut]
    artifacts: List[ConversationArtifactOut] = []


class ConversationUpdate(BaseModel):
    title: Optional[str] = Field(None, max_length=255)


# ---------- Helpers ----------

def _ensure_owns_conversation(
    db: Session,
    user_id: int,
    conversation_id: int,
) -> Conversation:
    convo = (
        db.query(Conversation)
        .filter(Conversation.id == conversation_id, Conversation.user_id == user_id)
        .first()
    )
    if not convo:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return convo


def _last_message_preview(db: Session, conversation_id: int) -> Optional[str]:
    last_msg = (
        db.query(ConversationMessage)
        .filter(ConversationMessage.conversation_id == conversation_id)
        .order_by(ConversationMessage.created_at.desc(), ConversationMessage.id.desc())
        .first()
    )
    if not last_msg:
        return None
    return last_msg.content[:120] + "…" if len(last_msg.content) > 120 else last_msg.content


# ---------- Routes ----------

@router.get("/", response_model=List[ConversationSummary])
def list_conversations(
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
    limit: int = 50,
):
    limit = max(1, min(limit, 100))

    # Prefer updated_at so "touched" conversations float to the top.
    conversations = (
        db.query(Conversation)
        .filter(Conversation.user_id == user.user_id)
        .order_by(Conversation.updated_at.desc().nullslast(), Conversation.created_at.desc())
        .limit(limit)
        .all()
    )

    summaries: List[ConversationSummary] = []
    for c in conversations:
        preview = _last_message_preview(db, c.id)

        summaries.append(
            ConversationSummary(
                id=c.id,
                title=c.title,
                created_at=c.created_at,
                updated_at=c.updated_at,
                last_message_preview=preview,
            )
        )

    return summaries


@router.post(
    "/",
    response_model=ConversationSummary,
    status_code=status.HTTP_201_CREATED,
)
def create_conversation(
    payload: ConversationCreate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    convo = Conversation(
        user_id=user.user_id,
        title=payload.title.strip() if payload.title else None,
    )
    db.add(convo)
    db.commit()
    db.refresh(convo)

    return ConversationSummary(
        id=convo.id,
        title=convo.title,
        created_at=convo.created_at,
        updated_at=convo.updated_at,
        last_message_preview=None,
    )


@router.get("/{conversation_id}", response_model=ConversationDetail)
def get_conversation(
    conversation_id: int,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    convo = _ensure_owns_conversation(db, user.user_id, conversation_id)

    messages = (
        db.query(ConversationMessage)
        .filter(ConversationMessage.conversation_id == convo.id)
        .order_by(ConversationMessage.created_at.asc(), ConversationMessage.id.asc())
        .all()
    )

    # ✅ IMPORTANT: include persisted artifacts so the frontend can restore them
    artifacts = list_conversation_artifacts(db, convo.id)

    return ConversationDetail(
        id=convo.id,
        title=convo.title,
        created_at=convo.created_at,
        updated_at=convo.updated_at,
        messages=[ConversationMessageOut.model_validate(m) for m in messages],
        artifacts=[ConversationArtifactOut.model_validate(a) for a in artifacts],
    )


@router.post(
    "/{conversation_id}/messages",
    response_model=ConversationMessageOut,
    status_code=status.HTTP_201_CREATED,
)
def add_message(
    conversation_id: int,
    payload: ConversationMessageCreate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    convo = _ensure_owns_conversation(db, user.user_id, conversation_id)

    msg = ConversationMessage(
        conversation_id=convo.id,
        user_id=user.user_id if payload.role == "user" else None,
        role=payload.role,
        content=payload.content,
        nl_query_id=payload.nl_query_id,
    )
    db.add(msg)

    # touch conversation for ordering parity
    convo.updated_at = datetime.utcnow()
    db.add(convo)

    db.commit()
    db.refresh(msg)

    return msg


@router.patch("/{conversation_id}", response_model=ConversationSummary)
def update_conversation(
    conversation_id: int,
    payload: ConversationUpdate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    convo = _ensure_owns_conversation(db, user.user_id, conversation_id)

    if payload.title is not None:
        convo.title = payload.title.strip() if payload.title else None

    convo.updated_at = datetime.utcnow()
    db.add(convo)
    db.commit()
    db.refresh(convo)

    preview = _last_message_preview(db, convo.id)

    return ConversationSummary(
        id=convo.id,
        title=convo.title,
        created_at=convo.created_at,
        updated_at=convo.updated_at,
        last_message_preview=preview,
    )


@router.delete("/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_conversation(
    conversation_id: int,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    convo = _ensure_owns_conversation(db, user.user_id, conversation_id)

    # delete messages then conversation (safe even without cascade)
    db.query(ConversationMessage).filter(
        ConversationMessage.conversation_id == convo.id
    ).delete(synchronize_session=False)

    db.delete(convo)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/{conversation_id}/artifacts", response_model=List[ConversationArtifactOut])
def get_conversation_artifacts(
    conversation_id: int,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    convo = _ensure_owns_conversation(db, user.user_id, conversation_id)
    artifacts = list_conversation_artifacts(db, convo.id)
    return [ConversationArtifactOut.model_validate(a) for a in artifacts]
