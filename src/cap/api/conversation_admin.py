# cap/api/conversation_admin.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import Conversation, ConversationMessage
from cap.services.conversation_persistence import list_conversation_artifacts
from cap.core.auth_dependencies import get_current_admin_user

router = APIRouter(
    prefix="/api/v1/admin/conversations",
    tags=["admin_conversations"],
)

@router.get("/{conversation_id}")
def get_conversation_admin(
    conversation_id: int,
    db: Session = Depends(get_db),
    admin=Depends(get_current_admin_user),
):
    convo = db.query(Conversation).filter(Conversation.id == conversation_id).first()
    if not convo:
        raise HTTPException(status_code=404, detail="Conversation not found")

    messages = (
        db.query(ConversationMessage)
        .filter(ConversationMessage.conversation_id == convo.id)
        .order_by(ConversationMessage.created_at.asc(), ConversationMessage.id.asc())
        .all()
    )

    artifacts = list_conversation_artifacts(db, convo.id)

    return {
        "id": convo.id,
        "title": convo.title,
        "created_at": convo.created_at.isoformat() if convo.created_at else None,
        "updated_at": convo.updated_at.isoformat() if convo.updated_at else None,
        "owner_user_id": convo.user_id,
        "messages": [
            {
                "id": m.id,
                "conversation_id": m.conversation_id,
                "user_id": m.user_id,
                "role": m.role,
                "content": m.content,
                "nl_query_id": m.nl_query_id,
                "created_at": m.created_at.isoformat() if m.created_at else None,
            }
            for m in messages
        ],
        "artifacts": [
            {
                "id": a.id,
                "conversation_id": a.conversation_id,
                "nl_query_id": getattr(a, "nl_query_id", None),
                "conversation_message_id": getattr(a, "conversation_message_id", None),
                "artifact_type": a.artifact_type,
                "kv_type": getattr(a, "kv_type", None),
                "config": a.config,
                "artifact_hash": a.artifact_hash,
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in artifacts
        ],
    }
