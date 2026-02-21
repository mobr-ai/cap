from typing import Any, Dict, Literal, Optional, List

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.services.metrics_service import MetricsService
from cap.database.model import Dashboard, DashboardItem, Conversation
from cap.core.auth_dependencies import get_current_user

router = APIRouter(
    prefix="/api/v1/dashboard",
    tags=["dashboard"],
)

# ---------- Schemas ----------

class DashboardBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = Field(None, max_length=255)


class DashboardOut(DashboardBase):
    id: int
    is_default: bool
    model_config = {"from_attributes": True}


class DashboardCreate(DashboardBase):
    is_default: bool = False


class DashboardItemBase(BaseModel):
    artifact_type: str = Field(..., pattern="^(table|chart)$")
    title: str = Field(..., max_length=150)
    source_query: Optional[str] = Field(None, max_length=1000)
    config: dict

    conversation_message_id: Optional[int] = Field(
        None, description="Originating conversation message id"
    )

    conversation_id: Optional[int] = Field(
        None, description="Originating conversation id"
    )


class DashboardItemOut(DashboardItemBase):
    id: int
    dashboard_id: int
    position: int

    # index-of-total, only valid when order=position (manual)
    position_min: Optional[int] = None
    position_max: Optional[int] = None

    # tells frontend whether move up/down should be enabled
    can_reorder: bool = False

    created_at: datetime
    conversation_title: Optional[str] = None
    model_config = {"from_attributes": True}


class PinRequest(DashboardItemBase):
    dashboard_id: Optional[int] = None  # null → use/create default


class DashboardItemUpdate(BaseModel):
    title: Optional[str] = Field(None, max_length=150)
    config_patch: Optional[Dict[str, Any]] = Field(default=None)
    # legacy
    move: Optional[Literal["up", "down"]] = None
    # explicit swap target (visual order)
    swap_with_id: Optional[int] = None
    order: Optional[str] = None

# ---------- Helpers ----------
def _item_position_index(
    db: Session,
    dashboard_id: int,
    item_id: int,
    order_by,
) -> tuple[int, int]:
    rows = (
        db.query(DashboardItem.id)
        .filter(DashboardItem.dashboard_id == dashboard_id)
        .order_by(order_by, DashboardItem.id.asc())
        .all()
    )
    ids = [r[0] for r in rows]
    total = len(ids)
    if total == 0:
        return 1, 1
    try:
        idx = ids.index(item_id) + 1
    except ValueError:
        idx = 1
    return idx, total


def _dashboard_counts(db: Session, dashboard_id: int) -> tuple[int, int]:
    total_items = (
        db.query(DashboardItem)
        .filter(DashboardItem.dashboard_id == dashboard_id)
        .count()
    )
    unique_types = (
        db.query(DashboardItem.artifact_type)
        .filter(DashboardItem.dashboard_id == dashboard_id)
        .distinct()
        .count()
    )
    return total_items, unique_types


def _get_or_create_default_dashboard(db: Session, user_id: int) -> Dashboard:
    dashboard = (
        db.query(Dashboard)
        .filter(Dashboard.user_id == user_id, Dashboard.is_default.is_(True))
        .first()
    )
    if dashboard:
        return dashboard

    dashboard = Dashboard(
        user_id=user_id,
        name="My Dashboard",
        description="Default dashboard",
        is_default=True,
    )
    db.add(dashboard)
    db.commit()
    db.refresh(dashboard)
    return dashboard


def _ensure_owns_dashboard(db: Session, user_id: int, dashboard_id: int) -> Dashboard:
    dashboard = (
        db.query(Dashboard)
        .filter(Dashboard.id == dashboard_id, Dashboard.user_id == user_id)
        .first()
    )
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return dashboard


def _ensure_owns_item(db: Session, user_id: int, item_id: int) -> DashboardItem:
    item = db.query(DashboardItem).filter(DashboardItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    dashboard = (
        db.query(Dashboard)
        .filter(Dashboard.id == item.dashboard_id, Dashboard.user_id == user_id)
        .first()
    )
    if not dashboard:
        raise HTTPException(status_code=403, detail="Not authorized")
    return item


def _item_with_conversation(db: Session, item_id: int):
    """
    Helper to return (DashboardItem, conversation_title)
    """
    return (
        db.query(
            DashboardItem,
            Conversation.title.label("conversation_title"),
        )
        .outerjoin(Conversation, Conversation.id == DashboardItem.conversation_id)
        .filter(DashboardItem.id == item_id)
        .first()
    )


def _deep_merge(dst: dict, src: dict) -> dict:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst

# ---------- Routes: Dashboards ----------

@router.get("/", response_model=List[DashboardOut])
@router.get("", response_model=List[DashboardOut])
def list_dashboards(
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    return (
        db.query(Dashboard)
        .filter(Dashboard.user_id == user.user_id)
        .order_by(Dashboard.is_default.desc(), Dashboard.created_at.asc())
        .all()
    )

# ---------- Routes: Items ----------

@router.get("/{dashboard_id}/items", response_model=List[DashboardItemOut])
def list_items(
    dashboard_id: int,
    order: Optional[str] = "position",
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    _ensure_owns_dashboard(db, user.user_id, dashboard_id)

    if order == "newest":
        order_by = DashboardItem.created_at.desc()
    elif order == "oldest":
        order_by = DashboardItem.created_at.asc()
    elif order == "title":
        order_by = DashboardItem.title.asc()
    else:
        order = "position"
        order_by = DashboardItem.position.asc()

    rows = (
        db.query(
            DashboardItem,
            Conversation.title.label("conversation_title"),
        )
        .outerjoin(Conversation, Conversation.id == DashboardItem.conversation_id)
        .filter(DashboardItem.dashboard_id == dashboard_id)
        .order_by(order_by, DashboardItem.id.asc())
        .all()
    )

    manual = (order == "position")
    total = len(rows)

    out: List[DashboardItemOut] = []
    for i, (item, convo_title) in enumerate(rows, start=1):
        out.append(
            DashboardItemOut(
                id=item.id,
                dashboard_id=item.dashboard_id,
                position=item.position,
                position_min=(i if manual else None),
                position_max=(total if manual else None),
                can_reorder=manual,
                created_at=item.created_at,
                artifact_type=item.artifact_type,
                title=item.title,
                source_query=item.source_query,
                config=item.config,
                conversation_message_id=item.conversation_message_id,
                conversation_id=item.conversation_id,
                conversation_title=convo_title,
            )
        )
    return out


@router.patch("/items/{item_id}", response_model=DashboardItemOut)
def update_item(
    item_id: int,
    payload: DashboardItemUpdate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    item = _ensure_owns_item(db, user.user_id, item_id)

    # Swap positions explicitly (visual-order aware)
    if payload.swap_with_id is not None:
        other = _ensure_owns_item(db, user.user_id, payload.swap_with_id)

        if other.dashboard_id != item.dashboard_id:
            raise HTTPException(status_code=400, detail="Invalid swap target")

        item.position, other.position = other.position, item.position
    # Move (swap positions with neighbor in same dashboard) - one click = one widget
    elif payload.move in ("up", "down"):
        if payload.order and payload.order != "position":
            raise HTTPException(
                status_code=400,
                detail="Move is only available when order=position",
            )

        items = (
            db.query(DashboardItem)
            .filter(DashboardItem.dashboard_id == item.dashboard_id)
            .order_by(DashboardItem.position.asc(), DashboardItem.id.asc())
            .all()
        )
        idx = next((i for i, it in enumerate(items) if it.id == item.id), None)
        if idx is None:
            raise HTTPException(status_code=404, detail="Item not found")

        if payload.move == "up":
            if idx == 0:
                raise HTTPException(status_code=400, detail="Cannot move item further")
            neighbor = items[idx - 1]
        else:
            if idx == len(items) - 1:
                raise HTTPException(status_code=400, detail="Cannot move item further")
            neighbor = items[idx + 1]

        item.position, neighbor.position = neighbor.position, item.position

    # Title (NOTE: schema currently requires title always; consider Optional below)
    if payload.title is not None:
        item.title = payload.title

    # Config patch (deep merge)
    if payload.config_patch:
        # IMPORTANT: make a NEW dict so SQLAlchemy detects change on JSON columns
        cfg = dict(item.config or {})

        # Special-case appearance: replace, don't merge
        ui_patch = payload.config_patch.get("ui")
        if ui_patch and "appearance" in ui_patch:
            # also ensure nested dict is a NEW object
            cfg_ui = dict(cfg.get("ui") or {})
            cfg_ui["appearance"] = ui_patch["appearance"]
            cfg["ui"] = cfg_ui

            payload.config_patch = {
                **payload.config_patch,
                "ui": {k: v for k, v in ui_patch.items() if k != "appearance"},
            }

        _deep_merge(cfg, payload.config_patch)

        if cfg.get("layout") is None:
            cfg.pop("layout", None)

        # assign the new dict
        item.config = cfg

    db.commit()

    row = _item_with_conversation(db, item.id)
    if not row:
        raise HTTPException(status_code=404, detail="Item not found")
    it, convo_title = row

    # IMPORTANT: use the same default ordering as list_items when order is "position"
    idx, total = _item_position_index(
        db,
        it.dashboard_id,
        it.id,
        DashboardItem.position.asc(),
    )
    return DashboardItemOut(
        id=it.id,
        dashboard_id=it.dashboard_id,
        position=it.position,
        position_min=idx,
        position_max=total,
        can_reorder=True,
        created_at=it.created_at,
        artifact_type=it.artifact_type,
        title=it.title,
        source_query=it.source_query,
        config=it.config,
        conversation_message_id=it.conversation_message_id,
        conversation_id=it.conversation_id,
        conversation_title=convo_title,
    )


@router.post(
    "/{dashboard_id}/items",
    response_model=DashboardItemOut,
    status_code=status.HTTP_201_CREATED,
)
def add_item(
    dashboard_id: int,
    payload: DashboardItemBase,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    _ensure_owns_dashboard(db, user.user_id, dashboard_id)

    max_pos = (
        db.query(DashboardItem.position)
        .filter(DashboardItem.dashboard_id == dashboard_id)
        .order_by(DashboardItem.position.desc())
        .first()
    )
    next_pos = (max_pos[0] if max_pos else 0) + 1

    item = DashboardItem(
        dashboard_id=dashboard_id,
        artifact_type=payload.artifact_type,
        title=payload.title,
        source_query=payload.source_query,
        config=payload.config,
        position=next_pos,
        conversation_message_id=payload.conversation_message_id,
        conversation_id=payload.conversation_id,
    )
    db.add(item)
    db.commit()
    db.refresh(item)

    total_items, unique_types = _dashboard_counts(db, dashboard_id)

    MetricsService.record_dashboard_metrics(
        db=db,
        user_id=user.user_id,
        dashboard_id=dashboard_id,
        action_type="item_added",
        artifact_type=payload.artifact_type,
        total_items=total_items,
        unique_artifact_types=unique_types,
    )

    row = _item_with_conversation(db, item.id)
    it, convo_title = row
    # IMPORTANT: use the same default ordering as list_items when order is "position"
    idx, total = _item_position_index(
        db,
        it.dashboard_id,
        it.id,
        DashboardItem.position.asc(),
    )

    return DashboardItemOut(
        id=it.id,
        dashboard_id=it.dashboard_id,
        position=it.position,
        position_min=idx,
        position_max=total,
        can_reorder=True,
        created_at=it.created_at,
        artifact_type=it.artifact_type,
        title=it.title,
        source_query=it.source_query,
        config=it.config,
        conversation_message_id=it.conversation_message_id,
        conversation_id=it.conversation_id,
        conversation_title=convo_title,
    )


@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_item(
    item_id: int,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    item = _ensure_owns_item(db, user.user_id, item_id)

    dashboard_id = item.dashboard_id
    removed_type = item.artifact_type

    db.delete(item)
    db.commit()

    total_items, unique_types = _dashboard_counts(db, dashboard_id)

    MetricsService.record_dashboard_metrics(
        db=db,
        user_id=user.user_id,
        dashboard_id=dashboard_id,
        action_type="item_removed",
        artifact_type=removed_type,
        total_items=total_items,
        unique_artifact_types=unique_types,
    )
    return

# ---------- Special: pin from chat ----------

@router.post("/pin", response_model=DashboardItemOut, status_code=status.HTTP_201_CREATED)
def pin_artifact(
    payload: PinRequest,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    if payload.dashboard_id is not None:
        dashboard = _ensure_owns_dashboard(db, user.user_id, payload.dashboard_id)
    else:
        dashboard = _get_or_create_default_dashboard(db, user.user_id)

    max_pos = (
        db.query(DashboardItem.position)
        .filter(DashboardItem.dashboard_id == dashboard.id)
        .order_by(DashboardItem.position.desc())
        .first()
    )
    next_pos = (max_pos[0] if max_pos else 0) + 1

    item = DashboardItem(
        dashboard_id=dashboard.id,
        artifact_type=payload.artifact_type,
        created_at=datetime.now(),
        title=payload.title,
        source_query=payload.source_query,
        config=payload.config,
        position=next_pos,
        conversation_message_id=payload.conversation_message_id,
        conversation_id=payload.conversation_id
    )
    db.add(item)
    db.commit()
    db.refresh(item)

    total_items, unique_types = _dashboard_counts(db, dashboard.id)

    MetricsService.record_dashboard_metrics(
        db=db,
        user_id=user.user_id,
        dashboard_id=dashboard.id,
        action_type="item_added",
        artifact_type=payload.artifact_type,
        total_items=total_items,
        unique_artifact_types=unique_types,
    )

    row = _item_with_conversation(db, item.id)
    it, convo_title = row
    # IMPORTANT: use the same default ordering as list_items when order is "position"
    idx, total = _item_position_index(
        db,
        it.dashboard_id,
        it.id,
        DashboardItem.position.asc(),
    )

    return DashboardItemOut(
        id=it.id,
        dashboard_id=it.dashboard_id,
        position=it.position,
        position_min=idx,
        position_max=total,
        created_at=it.created_at,
        can_reorder=True,
        artifact_type=it.artifact_type,
        title=it.title,
        source_query=it.source_query,
        config=it.config,
        conversation_message_id=it.conversation_message_id,
        conversation_id=it.conversation_id,
        conversation_title=convo_title,
    )
