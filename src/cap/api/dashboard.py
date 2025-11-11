# cap/api/dashboard.py

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import Dashboard, DashboardItem
from cap.api.auth_dependencies import get_current_user

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


class DashboardItemOut(DashboardItemBase):
    id: int
    dashboard_id: int
    position: int

    model_config = {"from_attributes": True}


class PinRequest(DashboardItemBase):
    dashboard_id: Optional[int] = None  # if null → use/create default


# ---------- Helpers ----------

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


# ---------- Routes: Dashboards ----------

@router.get("/", response_model=List[DashboardOut])
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


@router.post("/", response_model=DashboardOut, status_code=status.HTTP_201_CREATED)
def create_dashboard(
    payload: DashboardCreate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    if payload.is_default:
        # clear existing default
        db.query(Dashboard).filter(
            Dashboard.user_id == user.user_id,
            Dashboard.is_default.is_(True),
        ).update({"is_default": False})

    d = Dashboard(
        user_id=user.user_id,
        name=payload.name,
        description=payload.description,
        is_default=payload.is_default,
    )
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


@router.get("/{dashboard_id}", response_model=DashboardOut)
def get_dashboard(
    dashboard_id: int,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    d = _ensure_owns_dashboard(db, user.user_id, dashboard_id)
    return d


@router.delete("/{dashboard_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_dashboard(
    dashboard_id: int,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    d = _ensure_owns_dashboard(db, user.user_id, dashboard_id)
    # items cascade via FK ondelete=CASCADE if configured; otherwise delete manually
    db.query(DashboardItem).filter(DashboardItem.dashboard_id == d.id).delete()
    db.delete(d)
    db.commit()
    return


# ---------- Routes: Items ----------

@router.get("/{dashboard_id}/items", response_model=List[DashboardItemOut])
def list_items(
    dashboard_id: int,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    _ensure_owns_dashboard(db, user.user_id, dashboard_id)
    items = (
        db.query(DashboardItem)
        .filter(DashboardItem.dashboard_id == dashboard_id)
        .order_by(DashboardItem.position.asc(), DashboardItem.id.asc())
        .all()
    )
    return items


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
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_item(
    item_id: int,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    item = _ensure_owns_item(db, user.user_id, item_id)
    db.delete(item)
    db.commit()
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
        title=payload.title,
        source_query=payload.source_query,
        config=payload.config,
        position=next_pos,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item
