"""
Metrics reporting API.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Float, Integer, select, case
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

from cap.database.session import get_db
from cap.database.model import (
    Dashboard,
    DashboardItem,
    QueryMetrics,
    KGMetrics,
    DashboardMetrics,
    User,
    ConversationMessage,
)
from cap.services.lang_detect_client import LanguageDetector
from cap.core.auth_dependencies import get_current_admin_user


router = APIRouter(prefix="/api/v1/metrics", tags=["metrics"])


@router.get("/report")
def get_aggregated_metrics(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """Get aggregated metrics for all success dimensions."""

    # Date filtering
    filters = []
    if start_date:
        filters.append(QueryMetrics.created_at >= datetime.fromisoformat(start_date))
    if end_date:
        filters.append(QueryMetrics.created_at <= datetime.fromisoformat(end_date))

    # Dimension 1: LLM Capability
    llm_stats = (
        db.query(
            func.count(QueryMetrics.id).label("total"),
            func.sum(cast(cast(QueryMetrics.sparql_valid, Integer), Float)).label(
                "valid"
            ),
            func.sum(cast(cast(QueryMetrics.semantic_valid, Integer), Float)).label(
                "semantic_valid"
            ),
            func.sum(cast(cast(QueryMetrics.is_federated, Integer), Float)).label(
                "federated"
            ),
            func.count(func.distinct(QueryMetrics.detected_language)).label(
                "unique_languages"
            ),
        )
        .filter(and_(*filters) if filters else True)
        .first()
    )

    llm_capability = {
        "total_queries": llm_stats.total or 0,
        "valid_sparql_rate": (llm_stats.valid / llm_stats.total * 100)
        if llm_stats.total
        else 0,
        "semantic_valid_rate": (llm_stats.semantic_valid / llm_stats.total * 100)
        if llm_stats.total
        else 0,
        "federated_query_rate": (llm_stats.federated / llm_stats.total * 100)
        if llm_stats.total
        else 0,
        "unique_languages": llm_stats.unique_languages or 0,
        "target_valid_rate": 90.0,
        "target_semantic_rate": 85.0,
    }

    # Language breakdown
    lang_breakdown = (
        db.query(QueryMetrics.detected_language, func.count(QueryMetrics.id).label("count"))
        .filter(and_(*filters) if filters else True)
        .group_by(QueryMetrics.detected_language)
        .all()
    )

    llm_capability["languages"] = [
        {
            "code": lang,
            "name": LanguageDetector.get_language_name(lang),
            "query_count": count,
        }
        for lang, count in lang_breakdown
    ]

    # Dimension 2: Knowledge Graph
    kg_stats = (
        db.query(
            func.count(KGMetrics.id).label("total_loads"),
            func.sum(KGMetrics.triples_loaded).label("total_triples"),
            func.sum(cast(cast(KGMetrics.ontology_aligned, Integer), Float)).label(
                "aligned"
            ),
            func.sum(cast(cast(KGMetrics.has_offchain_metadata, Integer), Float)).label(
                "with_metadata"
            ),
        )
        .first()
    )

    query_kg_stats = (
        db.query(
            func.sum(
                cast(cast(QueryMetrics.has_offchain_metadata, Integer), Float)
            ).label("queries_with_metadata")
        )
        .filter(and_(*filters) if filters else True)
        .first()
    )

    knowledge_graph = {
        "total_triples_loaded": kg_stats.total_triples or 0,
        "ontology_alignment_rate": (kg_stats.aligned / kg_stats.total_loads * 100)
        if kg_stats.total_loads
        else 0,
        "loads_with_metadata_rate": (kg_stats.with_metadata / kg_stats.total_loads * 100)
        if kg_stats.total_loads
        else 0,
        "queries_using_metadata": query_kg_stats.queries_with_metadata or 0,
        "target_metadata_resolution": 95.0,
    }

    # Dimension 3: Dashboard Adoption (authoritative counts)
    dash_counts = (
        db.query(
            func.count(Dashboard.id).label("total_dashboards"),
            func.count(func.distinct(Dashboard.user_id)).label("unique_users"),
        )
        .first()
    )

    avg_items = db.query(
        func.avg(
            db.query(func.count(DashboardItem.id))
            .filter(DashboardItem.dashboard_id == Dashboard.id)
            .correlate(Dashboard)
            .scalar_subquery()
        )
    ).scalar()

    dashboard_adoption = {
        "unique_dashboards": int(dash_counts.total_dashboards or 0),
        "unique_users": int(dash_counts.unique_users or 0),
        "avg_items_per_dashboard": float(avg_items or 0),
        "target_dashboards": 200,
        "target_avg_widgets": 4,
    }

    # Dimension 4: Performance
    perf_stats = (
        db.query(
            func.avg(QueryMetrics.llm_latency_ms).label("avg_llm"),
            func.percentile_cont(0.95)
            .within_group(QueryMetrics.llm_latency_ms)
            .label("p95_llm"),
            func.avg(QueryMetrics.sparql_latency_ms).label("avg_sparql"),
            func.percentile_cont(0.95)
            .within_group(QueryMetrics.sparql_latency_ms)
            .label("p95_sparql"),
            func.avg(QueryMetrics.total_latency_ms).label("avg_total"),
        )
        .filter(and_(*filters) if filters else True)
        .first()
    )

    performance = {
        "avg_llm_latency_ms": float(perf_stats.avg_llm or 0),
        "p95_llm_latency_ms": float(perf_stats.p95_llm or 0),
        "avg_sparql_latency_ms": float(perf_stats.avg_sparql or 0),
        "p95_sparql_latency_ms": float(perf_stats.p95_sparql or 0),
        "avg_total_latency_ms": float(perf_stats.avg_total or 0),
        "target_latency_ms": 20000,
    }

    # Dimension 5: Ecosystem Engagement (daily languages)
    today = datetime.now(timezone.utc).date()
    last_7_days = today - timedelta(days=7)

    daily_langs = (
        db.query(
            func.date(QueryMetrics.created_at).label("date"),
            func.count(func.distinct(QueryMetrics.detected_language)).label("lang_count"),
        )
        .filter(QueryMetrics.created_at >= last_7_days)
        .group_by(func.date(QueryMetrics.created_at))
        .all()
    )

    ecosystem_engagement = {
        "daily_language_diversity": [
            {"date": str(date), "unique_languages": count} for date, count in daily_langs
        ],
        "target_daily_languages": 10,
    }

    # Dimension 6: Query Complexity
    complexity_stats = (
        db.query(
            func.avg(QueryMetrics.complexity_score).label("avg_complexity"),
            func.sum(
                cast(cast(QueryMetrics.has_multi_relationship, Integer), Float)
            ).label("multi_rel"),
        )
        .filter(and_(*filters) if filters else True)
        .first()
    )

    complexity_dist = (
        db.query(QueryMetrics.complexity_score, func.count(QueryMetrics.id).label("count"))
        .filter(and_(*filters) if filters else True)
        .group_by(QueryMetrics.complexity_score)
        .all()
    )

    query_complexity = {
        "avg_complexity_score": float(complexity_stats.avg_complexity or 0),
        "multi_relationship_rate": (complexity_stats.multi_rel / llm_stats.total * 100)
        if llm_stats.total
        else 0,
        "complexity_distribution": [{"score": score, "count": count} for score, count in complexity_dist],
        "target_multi_relationship_rate": 40.0,
    }

    return {
        "period": {"start_date": start_date or "all_time", "end_date": end_date or "now"},
        "llm_capability": llm_capability,
        "knowledge_graph": knowledge_graph,
        "dashboard_adoption": dashboard_adoption,
        "performance": performance,
        "ecosystem_engagement": ecosystem_engagement,
        "query_complexity": query_complexity,
    }


def _query_metrics_to_dict(r: QueryMetrics) -> dict:
    return {
        # cards / lists
        "id": r.id,
        "user_id": r.user_id,
        "nl_query": r.nl_query,
        "normalized_query": getattr(r, "normalized_query", None),
        "language": r.detected_language,
        "succeeded": r.query_succeeded,
        "complexity_score": r.complexity_score,
        "total_latency_ms": r.total_latency_ms,
        "created_at": r.created_at.isoformat() if r.created_at else None,

        # details / modal
        "sparql_query": getattr(r, "sparql_query", None),
        "sparql_valid": getattr(r, "sparql_valid", None),
        "semantic_valid": getattr(r, "semantic_valid", None),
        "is_sequential": getattr(r, "is_sequential", None),
        "is_federated": getattr(r, "is_federated", None),
        "has_temporal": getattr(r, "has_temporal", None),
        "has_offchain_metadata": getattr(r, "has_offchain_metadata", None),
        "llm_latency_ms": getattr(r, "llm_latency_ms", None),
        "sparql_latency_ms": getattr(r, "sparql_latency_ms", None),
        "result_count": getattr(r, "result_count", None),
        "result_type": getattr(r, "result_type", None),
        "error_message": getattr(r, "error_message", None),
    }


def _locator_map_for_query_ids(
    db: Session, query_ids: List[int]
) -> Dict[int, Dict[str, Optional[int]]]:
    """
    Returns: { query_id: {"conversation_id": int|None, "conversation_message_id": int|None} }

    Primary strategy:
      - Lookup ConversationMessage rows where nl_query_id matches QueryMetrics.id
      - Prefer role == "user", otherwise newest message
      - Uses Postgres DISTINCT ON via SQLAlchemy .distinct(col)

    Fallback strategy (for older data where nl_query_id is NULL historically):
      - Join QueryMetrics -> ConversationMessage by:
          same user_id, role == "user", content == nl_query (case-insensitive),
          and created_at within a window around QueryMetrics.created_at
      - Pick best candidate per query_id using row_number() ranking:
          exact match first, then closest timestamp, then newest message id
    """
    if not query_ids:
        return {}

    # -------------------------
    # 1) Primary strategy: nl_query_id link
    # -------------------------
    role_rank = case(
        (ConversationMessage.role == "user", 0),
        else_=1,
    )

    rows = (
        db.query(
            ConversationMessage.nl_query_id.label("qid"),
            ConversationMessage.conversation_id.label("conversation_id"),
            ConversationMessage.id.label("conversation_message_id"),
        )
        .filter(ConversationMessage.nl_query_id.in_(query_ids))
        .order_by(
            ConversationMessage.nl_query_id.asc(),
            role_rank.asc(),
            ConversationMessage.created_at.desc(),
            ConversationMessage.id.desc(),
        )
        .distinct(ConversationMessage.nl_query_id)
        .all()
    )

    out: Dict[int, Dict[str, Optional[int]]] = {}
    for r in rows:
        if r.qid is None:
            continue
        out[int(r.qid)] = {
            "conversation_id": int(r.conversation_id) if r.conversation_id is not None else None,
            "conversation_message_id": int(r.conversation_message_id) if r.conversation_message_id is not None else None,
        }

    # -------------------------
    # 2) Fallback strategy: content/time match for older rows
    # -------------------------
    missing = [int(qid) for qid in query_ids if int(qid) not in out]
    if not missing:
        return out

    qm_sub = (
        select(
            QueryMetrics.id.label("qid"),
            QueryMetrics.user_id.label("user_id"),
            QueryMetrics.nl_query.label("nl_query"),
            QueryMetrics.created_at.label("q_created_at"),
        )
        .where(QueryMetrics.id.in_(missing))
        .subquery()
    )

    cm = ConversationMessage
    window = timedelta(hours=24)

    # Prefer exact match; then closest timestamp.
    rank_exact = case((cm.content == qm_sub.c.nl_query, 0), else_=1)
    rank_time = func.abs(func.extract("epoch", cm.created_at - qm_sub.c.q_created_at))

    ranked = (
        select(
            qm_sub.c.qid.label("qid"),
            cm.conversation_id.label("conversation_id"),
            cm.id.label("conversation_message_id"),
            func.row_number()
            .over(
                partition_by=qm_sub.c.qid,
                order_by=(rank_exact.asc(), rank_time.asc(), cm.id.desc()),
            )
            .label("rn"),
        )
        .select_from(qm_sub)
        .join(
            cm,
            and_(
                cm.user_id == qm_sub.c.user_id,
                cm.role == "user",
                or_(
                    cm.content == qm_sub.c.nl_query,
                    func.lower(cm.content) == func.lower(qm_sub.c.nl_query),
                ),
                cm.created_at >= (qm_sub.c.q_created_at - window),
                cm.created_at <= (qm_sub.c.q_created_at + window),
            ),
        )
        .subquery()
    )

    best = (
        db.query(
            ranked.c.qid,
            ranked.c.conversation_id,
            ranked.c.conversation_message_id,
        )
        .filter(ranked.c.rn == 1)
        .all()
    )

    for r in best:
        if r.qid is None:
            continue
        qid = int(r.qid)
        if qid in out:
            continue
        out[qid] = {
            "conversation_id": int(r.conversation_id) if r.conversation_id is not None else None,
            "conversation_message_id": int(r.conversation_message_id) if r.conversation_message_id is not None else None,
        }

    return out


@router.get("/queries")
def get_recent_queries(
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    """Admin-only: recent query metrics (used in Admin Metrics tab)."""
    queries = (
        db.query(QueryMetrics)
        .order_by(QueryMetrics.created_at.desc())
        .limit(limit)
        .all()
    )

    qids = [int(q.id) for q in queries if q and q.id is not None]
    loc = _locator_map_for_query_ids(db, qids)

    payload = []
    for q in queries:
        d = _query_metrics_to_dict(q)
        d.update(loc.get(int(q.id), {"conversation_id": None, "conversation_message_id": None}))
        payload.append(d)

    return {"queries": payload}


@router.get("/queries/{query_id}")
def get_query_details_admin(
    query_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    """Admin-only: one QueryMetrics row + conversation locator fields."""
    r = db.query(QueryMetrics).filter(QueryMetrics.id == query_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="Query not found")

    d = _query_metrics_to_dict(r)

    loc = _locator_map_for_query_ids(db, [int(query_id)])
    d.update(loc.get(int(query_id), {"conversation_id": None, "conversation_message_id": None}))

    return d


@router.get("/queries/by-user/{user_id}")
def get_user_queries_admin(
    user_id: int,
    limit: int = Query(200, ge=1, le=1000),
    q: Optional[str] = Query(None, description="Search text in nl_query (case-insensitive)"),
    start_date: Optional[str] = Query(None, description="Start date (ISO or YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (ISO or YYYY-MM-DD)"),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    """
    Admin-only: list QueryMetrics for a specific user.

    Filters:
    - q: substring match on nl_query (ILIKE)
    - start_date/end_date: inclusive datetime bounds
    """
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    filters = [QueryMetrics.user_id == user_id]

    if q:
        like = f"%{q.strip()}%"
        filters.append(QueryMetrics.nl_query.ilike(like))

    def _parse_dt(v: str) -> datetime:
        return datetime.fromisoformat(v)

    if start_date:
        filters.append(QueryMetrics.created_at >= _parse_dt(start_date))
    if end_date:
        filters.append(QueryMetrics.created_at <= _parse_dt(end_date))

    rows = (
        db.query(QueryMetrics)
        .filter(and_(*filters))
        .order_by(QueryMetrics.created_at.desc())
        .limit(limit)
        .all()
    )

    qids = [int(r.id) for r in rows if r and r.id is not None]
    loc = _locator_map_for_query_ids(db, qids)

    return {
        "user_id": user_id,
        "limit": limit,
        "q": q,
        "start_date": start_date,
        "end_date": end_date,
        "queries": [
            {**_query_metrics_to_dict(r), **loc.get(int(r.id), {"conversation_id": None, "conversation_message_id": None})}
            for r in rows
        ],
    }


@router.get("/users/{user_id}/query-summary")
def get_user_query_summary_admin(
    user_id: int,
    days: int = Query(30, ge=1, le=365, description="Time window in days for metrics aggregation"),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    """
    Admin-only: aggregated query metrics for a single user.

    Intended for admin dashboards and user-inspection pages.
    """
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    since = datetime.now(timezone.utc) - timedelta(days=days)

    stats = (
        db.query(
            func.count(QueryMetrics.id).label("total"),
            func.sum(cast(cast(QueryMetrics.query_succeeded, Integer), Float)).label("succeeded"),
            func.avg(QueryMetrics.total_latency_ms).label("avg_total_latency_ms"),
            func.avg(QueryMetrics.complexity_score).label("avg_complexity_score"),
            func.max(QueryMetrics.created_at).label("last_query_at"),
        )
        .filter(QueryMetrics.user_id == user_id, QueryMetrics.created_at >= since)
        .one()
    )

    total = int(stats.total or 0)
    succeeded = float(stats.succeeded or 0.0)

    return {
        "user_id": user_id,
        "window_days": days,
        "total_queries": total,
        "success_rate": (succeeded / total * 100.0) if total else 0.0,
        "avg_total_latency_ms": float(stats.avg_total_latency_ms or 0.0),
        "avg_complexity_score": float(stats.avg_complexity_score or 0.0),
        "last_query_at": (stats.last_query_at.isoformat() if stats.last_query_at else None),
    }
