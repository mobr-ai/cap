"""
Metrics reporting API.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, cast, Float, Integer
from datetime import datetime, timedelta, timezone
from typing import Optional

from cap.database.session import get_db
from cap.database.model import QueryMetrics, KGMetrics, DashboardMetrics
from cap.services.lang_detect_client import LanguageDetector

router = APIRouter(prefix="/api/v1/metrics", tags=["metrics"])


@router.get("/report")
def get_aggregated_metrics(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """Get aggregated metrics for all success dimensions."""

    # Date filtering
    filters = []
    if start_date:
        filters.append(QueryMetrics.created_at >= datetime.fromisoformat(start_date))
    if end_date:
        filters.append(QueryMetrics.created_at <= datetime.fromisoformat(end_date))

    # Dimension 1: LLM Capability
    llm_stats = db.query(
        func.count(QueryMetrics.id).label('total'),
        func.sum(cast(cast(QueryMetrics.sparql_valid, Integer), Float)).label('valid'),
        func.sum(cast(cast(QueryMetrics.semantic_valid, Integer), Float)).label('semantic_valid'),
        func.sum(cast(cast(QueryMetrics.is_federated, Integer), Float)).label('federated'),
        func.count(func.distinct(QueryMetrics.detected_language)).label('unique_languages')
    ).filter(and_(*filters) if filters else True).first()

    llm_capability =  {
        "total_queries": llm_stats.total or 0,
        "valid_sparql_rate": (llm_stats.valid / llm_stats.total * 100) if llm_stats.total else 0,
        "semantic_valid_rate": (llm_stats.semantic_valid / llm_stats.total * 100) if llm_stats.total else 0,
        "federated_query_rate": (llm_stats.federated / llm_stats.total * 100) if llm_stats.total else 0,
        "unique_languages": llm_stats.unique_languages or 0,
        "target_valid_rate": 90.0,
        "target_semantic_rate": 85.0
    }

    # Language breakdown
    lang_breakdown = db.query(
        QueryMetrics.detected_language,
        func.count(QueryMetrics.id).label('count')
    ).filter(and_(*filters) if filters else True).group_by(
        QueryMetrics.detected_language
    ).all()

    llm_capability["languages"] = [
        {
            "code": lang,
            "name": LanguageDetector.get_language_name(lang),
            "query_count": count
        }
        for lang, count in lang_breakdown
    ]

    # Dimension 2: Knowledge Graph
    kg_stats = db.query(
        func.count(KGMetrics.id).label('total_loads'),
        func.sum(KGMetrics.triples_loaded).label('total_triples'),
        func.sum(cast(cast(KGMetrics.ontology_aligned, Integer), Float)).label('aligned'),
        func.sum(cast(cast(KGMetrics.has_offchain_metadata, Integer), Float)).label('with_metadata')
    ).first()

    query_kg_stats = db.query(
        func.sum(cast(cast(QueryMetrics.has_offchain_metadata, Integer), Float)).label('queries_with_metadata')
    ).filter(and_(*filters) if filters else True).first()

    knowledge_graph = {
        "total_triples_loaded": kg_stats.total_triples or 0,
        "ontology_alignment_rate": (kg_stats.aligned / kg_stats.total_loads * 100) if kg_stats.total_loads else 0,
        "loads_with_metadata_rate": (kg_stats.with_metadata / kg_stats.total_loads * 100) if kg_stats.total_loads else 0,
        "queries_using_metadata": query_kg_stats.queries_with_metadata or 0,
        "target_metadata_resolution": 95.0
    }

    # Dimension 3: Dashboard Adoption
    dashboard_stats = db.query(
        func.count(func.distinct(DashboardMetrics.dashboard_id)).label('unique_dashboards'),
        func.count(func.distinct(DashboardMetrics.user_id)).label('unique_users')
    ).first()

    avg_widgets = db.query(
        func.avg(DashboardMetrics.unique_artifact_types).label('avg_types')
    ).filter(DashboardMetrics.action_type == 'item_added').first()

    dashboard_adoption = {
        "unique_dashboards": dashboard_stats.unique_dashboards or 0,
        "unique_users": dashboard_stats.unique_users or 0,
        "avg_widget_types_per_dashboard": float(avg_widgets.avg_types or 0),
        "target_dashboards": 200,
        "target_avg_widgets": 4
    }

    # Dimension 4: Performance
    perf_stats = db.query(
        func.avg(QueryMetrics.llm_latency_ms).label('avg_llm'),
        func.percentile_cont(0.95).within_group(QueryMetrics.llm_latency_ms).label('p95_llm'),
        func.avg(QueryMetrics.sparql_latency_ms).label('avg_sparql'),
        func.percentile_cont(0.95).within_group(QueryMetrics.sparql_latency_ms).label('p95_sparql'),
        func.avg(QueryMetrics.total_latency_ms).label('avg_total')
    ).filter(and_(*filters) if filters else True).first()

    performance = {
        "avg_llm_latency_ms": float(perf_stats.avg_llm or 0),
        "p95_llm_latency_ms": float(perf_stats.p95_llm or 0),
        "avg_sparql_latency_ms": float(perf_stats.avg_sparql or 0),
        "p95_sparql_latency_ms": float(perf_stats.p95_sparql or 0),
        "avg_total_latency_ms": float(perf_stats.avg_total or 0),
        "target_latency_ms": 20000
    }

    # Dimension 5: Ecosystem Engagement (daily languages)
    today = datetime.now(timezone.utc).date()
    last_7_days = today - timedelta(days=7)

    daily_langs = db.query(
        func.date(QueryMetrics.created_at).label('date'),
        func.count(func.distinct(QueryMetrics.detected_language)).label('lang_count')
    ).filter(
        QueryMetrics.created_at >= last_7_days
    ).group_by(
        func.date(QueryMetrics.created_at)
    ).all()

    ecosystem_engagement = {
        "daily_language_diversity": [
            {"date": str(date), "unique_languages": count}
            for date, count in daily_langs
        ],
        "target_daily_languages": 10
    }

    # Dimension 6: Query Complexity
    complexity_stats = db.query(
        func.avg(QueryMetrics.complexity_score).label('avg_complexity'),
        func.sum(cast(cast(QueryMetrics.has_multi_relationship, Integer), Float)).label('multi_rel')
    ).filter(and_(*filters) if filters else True).first()

    complexity_dist = db.query(
        QueryMetrics.complexity_score,
        func.count(QueryMetrics.id).label('count')
    ).filter(and_(*filters) if filters else True).group_by(
        QueryMetrics.complexity_score
    ).all()

    query_complexity = {
        "avg_complexity_score": float(complexity_stats.avg_complexity or 0),
        "multi_relationship_rate": (complexity_stats.multi_rel / llm_stats.total * 100) if llm_stats.total else 0,
        "complexity_distribution": [
            {"score": score, "count": count}
            for score, count in complexity_dist
        ],
        "target_multi_relationship_rate": 40.0
    }

    return {
        "period": {
            "start_date": start_date or "all_time",
            "end_date": end_date or "now"
        },
        "llm_capability": llm_capability,
        "knowledge_graph": knowledge_graph,
        "dashboard_adoption": dashboard_adoption,
        "performance": performance,
        "ecosystem_engagement": ecosystem_engagement,
        "query_complexity": query_complexity
    }


@router.get("/queries")
def get_recent_queries(
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """Get recent query metrics."""
    queries = db.query(QueryMetrics).order_by(
        QueryMetrics.created_at.desc()
    ).limit(limit).all()

    return {
        "queries": [
            {
                "id": q.id,
                "nl_query": q.nl_query,
                "language": q.detected_language,
                "succeeded": q.query_succeeded,
                "complexity_score": q.complexity_score,
                "total_latency_ms": q.total_latency_ms,
                "created_at": q.created_at.isoformat()
            }
            for q in queries
        ]
    }