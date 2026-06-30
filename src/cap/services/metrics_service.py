"""
Centralized metrics collection service.
"""
import logging
import re
from typing import Any

from opentelemetry import trace
from sqlalchemy.orm import Session

from cap.chains.cardano.canon.pattern_registry import PatternRegistry
from cap.database.model import DashboardMetrics, QueryMetrics
from cap.services.admin_alerts_service import (
    maybe_notify_admins_beta_query_created,
    maybe_notify_admins_query_created,
)
from cap.services.lang_detect_client import LanguageDetector

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class MetricsService:
    """Centralized service for collecting and storing metrics."""

    @staticmethod
    def calculate_complexity(
        sparql_query: str = "",
        sql_query: str = "",
        kv_results: dict | None = None,
    ) -> dict[str, Any]:
        """Calculate query complexity indicators for SPARQL and SQL."""
        combined_query = f"{sparql_query or ''}\n{sql_query or ''}"

        temporal_terms = (
            PatternRegistry.YEARLY_TERMS
            + PatternRegistry.MONTHLY_TERMS
            + PatternRegistry.WEEKLY_TERMS
            + PatternRegistry.DAILY_TERMS
            + PatternRegistry.EPOCH_PERIOD_TERMS
            + PatternRegistry.TIME_PERIOD_UNITS
        )

        temporal_pat = "|".join(temporal_terms)
        metadata_pat = "|".join(PatternRegistry.DEFAULT_METADATA_PROPERTIES)

        sql_join_count = len(re.findall(r"\bJOIN\b", sql_query or "", re.IGNORECASE))
        sparql_join_count = len(re.findall(r"\?[\w]+\s+[\w:]+\s+\?[\w]+", sparql_query or ""))

        indicators = {
            "multi_join": (sparql_join_count > 1) or (sql_join_count > 1),
            "aggregation": bool(
                re.search(
                    r"\b(COUNT|SUM|AVG|MIN|MAX|GROUP BY)\b",
                    combined_query,
                    re.IGNORECASE,
                )
            ),
            "subquery": bool(
                re.search(r"\(\s*SELECT\b", sql_query or "", re.IGNORECASE)
                or (
                    "SELECT" in (sparql_query or "")[(sparql_query or "").find("WHERE"):]
                    if "WHERE" in (sparql_query or "")
                    else False
                )
            ),
            "optional": "OPTIONAL" in (sparql_query or "").upper(),
            "filter": bool(
                "FILTER" in (sparql_query or "").upper()
                or re.search(r"\bWHERE\b", sql_query or "", re.IGNORECASE)
            ),
            "union": bool(
                "UNION" in (sparql_query or "").upper()
                or re.search(r"\bUNION\b", sql_query or "", re.IGNORECASE)
            ),
            "temporal": bool(re.search(rf"\b({temporal_pat})\b", combined_query, re.IGNORECASE)),
            "offchain_metadata": bool(
                sql_query
                or re.search(rf"\b({metadata_pat})\b", combined_query, re.IGNORECASE)
            ),
        }

        complexity_score = sum(indicators.values())

        return {
            "complexity_score": complexity_score,
            "has_multi_relationship": indicators["multi_join"],
            "has_aggregation": indicators["aggregation"],
            "has_temporal": indicators["temporal"],
            "has_offchain_metadata": indicators["offchain_metadata"],
        }

    @staticmethod
    def record_query_metrics(
        db: Session,
        nl_query: str,
        normalized_query: str,
        sparql_query: str = "",
        sql_query: str = "",
        query_source: str | None = None,
        kv_results: dict[str, Any] | None = None,
        is_sequential: bool = False,
        sparql_valid: bool = False,
        sql_valid: bool = False,
        query_succeeded: bool = False,
        llm_latency_ms: int = 0,
        sparql_latency_ms: int = 0,
        sql_latency_ms: int = 0,
        total_latency_ms: int = 0,
        user_id: int | None = None,
        telegram_account_id: int | None = None,
        telegram_user_id: int | None = None,
        telegram_chat_id: int | None = None,
        request_source: str = "cap_web",
        error_message: str | None = None,
    ) -> QueryMetrics | None:
        """Record query execution metrics for CAP web, linked Telegram, and Telegram guests."""

        if db is None:
            return None

        sparql_query = sparql_query or ""
        sql_query = sql_query or ""
        has_sparql = bool(sparql_query.strip())
        has_sql = bool(sql_query.strip())

        detected_lang = LanguageDetector.detect_language(nl_query)

        complexity = MetricsService.calculate_complexity(
            sparql_query=sparql_query,
            sql_query=sql_query,
            kv_results=kv_results,
        )

        result_count = 0
        result_type = None
        if kv_results:
            result_type = kv_results.get("result_type")
            if result_type in {"multiple", "table"}:
                result_count = kv_results.get("count", 0)
                if not result_count and isinstance(kv_results.get("data"), list):
                    result_count = len(kv_results["data"])
            elif result_type == "single":
                result_count = 1

        is_federated = bool(
            query_source == "federated"
            or (has_sparql and has_sql)
            or is_sequential
        )

        semantic_valid = bool(
            (sparql_valid or sql_valid)
            and (result_count > 0 or result_type == "boolean")
        )

        metric = QueryMetrics(
            user_id=user_id,
            telegram_account_id=telegram_account_id,
            telegram_user_id=telegram_user_id,
            telegram_chat_id=telegram_chat_id,
            request_source=request_source,
            nl_query=nl_query,
            normalized_query=normalized_query,
            detected_language=detected_lang,
            sparql_query=sparql_query,
            sql_query=sql_query,
            query_source=query_source,
            has_sparql=has_sparql,
            has_sql=has_sql,
            is_sequential=is_sequential,
            is_federated=is_federated,
            result_count=result_count,
            result_type=result_type,
            kv_results=kv_results,
            sparql_valid=sparql_valid,
            sql_valid=sql_valid,
            semantic_valid=semantic_valid,
            query_succeeded=query_succeeded,
            error_message=error_message,
            llm_latency_ms=llm_latency_ms,
            sparql_latency_ms=sparql_latency_ms,
            sql_latency_ms=sql_latency_ms,
            total_latency_ms=total_latency_ms,
            **complexity,
        )

        db.add(metric)
        db.commit()

        logger.info(
            "Recorded query metrics: source=%s query_source=%s user_id=%s telegram_user_id=%s has_sparql=%s has_sql=%s latency=%sms",
            request_source,
            query_source,
            user_id,
            telegram_user_id,
            has_sparql,
            has_sql,
            total_latency_ms,
        )

        try:
            maybe_notify_admins_query_created(db, metric)
        except Exception:
            logger.exception("Failed to queue all-user query admin notification")

        try:
            maybe_notify_admins_beta_query_created(db, metric)
        except Exception:
            logger.exception("Failed to queue beta-user query admin notification")

        return metric


    @staticmethod
    def record_dashboard_metrics(
        db: Session,
        user_id: int,
        dashboard_id: int,
        action_type: str,
        artifact_type: str | None = None,
        total_items: int = 0,
        unique_artifact_types: int = 0
    ) -> DashboardMetrics:
        """Record dashboard interaction metrics."""

        metric = DashboardMetrics(
            user_id=user_id,
            dashboard_id=dashboard_id,
            action_type=action_type,
            artifact_type=artifact_type,
            total_items=total_items,
            unique_artifact_types=unique_artifact_types
        )

        db.add(metric)
        db.commit()

        return metric
