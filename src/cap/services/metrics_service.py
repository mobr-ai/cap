"""
Centralized metrics collection service.
"""
import re
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from opentelemetry import trace
import logging

from cap.rdf.cache.pattern_registry import PatternRegistry
from cap.database.model import QueryMetrics, KGMetrics, DashboardMetrics
from cap.services.lang_detect_client import LanguageDetector

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class MetricsService:
    """Centralized service for collecting and storing metrics."""

    @staticmethod
    def calculate_complexity(sparql_query: str, kv_results: Optional[Dict] = None) -> Dict[str, Any]:
        """Calculate query complexity indicators."""
        temporal_terms = (PatternRegistry.YEARLY_TERMS +
                          PatternRegistry.MONTHLY_TERMS +
                          PatternRegistry.WEEKLY_TERMS +
                          PatternRegistry.DAILY_TERMS +
                          PatternRegistry.EPOCH_PERIOD_TERMS +
                          PatternRegistry.TIME_PERIOD_UNITS)

        temporal_pat = '|'.join(temporal_terms)
        metadata_pat = '|'.join(PatternRegistry.DEFAULT_METADATA_PROPERTIES)

        indicators = {
            'multi_join': len(re.findall(r'\?[\w]+\s+[\w:]+\s+\?[\w]+', sparql_query)) > 1,
            'aggregation': bool(re.search(r'\b(COUNT|SUM|AVG|MIN|MAX|GROUP BY)\b', sparql_query, re.IGNORECASE)),
            'subquery': 'SELECT' in sparql_query[sparql_query.find('WHERE'):] if 'WHERE' in sparql_query else False,
            'optional': 'OPTIONAL' in sparql_query.upper(),
            'filter': 'FILTER' in sparql_query.upper(),
            'union': 'UNION' in sparql_query.upper(),
            'temporal': bool(re.search(rf'\b({temporal_pat})\b', sparql_query, re.IGNORECASE)),
            'offchain_metadata': bool(re.search(rf'\b({metadata_pat})\b', sparql_query, re.IGNORECASE))
        }

        complexity_score = sum(indicators.values())

        return {
            'complexity_score': complexity_score,
            'has_multi_relationship': indicators['multi_join'],
            'has_aggregation': indicators['aggregation'],
            'has_temporal': indicators['temporal'],
            'has_offchain_metadata': indicators['offchain_metadata']
        }

    @staticmethod
    def record_query_metrics(
        db: Session,
        nl_query: str,
        normalized_query: str,
        sparql_query: str,
        kv_results: Optional[Dict[str, Any]],
        is_sequential: bool,
        sparql_valid: bool,
        query_succeeded: bool,
        llm_latency_ms: int,
        sparql_latency_ms: int,
        total_latency_ms: int,
        user_id: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> QueryMetrics:
        """Record query execution metrics."""

        if db is None:
            return

        # Detect language
        detected_lang = LanguageDetector.detect_language(nl_query)

        # Calculate complexity
        complexity = MetricsService.calculate_complexity(sparql_query, kv_results)

        # Extract result info
        result_count = 0
        result_type = None
        if kv_results:
            result_type = kv_results.get('result_type')
            if result_type == 'multiple':
                result_count = kv_results.get('count', 0)
            elif result_type == 'single':
                result_count = 1

        # Check if federated
        is_federated = 'INJECT' in sparql_query or is_sequential

        # Semantic validity check (heuristic: has valid structure and returned results)
        semantic_valid = sparql_valid and (result_count > 0 or result_type == 'boolean')

        metric = QueryMetrics(
            user_id=user_id,
            nl_query=nl_query,
            normalized_query=normalized_query,
            detected_language=detected_lang,
            sparql_query=sparql_query,
            is_sequential=is_sequential,
            is_federated=is_federated,
            result_count=result_count,
            result_type=result_type,
            kv_results=kv_results,
            sparql_valid=sparql_valid,
            semantic_valid=semantic_valid,
            query_succeeded=query_succeeded,
            error_message=error_message,
            llm_latency_ms=llm_latency_ms,
            sparql_latency_ms=sparql_latency_ms,
            total_latency_ms=total_latency_ms,
            **complexity
        )

        db.add(metric)
        db.commit()

        logger.info(f"Recorded query metrics: lang={detected_lang}, complexity={complexity['complexity_score']}, latency={total_latency_ms}ms")

        return metric

    @staticmethod
    def record_kg_metrics(
        db: Session,
        entity_type: str,
        triples_loaded: int,
        load_duration_ms: int,
        load_succeeded: bool,
        batch_number: int,
        graph_uri: str,
        turtle_data: str
    ) -> KGMetrics:
        """Record knowledge graph load metrics."""

        # Check ontology alignment
        ontology_aligned = bool(re.search(r'\b(c:|b:)\w+', turtle_data))

        # Check for off-chain metadata
        has_offchain = bool(re.search(
            r'\b(hasPoolMetadata|hasTxMetadata|hasTokenName|hasDatumContent)\b',
            turtle_data
        ))

        metric = KGMetrics(
            entity_type=entity_type,
            triples_loaded=triples_loaded,
            load_duration_ms=load_duration_ms,
            load_succeeded=load_succeeded,
            ontology_aligned=ontology_aligned,
            has_offchain_metadata=has_offchain,
            batch_number=batch_number,
            graph_uri=graph_uri
        )

        db.add(metric)
        db.commit()

        return metric

    @staticmethod
    def record_dashboard_metrics(
        db: Session,
        user_id: int,
        dashboard_id: int,
        action_type: str,
        artifact_type: Optional[str] = None,
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