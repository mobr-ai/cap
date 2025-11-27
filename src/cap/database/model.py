# cap/database/model.py

from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    LargeBinary,
    DateTime,
    ForeignKey,
    JSON,
    Text,
    Index,
    text,
)

Base = declarative_base()

class User(Base):
    __tablename__ = "user"
    user_id        = Column(Integer, primary_key=True)
    email          = Column(String, unique=True, index=True, nullable=True)
    password_hash  = Column(String, nullable=True)
    google_id      = Column(String, unique=True, nullable=True)
    wallet_address = Column(String(128), index=True, nullable=True)
    username       = Column(String(30), unique=True, index=True, nullable=True)
    display_name   = Column(String(30), nullable=True)

    settings       = Column(String, nullable=True)
    refer_id       = Column(Integer)
    is_confirmed   = Column(Boolean, default=False)
    confirmation_token = Column(String(128), nullable=True)

    # on-prem avatar storage
    avatar_blob    = Column(LargeBinary, nullable=True)      # BYTEA
    avatar_mime    = Column(String(64), nullable=True)       # e.g., "image/png"
    avatar_etag    = Column(String(64), nullable=True)       # md5/sha1 for cache/If-None-Match

    # URL kept for compatibility
    avatar         = Column(String, nullable=True)

# -----------------------------
# Dashboards
# -----------------------------

class Dashboard(Base):
    __tablename__ = "dashboard"

    id          = Column(Integer, primary_key=True)
    user_id     = Column(Integer, ForeignKey("user.user_id"), index=True, nullable=False)
    name        = Column(String(100), nullable=False)
    description = Column(String(255), nullable=True)
    is_default  = Column(Boolean, default=False)

    created_at  = Column(DateTime, server_default=text("NOW()"))
    updated_at  = Column(
        DateTime,
        server_default=text("NOW()"),
        onupdate=text("NOW()"),
    )


class DashboardItem(Base):
    __tablename__ = "dashboard_item"

    id            = Column(Integer, primary_key=True)
    dashboard_id  = Column(
        Integer,
        ForeignKey("dashboard.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    # "table", "chart" (extend later e.g. "metric", "text", etc.)
    artifact_type = Column(String(50), nullable=False)

    # Short label shown to the user
    title         = Column(String(150), nullable=False)

    # Optional: original NL query that produced it
    source_query  = Column(String(1000), nullable=True)

    # Arbitrary JSON config/spec (vega spec, kv payload, etc.)
    config        = Column(JSON, nullable=False)

    position      = Column(Integer, nullable=False, server_default=text("0"))

    created_at    = Column(DateTime, server_default=text("NOW()"))


class QueryMetrics(Base):
    __tablename__ = "query_metrics"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("user.user_id"), index=True, nullable=True)

    # Query details
    nl_query = Column(Text, nullable=False)
    normalized_query = Column(Text, nullable=False, index=True)
    detected_language = Column(String(10), nullable=False, index=True)
    sparql_query = Column(Text, nullable=False)
    is_sequential = Column(Boolean, default=False)
    is_federated = Column(Boolean, default=False)

    # Results
    result_count = Column(Integer)
    result_type = Column(String(50))  # table, bar_chart, etc.
    kv_results = Column(JSON)

    # Quality indicators
    sparql_valid = Column(Boolean, nullable=False)
    semantic_valid = Column(Boolean, nullable=False)
    query_succeeded = Column(Boolean, nullable=False)
    error_message = Column(Text, nullable=True)

    # Complexity metrics
    complexity_score = Column(Integer, default=0)
    has_multi_relationship = Column(Boolean, default=False)
    has_aggregation = Column(Boolean, default=False)
    has_temporal = Column(Boolean, default=False)
    has_offchain_metadata = Column(Boolean, default=False)

    # Performance metrics (milliseconds)
    llm_latency_ms = Column(Integer)
    sparql_latency_ms = Column(Integer)
    total_latency_ms = Column(Integer)

    # Timestamps
    created_at = Column(DateTime, server_default=text("NOW()"), index=True)

    # Indexing for analytics
    __table_args__ = (
        Index('idx_query_metrics_language_date', 'detected_language', 'created_at'),
        Index('idx_query_metrics_user_date', 'user_id', 'created_at'),
        Index('idx_query_metrics_performance', 'total_latency_ms', 'created_at'),
    )


class KGMetrics(Base):
    __tablename__ = "kg_metrics"

    id = Column(Integer, primary_key=True)
    entity_type = Column(String(100), nullable=False, index=True)

    # Load metrics
    triples_loaded = Column(Integer, default=0)
    load_duration_ms = Column(Integer)
    load_succeeded = Column(Boolean, nullable=False)

    # Quality metrics
    ontology_aligned = Column(Boolean, default=True)
    has_offchain_metadata = Column(Boolean, default=False)

    # ETL context
    batch_number = Column(Integer)
    graph_uri = Column(String(500))

    created_at = Column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index('idx_kg_metrics_entity_date', 'entity_type', 'created_at'),
    )


class DashboardMetrics(Base):
    __tablename__ = "dashboard_metrics"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("user.user_id"), nullable=False, index=True)
    dashboard_id = Column(Integer, ForeignKey("dashboard.id"), nullable=False)

    action_type = Column(String(50), nullable=False)  # created, item_added, item_removed
    artifact_type = Column(String(50), nullable=True)  # table, bar_chart, etc.

    # State at time of action
    total_items = Column(Integer, default=0)
    unique_artifact_types = Column(Integer, default=0)

    created_at = Column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index('idx_dashboard_metrics_user_date', 'user_id', 'created_at'),
    )