# cap/database/model.py
# cap/database/model.py
import uuid
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

    is_admin       = Column(
        Boolean,
        nullable=False,
        server_default=text("false"),
        default=False,  # optional but nice for in-memory defaults
    )

    # on-prem avatar storage
    avatar_blob    = Column(LargeBinary, nullable=True)      # BYTEA
    avatar_mime    = Column(String(64), nullable=True)       # e.g., "image/png"
    avatar_etag    = Column(String(64), nullable=True)       # md5/sha1 for cache/If-None-Match

    # URL kept for compatibility
    avatar         = Column(String, nullable=True)

class Conversation(Base):
    __tablename__ = "conversation"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("user.user_id"), index=True, nullable=False)
    title = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=text("NOW()"))
    updated_at = Column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))


class ConversationMessage(Base):
    __tablename__ = "conversation_message"

    id = Column(Integer, primary_key=True)
    conversation_id = Column(
        Integer,
        ForeignKey("conversation.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    user_id = Column(Integer, ForeignKey("user.user_id"), index=True, nullable=True)
    role = Column(String(50), nullable=False)
    content = Column(Text, nullable=False)
    nl_query_id = Column(Integer, ForeignKey("query_metrics.id"), index=True, nullable=True)
    created_at = Column(DateTime, server_default=text("NOW()"))


class ConversationArtifact(Base):
    __tablename__ = "conversation_artifact"

    id = Column(Integer, primary_key=True)

    conversation_id = Column(
        Integer,
        ForeignKey("conversation.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    # Optional linkage to the query metrics record that produced it
    nl_query_id = Column(
        Integer,
        ForeignKey("query_metrics.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )

    # Optional linkage to a message (if you later want)
    conversation_message_id = Column(
        Integer,
        ForeignKey("conversation_message.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )

    # "chart" | "table"
    artifact_type = Column(String(50), nullable=False)

    # e.g. "bar" | "pie" | "line" | "table" (optional)
    kv_type = Column(String(50), nullable=True)

    # config/spec payload (vegaSpec or kv table payload)
    config = Column(JSON, nullable=False)

    # stable dedupe key calculated by backend (unique per conversation)
    artifact_hash = Column(String(128), nullable=False)

    created_at = Column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index("idx_conversation_artifact_convo_created", "conversation_id", "created_at"),
        Index("uq_conversation_artifact_convo_hash", "conversation_id", "artifact_hash", unique=True),
    )


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
    conversation_message_id = Column(
        Integer,
        ForeignKey("conversation_message.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    conversation_id = Column(
        Integer,
        ForeignKey("conversation.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # "table", "chart" (extend later e.g. "metric", "indicator", etc.)
    artifact_type = Column(String(50), nullable=False)

    # Short label shown to the user
    title         = Column(String(150), nullable=False)

    # Optional: original NL query that produced it
    source_query  = Column(String(1000), nullable=True)

    # Arbitrary JSON config/spec (vega spec, kv payload, etc.)
    config        = Column(JSON, nullable=False)

    position      = Column(Integer, nullable=False, server_default=text("0"))

    created_at    = Column(DateTime, server_default=text("NOW()"))


# -----------------------------
# Metrics and Settings
# -----------------------------

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


class AdminSetting(Base):
    __tablename__ = "admin_setting"

    key = Column(String(64), primary_key=True)
    value = Column(JSON, nullable=False, server_default=text("'{}'::jsonb"))
    created_at = Column(DateTime, server_default=text("NOW()"))
    updated_at = Column(
        DateTime,
        server_default=text("NOW()"),
        onupdate=text("NOW()"),
    )


class SharedImage(Base):
    __tablename__ = "shared_image"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(Integer, ForeignKey("user.user_id"), index=True, nullable=False)

    access_token = Column(String(64), nullable=False, index=True)

    content_sha256 = Column(String(64), nullable=False)
    mime = Column(String(64), nullable=False)
    bytes = Column(Integer, nullable=False)
    etag = Column(String(64), nullable=False)

    storage_path = Column(String(512), nullable=False)

    created_at = Column(DateTime, server_default=text("NOW()"), index=True)
    expires_at = Column(DateTime, nullable=False, index=True)

    __table_args__ = (
        Index("uq_shared_image_user_sha", "user_id", "content_sha256", unique=True),
        Index("idx_shared_image_expires", "expires_at"),
    )
