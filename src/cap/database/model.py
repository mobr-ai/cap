import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()


class User(Base):
    __tablename__ = "user"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str | None] = mapped_column(String, unique=True, index=True, nullable=True)
    password_hash: Mapped[str | None] = mapped_column(String, nullable=True)
    google_id: Mapped[str | None] = mapped_column(String, unique=True, nullable=True)
    wallet_address: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)
    username: Mapped[str | None] = mapped_column(String(30), unique=True, index=True, nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(30), nullable=True)

    settings: Mapped[str | None] = mapped_column(String, nullable=True)
    refer_id: Mapped[int | None] = mapped_column(Integer)
    is_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    confirmation_token: Mapped[str | None] = mapped_column(String(128), nullable=True)

    is_admin: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
        default=False,
    )

    avatar_blob: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    avatar_mime: Mapped[str | None] = mapped_column(String(64), nullable=True)
    avatar_etag: Mapped[str | None] = mapped_column(String(64), nullable=True)

    avatar: Mapped[str | None] = mapped_column(String, nullable=True)


class CardanoAuthChallenge(Base):
    __tablename__ = "cardano_auth_challenge"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    challenge_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    wallet_address: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    wallet_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    nonce: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    message_hex: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'pending'"))
    used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    __table_args__ = (
        Index("idx_cardano_auth_challenge_wallet_status", "wallet_address", "status"),
        Index("idx_cardano_auth_challenge_expires_status", "expires_at", "status"),
    )


class BillingPlan(Base):
    __tablename__ = "billing_plan"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    entitlement_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"), default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))


class BillingPrice(Base):
    __tablename__ = "billing_price"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    plan_id: Mapped[int] = mapped_column(Integer, ForeignKey("billing_plan.id", ondelete="CASCADE"), nullable=False, index=True)
    network: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    currency: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'lovelace'"))
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    duration_days: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"), default=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=text("NOW()"), index=True)
    ends_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))

    __table_args__ = (
        Index("idx_billing_price_plan_network_active", "plan_id", "network", "is_active"),
    )


class BillingPaymentAddress(Base):
    __tablename__ = "billing_payment_address"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    network: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    address: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    label: Mapped[str | None] = mapped_column(String(120), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"), default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))

    __table_args__ = (
        Index("idx_billing_payment_address_network_active", "network", "is_active"),
    )


class BillingFeatureConfig(Base):
    __tablename__ = "billing_feature_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    feature_code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    free_limit_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    period_days: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("30"))
    payg_price_lovelace: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("TRUE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))


class BillingNotificationSetting(Base):
    __tablename__ = "billing_notification_setting"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_code: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"), default=True)
    audience: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'user'"), default="user")
    channel: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'email'"), default="email")
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by_user_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("user.user_id", ondelete="SET NULL"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))

    __table_args__ = (
        UniqueConstraint(
            "event_code",
            "audience",
            "channel",
            name="uq_billing_notification_setting_event_audience_channel",
        ),
        Index("idx_billing_notification_setting_enabled", "enabled"),
    )


class PaymentSession(Base):
    __tablename__ = "payment_session"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_id: Mapped[str] = mapped_column(String(80), unique=True, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False, index=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'plan_purchase'"), index=True)

    plan_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("billing_plan.id", ondelete="SET NULL"), nullable=True, index=True)
    price_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("billing_price.id", ondelete="SET NULL"), nullable=True, index=True)
    payment_address_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("billing_payment_address.id", ondelete="SET NULL"), nullable=True, index=True)

    plan_code_snapshot: Mapped[str] = mapped_column(String(64), nullable=False)
    entitlement_code_snapshot: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    network_snapshot: Mapped[str] = mapped_column(String(32), nullable=False)
    currency_snapshot: Mapped[str] = mapped_column(String(32), nullable=False)
    amount_snapshot: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payment_address_snapshot: Mapped[str] = mapped_column(String(256), nullable=False)
    duration_days_snapshot: Mapped[int] = mapped_column(Integer, nullable=False)

    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'pending'"), index=True)
    provider: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tx_hash: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    provider_response: Mapped[Any | None] = mapped_column(JSON, nullable=True)

    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    paid_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index("idx_payment_session_user_status", "user_id", "status"),
        Index("idx_payment_session_status_expires", "status", "expires_at"),
    )


class UserEntitlement(Base):
    __tablename__ = "user_entitlement"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False, index=True)
    entitlement_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    payment_session_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("payment_session.id", ondelete="SET NULL"), nullable=True, index=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index("idx_user_entitlement_user_code_status", "user_id", "entitlement_code", "status"),
        Index("idx_user_entitlement_code_status_expires", "entitlement_code", "status", "expires_at"),
    )


class UserCreditBalance(Base):
    __tablename__ = "user_credit_balance"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False, index=True)
    currency: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'lovelace'"))
    balance: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))

    __table_args__ = (
        Index("uq_user_credit_balance_user_currency", "user_id", "currency", unique=True),
    )


class UserCreditLedger(Base):
    __tablename__ = "user_credit_ledger"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False, index=True)
    currency: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'lovelace'"))
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    balance_after: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reason: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    payment_session_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("payment_session.id", ondelete="SET NULL"), nullable=True, index=True)
    related_entitlement_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("user_entitlement.id", ondelete="SET NULL"), nullable=True, index=True)
    metadata_json: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index("idx_user_credit_ledger_user_created", "user_id", "created_at"),
    )


class UserBillingPreference(Base):
    __tablename__ = "user_billing_preference"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False, unique=True, index=True)
    auto_renew_premium_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"), default=False)
    auto_renew_plan_code: Mapped[str] = mapped_column(String(64), nullable=False, server_default=text("'cap_premium_access'"))
    payg_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"), default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))


class UserUsagePeriod(Base):
    __tablename__ = "user_usage_period"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False, index=True)
    feature_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    period_start: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    period_end: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    used_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    limit_count: Mapped[int] = mapped_column(Integer, nullable=False)
    free_query_tokens: Mapped[float | None] = mapped_column(Float, nullable=True)
    free_query_refilled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "feature_code",
            "period_start",
            "period_end",
            name="uq_user_usage_period_user_feature_window",
        ),
        Index("idx_user_usage_period_user_feature", "user_id", "feature_code"),
    )


class Conversation(Base):
    __tablename__ = "conversation"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), index=True, nullable=False)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))


class ConversationMessage(Base):
    __tablename__ = "conversation_message"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    conversation_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("conversation.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    user_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("user.user_id"), index=True, nullable=True)
    role: Mapped[str] = mapped_column(String(50), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    nl_query_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("query_metrics.id"), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))


class ConversationArtifact(Base):
    __tablename__ = "conversation_artifact"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    conversation_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("conversation.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    nl_query_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("query_metrics.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    conversation_message_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("conversation_message.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    artifact_type: Mapped[str] = mapped_column(String(50), nullable=False)
    kv_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    config: Mapped[Any] = mapped_column(JSON, nullable=False)
    artifact_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index("idx_conversation_artifact_convo_created", "conversation_id", "created_at"),
        Index("uq_conversation_artifact_convo_hash", "conversation_id", "artifact_hash", unique=True),
    )


class Dashboard(Base):
    __tablename__ = "dashboard"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))


class DashboardItem(Base):
    __tablename__ = "dashboard_item"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dashboard_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("dashboard.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    conversation_message_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("conversation_message.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    conversation_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("conversation.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    artifact_type: Mapped[str] = mapped_column(String(50), nullable=False)
    title: Mapped[str] = mapped_column(String(150), nullable=False)
    source_query: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    config: Mapped[Any] = mapped_column(JSON, nullable=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))


class QueryMetrics(Base):
    __tablename__ = "query_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("user.user_id"), index=True, nullable=True)

    nl_query: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_query: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    detected_language: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    sparql_query: Mapped[str] = mapped_column(Text, nullable=False)
    is_sequential: Mapped[bool] = mapped_column(Boolean, default=False)
    is_federated: Mapped[bool] = mapped_column(Boolean, default=False)

    result_count: Mapped[int | None] = mapped_column(Integer)
    result_type: Mapped[str | None] = mapped_column(String(50))
    kv_results: Mapped[Any | None] = mapped_column(JSON)

    sparql_valid: Mapped[bool] = mapped_column(Boolean, nullable=False)
    semantic_valid: Mapped[bool] = mapped_column(Boolean, nullable=False)
    query_succeeded: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    complexity_score: Mapped[int] = mapped_column(Integer, default=0)
    has_multi_relationship: Mapped[bool] = mapped_column(Boolean, default=False)
    has_aggregation: Mapped[bool] = mapped_column(Boolean, default=False)
    has_temporal: Mapped[bool] = mapped_column(Boolean, default=False)
    has_offchain_metadata: Mapped[bool] = mapped_column(Boolean, default=False)

    llm_latency_ms: Mapped[int | None] = mapped_column(Integer)
    sparql_latency_ms: Mapped[int | None] = mapped_column(Integer)
    total_latency_ms: Mapped[int | None] = mapped_column(Integer)

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index("idx_query_metrics_language_date", "detected_language", "created_at"),
        Index("idx_query_metrics_user_date", "user_id", "created_at"),
        Index("idx_query_metrics_performance", "total_latency_ms", "created_at"),
    )


class KGMetrics(Base):
    __tablename__ = "kg_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    triples_loaded: Mapped[int] = mapped_column(Integer, default=0)
    load_duration_ms: Mapped[int | None] = mapped_column(Integer)
    load_succeeded: Mapped[bool] = mapped_column(Boolean, nullable=False)
    ontology_aligned: Mapped[bool] = mapped_column(Boolean, default=True)
    has_offchain_metadata: Mapped[bool] = mapped_column(Boolean, default=False)
    batch_number: Mapped[int | None] = mapped_column(Integer)
    graph_uri: Mapped[str | None] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index("idx_kg_metrics_entity_date", "entity_type", "created_at"),
    )


class DashboardMetrics(Base):
    __tablename__ = "dashboard_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False, index=True)
    dashboard_id: Mapped[int] = mapped_column(Integer, ForeignKey("dashboard.id"), nullable=False)
    action_type: Mapped[str] = mapped_column(String(50), nullable=False)
    artifact_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    total_items: Mapped[int] = mapped_column(Integer, default=0)
    unique_artifact_types: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)

    __table_args__ = (
        Index("idx_dashboard_metrics_user_date", "user_id", "created_at"),
    )


class AdminSetting(Base):
    __tablename__ = "admin_setting"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[Any] = mapped_column(JSON, nullable=False, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))


class BetaProgramRegistration(Base):
    __tablename__ = "beta_program_registration"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    full_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    role: Mapped[str | None] = mapped_column(String(80), nullable=True)
    organization: Mapped[str | None] = mapped_column(String(160), nullable=True)
    use_case: Mapped[str | None] = mapped_column(Text, nullable=True)
    language: Mapped[str] = mapped_column(String(12), nullable=False, server_default=text("'en'"))
    source: Mapped[str] = mapped_column(String(80), nullable=False, server_default=text("'welcome_beta_cta'"))
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'new'"), index=True)
    admin_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=text("NOW()"),
        onupdate=text("NOW()"),
    )

    __table_args__ = (
        Index("idx_beta_registration_status_created", "status", "created_at"),
    )


class SharedImage(Base):
    __tablename__ = "shared_image"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id"), index=True, nullable=False)
    access_token: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    mime: Mapped[str] = mapped_column(String(64), nullable=False)
    bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    etag: Mapped[str] = mapped_column(String(64), nullable=False)
    storage_path: Mapped[str] = mapped_column(String(512), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    __table_args__ = (
        Index("uq_shared_image_user_sha", "user_id", "content_sha256", unique=True),
        Index("idx_shared_image_expires", "expires_at"),
    )


class Asset(Base):
    __tablename__ = "asset"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    asset_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    policy_id: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    asset_name_hex: Mapped[str | None] = mapped_column(Text, nullable=True)
    decimals: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class AssetOHLCV(Base):
    __tablename__ = "asset_ohlcv"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    asset_id: Mapped[str] = mapped_column(Text, ForeignKey("asset.asset_id", ondelete="CASCADE"), nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    interval: Mapped[str] = mapped_column(Text, nullable=False)
    open: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'unknown'"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))

    __table_args__ = (
        UniqueConstraint("asset_id", "ts", "interval", "source", name="uq_asset_ohlcv_asset_ts_interval_source"),
        Index("ix_asset_ohlcv_asset_ts", "asset_id", "ts"),
        Index("ix_asset_ohlcv_ts_interval", "ts", "interval"),
    )

class TelegramAccount(Base):
    __tablename__ = "telegram_account"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True, index=True)
    cap_user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("user.user_id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    telegram_username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    auth_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))


class TelegramChatBinding(Base):
    __tablename__ = "telegram_chat_binding"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True, index=True)
    chat_type: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_by_telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    default_cap_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("user.user_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), onupdate=text("NOW()"))


class TelegramRenderedImage(Base):
    __tablename__ = "telegram_rendered_image"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    cap_user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user.user_id", ondelete="CASCADE"), nullable=False, index=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telegram_chat_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    access_token: Mapped[str] = mapped_column(String(96), nullable=False, index=True)
    mime: Mapped[str] = mapped_column(String(64), nullable=False)
    bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    etag: Mapped[str] = mapped_column(String(64), nullable=False)
    storage_path: Mapped[str] = mapped_column(String(512), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

class TelegramGuestUsagePeriod(Base):
    __tablename__ = "telegram_guest_usage_period"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    feature_code: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        server_default=text("'telegram_guest_nl_query'"),
        index=True,
    )
    period_start: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    period_end: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    used_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    limit_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("3"))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("NOW()"), index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=text("NOW()"),
        onupdate=text("NOW()"),
    )

    __table_args__ = (
        UniqueConstraint(
            "telegram_user_id",
            "feature_code",
            "period_start",
            "period_end",
            name="uq_telegram_guest_usage_user_feature_window",
        ),
        Index("idx_telegram_guest_usage_user_feature", "telegram_user_id", "feature_code"),
    )
