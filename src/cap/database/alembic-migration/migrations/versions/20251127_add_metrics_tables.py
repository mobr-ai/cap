"""add metrics tables

Revision ID: 20251127_add_metrics_tables
Revises: 20251111_add_dashboard_tables
Create Date: 2025-11-27
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20251127_add_metrics_tables"
down_revision = "20251111_add_dashboard_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'query_metrics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=True),
        sa.Column('nl_query', sa.Text(), nullable=False),
        sa.Column('normalized_query', sa.Text(), nullable=False),
        sa.Column('detected_language', sa.String(length=10), nullable=False),
        sa.Column('sparql_query', sa.Text(), nullable=False),
        sa.Column('is_sequential', sa.Boolean(), nullable=True),
        sa.Column('is_federated', sa.Boolean(), nullable=True),
        sa.Column('result_count', sa.Integer(), nullable=True),
        sa.Column('result_type', sa.String(length=50), nullable=True),
        sa.Column('kv_results', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('sparql_valid', sa.Boolean(), nullable=False),
        sa.Column('semantic_valid', sa.Boolean(), nullable=False),
        sa.Column('query_succeeded', sa.Boolean(), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('complexity_score', sa.Integer(), nullable=True),
        sa.Column('has_multi_relationship', sa.Boolean(), nullable=True),
        sa.Column('has_aggregation', sa.Boolean(), nullable=True),
        sa.Column('has_temporal', sa.Boolean(), nullable=True),
        sa.Column('has_offchain_metadata', sa.Boolean(), nullable=True),
        sa.Column('llm_latency_ms', sa.Integer(), nullable=True),
        sa.Column('sparql_latency_ms', sa.Integer(), nullable=True),
        sa.Column('total_latency_ms', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('NOW()'), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['user.user_id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_query_metrics_language_date', 'query_metrics', ['detected_language', 'created_at'])
    op.create_index('idx_query_metrics_normalized', 'query_metrics', ['normalized_query'])
    op.create_index('idx_query_metrics_performance', 'query_metrics', ['total_latency_ms', 'created_at'])
    op.create_index('idx_query_metrics_user_date', 'query_metrics', ['user_id', 'created_at'])

    op.create_table(
        'kg_metrics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('entity_type', sa.String(length=100), nullable=False),
        sa.Column('triples_loaded', sa.Integer(), nullable=True),
        sa.Column('load_duration_ms', sa.Integer(), nullable=True),
        sa.Column('load_succeeded', sa.Boolean(), nullable=False),
        sa.Column('ontology_aligned', sa.Boolean(), nullable=True),
        sa.Column('has_offchain_metadata', sa.Boolean(), nullable=True),
        sa.Column('batch_number', sa.Integer(), nullable=True),
        sa.Column('graph_uri', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('NOW()'), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_kg_metrics_entity_date', 'kg_metrics', ['entity_type', 'created_at'])

    op.create_table(
        'dashboard_metrics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('dashboard_id', sa.Integer(), nullable=False),
        sa.Column('action_type', sa.String(length=50), nullable=False),
        sa.Column('artifact_type', sa.String(length=50), nullable=True),
        sa.Column('total_items', sa.Integer(), nullable=True),
        sa.Column('unique_artifact_types', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('NOW()'), nullable=True),
        sa.ForeignKeyConstraint(['dashboard_id'], ['dashboard.id'], ),
        sa.ForeignKeyConstraint(['user_id'], ['user.user_id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_dashboard_metrics_user_date', 'dashboard_metrics', ['user_id', 'created_at'])


def downgrade() -> None:
    op.drop_index('idx_dashboard_metrics_user_date', table_name='dashboard_metrics')
    op.drop_table('dashboard_metrics')
    op.drop_index('idx_kg_metrics_entity_date', table_name='kg_metrics')
    op.drop_table('kg_metrics')
    op.drop_index('idx_query_metrics_user_date', table_name='query_metrics')
    op.drop_index('idx_query_metrics_performance', table_name='query_metrics')
    op.drop_index('idx_query_metrics_normalized', table_name='query_metrics')
    op.drop_index('idx_query_metrics_language_date', table_name='query_metrics')
    op.drop_table('query_metrics')