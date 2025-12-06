import pytest
from datetime import datetime
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cap.etl.cdb.service import ETLPipeline, ETLStatus
from cap.rdf.triplestore import TriplestoreClient
from cap.config import settings

TEST_ETL_GRAPH = "http://test.etl.pipeline"
TEST_METADATA_GRAPH = "http://test.etl.metadata"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

@pytest.fixture(autouse=True)
async def cleanup(virtuoso_client: TriplestoreClient):
    """Cleanup test graphs before and after each test."""
    try:
        for graph in [TEST_ETL_GRAPH, TEST_METADATA_GRAPH]:
            exists = await virtuoso_client.check_graph_exists(graph)
            if exists:
                await virtuoso_client.delete_graph(graph)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

    yield

    try:
        for graph in [TEST_ETL_GRAPH, TEST_METADATA_GRAPH]:
            exists = await virtuoso_client.check_graph_exists(graph)
            if exists:
                await virtuoso_client.delete_graph(graph)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

@pytest.fixture
def test_db_session():
    """Create a test database session."""
    engine = create_engine(
        f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@"
        f"{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}",
        pool_pre_ping=True
    )
    SessionFactory = sessionmaker(bind=engine)
    return SessionFactory()

@pytest.mark.asyncio
async def test_etl_pipeline_initialization():
    """Test ETL pipeline initialization."""
    pipeline = ETLPipeline(batch_size=100, sync_interval=60)

    assert pipeline.batch_size == 100
    assert pipeline.sync_interval == 60
    assert not pipeline.running
    assert len(pipeline.progress) == len(pipeline.entity_types)

    for entity_type in pipeline.entity_types:
        progress = pipeline.progress[entity_type]
        assert progress.entity_type == entity_type
        assert progress.last_processed_id is None
        assert progress.total_records == 0
        assert progress.processed_records == 0
        assert progress.status == ETLStatus.RUNNING

@pytest.mark.asyncio
async def test_etl_sync_status():
    """Test getting ETL sync status."""
    pipeline = ETLPipeline(batch_size=100)

    # Set some progress
    pipeline.progress['account'].total_records = 1000
    pipeline.progress['account'].processed_records = 500
    pipeline.progress['account'].status = ETLStatus.RUNNING

    status = await pipeline.get_sync_status()

    assert not status['running']
    assert 'entity_progress' in status

    account_progress = status['entity_progress']['account']
    assert account_progress['status'] == 'running'
    assert account_progress['total_records'] == 1000
    assert account_progress['processed_records'] == 500
    assert account_progress['progress_percentage'] == 50.0

@pytest.mark.asyncio
async def test_etl_reset_progress(virtuoso_client: TriplestoreClient):
    """Test resetting ETL progress."""
    pipeline = ETLPipeline(batch_size=100)

    # Set some progress
    pipeline.progress['account'].last_processed_id = 1000
    pipeline.progress['account'].processed_records = 1000

    # Reset progress
    await pipeline.reset_sync_progress(['account'])

    # Verify reset
    assert pipeline.progress['account'].last_processed_id is None
    assert pipeline.progress['account'].processed_records == 0
    assert pipeline.progress['account'].total_records == 0

@pytest.mark.asyncio
async def test_etl_error_handling():
    """Test ETL error handling."""
    pipeline = ETLPipeline(batch_size=100)

    # Test with invalid entity type
    pipeline.progress['account'].status = ETLStatus.RUNNING

    # Simulate error
    pipeline.progress['account'].status = ETLStatus.ERROR
    pipeline.progress['account'].error_message = "Test error"

    status = await pipeline.get_sync_status()

    account_status = status['entity_progress']['account']
    assert account_status['status'] == 'error'
    assert account_status['error_message'] == "Test error"
