import pytest
from datetime import datetime
import logging

from cap.etl.cdb.loaders.loader import CDBLoader
from cap.etl.cdb.service import ETLProgress, ETLStatus
from cap.rdf.triplestore import TriplestoreClient
from cap.config import settings

TEST_LOADER_GRAPH = "http://test.etl.loader"
TEST_METADATA_GRAPH = "http://test.etl.loader.metadata"

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
async def cleanup(virtuoso_client: TriplestoreClient):
    """Cleanup test graphs before and after each test."""
    try:
        for graph in [TEST_LOADER_GRAPH, TEST_METADATA_GRAPH]:
            exists = await virtuoso_client.check_graph_exists(graph)
            if exists:
                await virtuoso_client.delete_graph(graph)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

    yield

    try:
        for graph in [TEST_LOADER_GRAPH, TEST_METADATA_GRAPH]:
            exists = await virtuoso_client.check_graph_exists(graph)
            if exists:
                await virtuoso_client.delete_graph(graph)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

@pytest.mark.asyncio
async def test_loader_initialization():
    """Test CDB loader initialization."""
    loader = CDBLoader()
    assert loader.virtuoso_client is not None

@pytest.mark.asyncio
async def test_loader_load_batch_empty_data(virtuoso_client: TriplestoreClient):
    """Test loading empty batch data."""
    loader = CDBLoader()

    # Load empty data
    success = await loader.load_batch(TEST_LOADER_GRAPH, "", {"entity_type": "test"})
    assert success

    # Verify graph exists but is empty
    exists = await virtuoso_client.check_graph_exists(TEST_LOADER_GRAPH)
    assert not exists  # Empty graph is not created

@pytest.mark.asyncio
async def test_loader_error_progress_metadata():
    """Test saving progress metadata with error."""
    loader = CDBLoader()

    progress = ETLProgress(
        entity_type="error_test",
        last_processed_id=1000,
        total_records=5000,
        processed_records=1000,
        status=ETLStatus.ERROR,
        last_updated=datetime.now(),
        error_message="Test error message"
    )

    # Should not save when there's an error
    await loader.save_progress_metadata("error_test", progress, TEST_METADATA_GRAPH)

@pytest.mark.asyncio
async def test_loader_clear_nonexistent_graph():
    """Test clearing non-existent graph."""
    loader = CDBLoader()

    # Try to clear non-existent graph
    success = await loader.clear_graph_data("http://nonexistent.graph")
    assert not success