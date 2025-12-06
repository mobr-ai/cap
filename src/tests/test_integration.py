import pytest
import logging
from httpx import AsyncClient
from cap.rdf.triplestore import TriplestoreClient

TEST_GRAPH = "http://test.integration"
logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
async def cleanup(virtuoso_client: TriplestoreClient):
    """Cleanup test graph before and after each test."""
    # Cleanup before test
    try:
        exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
        if exists:
            await virtuoso_client.delete_graph(TEST_GRAPH)
    except Exception as e:
        print(f"Cleanup before test failed: {e}")

    yield

    # Cleanup after test
    try:
        exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
        if exists:
            await virtuoso_client.delete_graph(TEST_GRAPH)
    except Exception as e:
        print(f"Cleanup after test failed: {e}")
