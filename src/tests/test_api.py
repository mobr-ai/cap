# src/tests/test_api.py
import pytest
import logging

from httpx import AsyncClient
from urllib.parse import quote_plus
from cap.data.virtuoso import VirtuosoClient

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

TEST_GRAPH = "http://www.mobr.ai/ontologies/cardano/test"
TEST_DATA = """
PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
cardano:TestBlock rdf:type cardano:Block .
cardano:TestBlock cardano:status cardano:Pending .
"""

@pytest.fixture(autouse=True)
async def cleanup(virtuoso_client: VirtuosoClient):
    """Cleanup test graph before and after each test."""
    try:
        exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
        logger.debug(f"[CLEANUP] Before - Graph exists: {exists}")
        if exists:
            await virtuoso_client.delete_graph(TEST_GRAPH)
    except Exception as e:
        logger.error(f"[CLEANUP] Before error: {str(e)}")
    
    yield
    
    try:
        exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
        logger.debug(f"[CLEANUP] After - Graph exists: {exists}")
        if exists:
            await virtuoso_client.delete_graph(TEST_GRAPH)
    except Exception as e:
        logger.error(f"[CLEANUP] After error: {str(e)}")

@pytest.mark.asyncio
async def test_execute_query(async_client: AsyncClient, virtuoso_client: VirtuosoClient):
    """Test SPARQL query execution."""
    await virtuoso_client.create_graph(TEST_GRAPH, TEST_DATA)

    query = """
        SELECT ?s ?p ?o
        WHERE {
            ?s ?p ?o
        }
        LIMIT 10
    """

    response = await async_client.post(
        "/api/v1/query",
        json={
            "query": query,
            "type": "SELECT"
        }
    )
    
    assert response.status_code == 200
    assert "results" in response.json()

@pytest.mark.asyncio
async def test_create_graph(async_client: AsyncClient, virtuoso_client: VirtuosoClient):
    """Test graph creation."""
    response = await async_client.post(
        "/api/v1/graphs",
        json={
            "graph_uri": TEST_GRAPH,
            "turtle_data": TEST_DATA
        }
    )
    
    assert response.status_code == 200
    assert response.json()["success"] is True

@pytest.mark.asyncio
async def test_create_graph_conflict(async_client: AsyncClient, virtuoso_client: VirtuosoClient):
    """Test graph creation when graph already exists."""
    await virtuoso_client.create_graph(TEST_GRAPH, TEST_DATA)
    
    response = await async_client.post(
        "/api/v1/graphs",
        json={
            "graph_uri": TEST_GRAPH,
            "turtle_data": TEST_DATA
        }
    )
    
    assert response.status_code == 409

@pytest.mark.asyncio
async def test_read_graph(async_client: AsyncClient, virtuoso_client: VirtuosoClient):
    """Test graph reading."""
    # Create graph
    logger.debug("[READ TEST] Creating test graph")
    await virtuoso_client.create_graph(TEST_GRAPH, TEST_DATA)
    
    # Verify graph exists
    exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
    logger.debug(f"[READ TEST] Graph exists after creation: {exists}")
    
    # Make request
    encoded_graph = quote_plus(TEST_GRAPH)
    url = f"/api/v1/graphs/{encoded_graph}"
    logger.debug(f"[READ TEST] Making request to URL: {url}")
    
    response = await async_client.get(url)
    logger.debug(f"[READ TEST] Response status: {response.status_code}")
    logger.debug(f"[READ TEST] Response body: {response.text}")
    
    assert response.status_code == 200
    assert "data" in response.json()

@pytest.mark.asyncio
async def test_update_graph(async_client: AsyncClient, virtuoso_client: VirtuosoClient):
    """Test graph updating."""
    # Create graph
    logger.debug("[UPDATE TEST] Creating test graph")
    await virtuoso_client.create_graph(TEST_GRAPH, TEST_DATA)
    
    # Verify graph exists
    exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
    logger.debug(f"[UPDATE TEST] Graph exists after creation: {exists}")
    
    update_data = {
        "insert_data": """
            cardano:TestBlock cardano:status cardano:Confirmed .
        """,
        "delete_data": """
            cardano:TestBlock cardano:status cardano:Pending .
        """
    }

    # Make request
    encoded_graph = quote_plus(TEST_GRAPH)
    url = f"/api/v1/graphs/{encoded_graph}"
    logger.debug(f"[UPDATE TEST] Making request to URL: {url}")
    logger.debug(f"[UPDATE TEST] Request data: {update_data}")
    
    response = await async_client.patch(url, json=update_data)
    logger.debug(f"[UPDATE TEST] Response status: {response.status_code}")
    logger.debug(f"[UPDATE TEST] Response body: {response.text}")
    
    assert response.status_code == 200
    assert response.json()["success"] is True

@pytest.mark.asyncio
async def test_update_graph_validation(async_client: AsyncClient, virtuoso_client: VirtuosoClient):
    """Test graph update validation."""
    # Create graph first
    await virtuoso_client.create_graph(TEST_GRAPH, TEST_DATA)
    
    encoded_graph = quote_plus(TEST_GRAPH)
    response = await async_client.patch(
        f"/api/v1/graphs/{encoded_graph}",
        json={
            "insert_data": None,
            "delete_data": None
        }
    )
    
    assert response.status_code == 400

@pytest.mark.asyncio
async def test_delete_graph(async_client: AsyncClient, virtuoso_client: VirtuosoClient):
    """Test graph deletion."""
    # Create graph
    logger.debug("[DELETE TEST] Creating test graph")
    await virtuoso_client.create_graph(TEST_GRAPH, TEST_DATA)
    
    # Verify graph exists
    exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
    logger.debug(f"[DELETE TEST] Graph exists after creation: {exists}")
    
    # Make request
    encoded_graph = quote_plus(TEST_GRAPH)
    url = f"/api/v1/graphs/{encoded_graph}"
    logger.debug(f"[DELETE TEST] Making request to URL: {url}")
    
    response = await async_client.delete(url)
    logger.debug(f"[DELETE TEST] Response status: {response.status_code}")
    logger.debug(f"[DELETE TEST] Response body: {response.text}")
    
    assert response.status_code == 200
    assert response.json()["success"] is True