import pytest
import logging
from httpx import AsyncClient
from cap.virtuoso import VirtuosoClient

TEST_GRAPH = "http://test.integration"
logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
async def cleanup(virtuoso_client: VirtuosoClient):
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

@pytest.mark.asyncio
async def test_full_graph_lifecycle(async_client: AsyncClient):
    # Create graph with initial data
    initial_data = """
    PREFIX test: <http://test.graph#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    test:Block1 rdf:type test:Block .
    """
    
    response = await async_client.post(
        "/api/v1/graphs",
        json={
            "graph_uri": TEST_GRAPH,
            "turtle_data": initial_data
        }
    )
    logger.debug(f"Create response: {response.status_code}")
    logger.debug(f"Create response body: {response.text}")
    assert response.status_code == 200
    
    # Read and verify data
    query = f"""
    PREFIX test: <http://test.graph#>
    ASK WHERE {{
        GRAPH <{TEST_GRAPH}> {{
            test:Block1 a test:Block
        }}
    }}
    """
    response = await async_client.post(
        "/api/v1/query",
        json={
            "query": query,
            "type": "ASK"
        }
    )
    logger.debug(f"Query response: {response.status_code}")
    logger.debug(f"Query response body: {response.text}")
    assert response.status_code == 200
    assert response.json()["results"]["boolean"] is True
    
    # Update data - Now including required prefixes
    update = {
        "delete_data": """
            test:Block1 rdf:type test:Block .
        """,
        "insert_data": """
            test:Block1 rdf:type test:UpdatedBlock .
        """,
        "prefixes": {
            "test": "http://test.graph#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        }
    }
    response = await async_client.patch(
        f"/api/v1/graphs/{TEST_GRAPH}",
        json=update
    )
    logger.debug(f"Update response: {response.status_code}")
    logger.debug(f"Update response body: {response.text}")
    assert response.status_code == 200
    
    # Verify update
    query = f"""
    PREFIX test: <http://test.graph#>
    ASK WHERE {{
        GRAPH <{TEST_GRAPH}> {{
            test:Block1 a test:UpdatedBlock
        }}
    }}
    """
    response = await async_client.post(
        "/api/v1/query",
        json={
            "query": query,
            "type": "ASK"
        }
    )
    logger.debug(f"Verify update response: {response.status_code}")
    logger.debug(f"Verify update response body: {response.text}")
    assert response.status_code == 200
    assert response.json()["results"]["boolean"] is True
    
    # Delete graph
    response = await async_client.delete(f"/api/v1/graphs/{TEST_GRAPH}")
    logger.debug(f"Delete response: {response.status_code}")
    logger.debug(f"Delete response body: {response.text}")
    assert response.status_code == 200
    
    # Verify deletion
    query = f"""
    ASK WHERE {{
        GRAPH <{TEST_GRAPH}> {{
            ?s ?p ?o
        }}
    }}
    """
    response = await async_client.post(
        "/api/v1/query",
        json={
            "query": query,
            "type": "ASK"
        }
    )
    logger.debug(f"Verify delete response: {response.status_code}")
    logger.debug(f"Verify delete response body: {response.text}")
    assert response.status_code == 200
    assert response.json()["results"]["boolean"] is False