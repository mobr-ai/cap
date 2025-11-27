import pytest
from cap.rdf.triplestore import TriplestoreClient

TEST_GRAPH = "http://test.graph"
TEST_PREFIXES = {
    "test": "http://test.graph#"
}
TEST_TRIPLE = """
PREFIX test: <http://test.graph#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
test:subject rdf:type test:TestType .
"""

@pytest.mark.asyncio
async def test_check_graph_exists(virtuoso_client: TriplestoreClient):
    # Clean start
    if await virtuoso_client.check_graph_exists(TEST_GRAPH):
        await virtuoso_client.delete_graph(TEST_GRAPH)

    # Test non-existent graph
    exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
    assert not exists

@pytest.mark.asyncio
async def test_create_and_delete_graph(virtuoso_client: TriplestoreClient):
    # Clean start
    if await virtuoso_client.check_graph_exists(TEST_GRAPH):
        await virtuoso_client.delete_graph(TEST_GRAPH)

    # Create graph
    await virtuoso_client.create_graph(TEST_GRAPH, TEST_TRIPLE)
    exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
    assert exists

    # Delete graph
    await virtuoso_client.delete_graph(TEST_GRAPH)
    exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
    assert not exists

@pytest.mark.asyncio
async def test_execute_query(virtuoso_client: TriplestoreClient):
    # Setup
    await virtuoso_client.create_graph(TEST_GRAPH, TEST_TRIPLE)

    # Test query
    query = f"""
    PREFIX test: <http://test.graph#>
    ASK WHERE {{
        GRAPH <{TEST_GRAPH}> {{
            ?s a test:TestType
        }}
    }}
    """
    result = await virtuoso_client.execute_query(query)
    assert result.get('boolean') is True

    # Cleanup
    await virtuoso_client.delete_graph(TEST_GRAPH)

@pytest.mark.asyncio
async def test_update_graph(virtuoso_client: TriplestoreClient):
    # Setup
    await virtuoso_client.create_graph(TEST_GRAPH, TEST_TRIPLE)

    # Update
    update_success = await virtuoso_client.update_graph(
        TEST_GRAPH,
        delete_data="test:subject rdf:type test:TestType .",
        insert_data="test:subject rdf:type test:UpdatedType .",
        additional_prefixes=TEST_PREFIXES
    )

    assert update_success is True

    # Verify
    query = f"""
    PREFIX test: <http://test.graph#>
    ASK WHERE {{
        GRAPH <{TEST_GRAPH}> {{
            test:subject a test:UpdatedType
        }}
    }}
    """
    result = await virtuoso_client.execute_query(query)
    assert result.get('boolean') is True

    # Cleanup
    await virtuoso_client.delete_graph(TEST_GRAPH)