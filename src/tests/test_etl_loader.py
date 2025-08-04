import pytest
from datetime import datetime
import logging

from cap.etl.cdb.loaders.loader import CDBLoader
from cap.etl.cdb.service import ETLProgress, ETLStatus
from cap.data.virtuoso import VirtuosoClient
from cap.config import settings

TEST_LOADER_GRAPH = "http://test.etl.loader"
TEST_METADATA_GRAPH = "http://test.etl.loader.metadata"

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
async def cleanup(virtuoso_client: VirtuosoClient):
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
async def test_loader_load_batch_empty_data(virtuoso_client: VirtuosoClient):
    """Test loading empty batch data."""
    loader = CDBLoader()

    # Load empty data
    success = await loader.load_batch(TEST_LOADER_GRAPH, "", {"entity_type": "test"})
    assert success

    # Verify graph exists but is empty
    exists = await virtuoso_client.check_graph_exists(TEST_LOADER_GRAPH)
    assert not exists  # Empty graph is not created

@pytest.mark.asyncio
async def test_loader_load_batch_with_prefixes(virtuoso_client: VirtuosoClient):
    """Test loading batch with prefix handling."""
    loader = CDBLoader()

    turtle_data = """
    PREFIX test: <http://test.example.com#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    test:subject1 rdf:type test:TestType .
    test:subject2 rdf:type test:TestType .
    """

    batch_info = {
        "entity_type": "test",
        "size": 2,
        "batch_number": 1
    }

    success = await loader.load_batch(TEST_LOADER_GRAPH, turtle_data, batch_info)
    assert success

    # Verify data was loaded
    query = f"""
    SELECT (COUNT(*) as ?count)
    WHERE {{
        GRAPH <{TEST_LOADER_GRAPH}> {{
            ?s ?p ?o
        }}
    }}
    """

    results = await virtuoso_client.execute_query(query)
    count = int(results['results']['bindings'][0]['count']['value'])
    assert count == 2

@pytest.mark.asyncio
async def test_loader_load_large_batch(virtuoso_client: VirtuosoClient):
    """Test loading large batch with chunking."""
    loader = CDBLoader()

    # Generate large turtle data
    turtle_lines = []

    for i in range(2000):
        turtle_lines.append(f"test:entity{i} rdf:type test:LargeEntity .")

    turtle_data = '\n'.join(turtle_lines)

    batch_info = {
        "entity_type": "large_test",
        "size": 2000
    }

    additional_prefixes = {"test": "http://test.example.com#"}
    success = await loader.load_batch(TEST_LOADER_GRAPH, turtle_data, batch_info, additional_prefixes)
    assert success

    # Verify all data was loaded
    query = f"""
    PREFIX test: <http://test.example.com#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT (COUNT(*) as ?count)
    WHERE {{
        GRAPH <{TEST_LOADER_GRAPH}> {{
            ?s rdf:type test:LargeEntity
        }}
    }}
    """

    results = await virtuoso_client.execute_query(query)
    count = int(results['results']['bindings'][0]['count']['value'])
    assert count == 2000

@pytest.mark.asyncio
async def test_loader_save_progress_metadata(virtuoso_client: VirtuosoClient):
    """Test saving ETL progress metadata."""
    loader = CDBLoader()

    progress = ETLProgress(
        entity_type="test_entity",
        last_processed_id=5000,
        total_records=10000,
        processed_records=5000,
        status=ETLStatus.RUNNING,
        last_updated=datetime.now()
    )

    initial_data = f"""
    @prefix test: <{TEST_METADATA_GRAPH}#> .
    test:placeholder test:property "value" .
    """

    created = await virtuoso_client.create_graph(TEST_METADATA_GRAPH, initial_data)
    assert created

    exists = await virtuoso_client.check_graph_exists(TEST_METADATA_GRAPH)
    assert exists

    await loader.save_progress_metadata("test_entity", progress, TEST_METADATA_GRAPH)

    # Verify metadata was saved
    query = f"""
    PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>

    SELECT ?lastId ?total ?processed ?status
    WHERE {{
        GRAPH <{TEST_METADATA_GRAPH}> {{
            <{settings.CARDANO_GRAPH}/etl/progress/test_entity>
                cardano:hasLastProcessedId ?lastId ;
                cardano:hasTotalRecords ?total ;
                cardano:hasProcessedRecords ?processed ;
                cardano:hasStatus ?status .
        }}
    }}
    """

    results = await virtuoso_client.execute_query(query)
    assert results['results']['bindings']

    binding = results['results']['bindings'][0]
    assert int(binding['lastId']['value']) == 5000
    assert int(binding['total']['value']) == 10000
    assert int(binding['processed']['value']) == 5000
    assert binding['status']['value'] == "running"

@pytest.mark.asyncio
async def test_loader_update_progress_metadata(virtuoso_client: VirtuosoClient):
    """Test updating existing progress metadata."""
    loader = CDBLoader()

    # Save initial progress
    progress1 = ETLProgress(
        entity_type="update_test",
        last_processed_id=1000,
        total_records=5000,
        processed_records=1000,
        status=ETLStatus.RUNNING,
        last_updated=datetime.now()
    )

    await loader.save_progress_metadata("update_test", progress1, TEST_METADATA_GRAPH)

    # Update progress
    progress2 = ETLProgress(
        entity_type="update_test",
        last_processed_id=2000,
        total_records=5000,
        processed_records=2000,
        status=ETLStatus.RUNNING,
        last_updated=datetime.now()
    )

    await loader.save_progress_metadata("update_test", progress2, TEST_METADATA_GRAPH)

    # Verify updated metadata
    query = f"""
    PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>

    SELECT ?lastId ?processed
    WHERE {{
        GRAPH <{TEST_METADATA_GRAPH}> {{
            <{settings.CARDANO_GRAPH}/etl/progress/update_test>
                cardano:hasLastProcessedId ?lastId ;
                cardano:hasProcessedRecords ?processed .
        }}
    }}
    """

    results = await virtuoso_client.execute_query(query)
    binding = results['results']['bindings'][0]
    assert int(binding['lastId']['value']) == 2000
    assert int(binding['processed']['value']) == 2000

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
async def test_loader_validate_data_integrity(virtuoso_client: VirtuosoClient):
    """Test data integrity validation."""
    loader = CDBLoader()

    # Load test data
    turtle_data = """
    @prefix test: <http://test.example.com#> .

    test:s1 test:p1 test:o1 .
    test:s2 test:p2 test:o2 .
    test:s3 test:p3 test:o3 .
    """

    await loader.load_batch(TEST_LOADER_GRAPH, turtle_data, {"entity_type": "test"})

    # Validate with correct count
    result = await loader.validate_data_integrity(TEST_LOADER_GRAPH, expected_count=3)
    assert result['valid']
    assert result['actual_count'] == 3

    # Validate with incorrect count
    result = await loader.validate_data_integrity(TEST_LOADER_GRAPH, expected_count=5)
    assert not result['valid']
    assert result['actual_count'] == 3

@pytest.mark.asyncio
async def test_loader_get_graph_statistics(virtuoso_client: VirtuosoClient):
    """Test getting graph statistics."""
    loader = CDBLoader()

    # Load test data
    turtle_data = """
    @prefix test: <http://test.example.com#> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

    test:person1 rdf:type test:Person ;
        test:name "Alice" ;
        test:age "30" .

    test:person2 rdf:type test:Person ;
        test:name "Bob" ;
        test:age "25" .
    """

    await loader.load_batch(TEST_LOADER_GRAPH, turtle_data, {"entity_type": "test"})

    # Get statistics
    stats = await loader.get_graph_statistics(TEST_LOADER_GRAPH)

    assert stats['graph_uri'] == TEST_LOADER_GRAPH
    assert stats['subjects'] == 2
    assert stats['predicates'] == 3  # rdf:type, test:name, test:age
    assert stats['objects'] == 5  # test:Person, "Alice", "Bob", "30", "25"
    assert stats['triples'] == 6

@pytest.mark.asyncio
async def test_loader_clear_graph_data(virtuoso_client: VirtuosoClient):
    """Test clearing graph data."""
    loader = CDBLoader()

    # Load test data
    turtle_data = """
    @prefix test: <http://test.example.com#> .
    test:data test:property "value" .
    """

    await loader.load_batch(TEST_LOADER_GRAPH, turtle_data, {"entity_type": "test"})

    # Verify data exists
    exists = await virtuoso_client.check_graph_exists(TEST_LOADER_GRAPH)
    assert exists

    # Clear graph
    success = await loader.clear_graph_data(TEST_LOADER_GRAPH)
    assert success

    # Verify graph is cleared
    exists = await virtuoso_client.check_graph_exists(TEST_LOADER_GRAPH)
    assert not exists

@pytest.mark.asyncio
async def test_loader_clear_nonexistent_graph():
    """Test clearing non-existent graph."""
    loader = CDBLoader()

    # Try to clear non-existent graph
    success = await loader.clear_graph_data("http://nonexistent.graph")
    assert not success