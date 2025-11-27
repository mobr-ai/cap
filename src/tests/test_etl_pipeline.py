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
async def test_etl_progress_tracking(virtuoso_client: TriplestoreClient):
    """Test ETL progress tracking and persistence."""
    pipeline = ETLPipeline(batch_size=100)

    # Update progress for an entity
    test_entity = 'account'
    pipeline.progress[test_entity].last_processed_id = 1000
    pipeline.progress[test_entity].total_records = 5000
    pipeline.progress[test_entity].processed_records = 1000
    pipeline.progress[test_entity].status = ETLStatus.RUNNING
    pipeline.progress[test_entity].last_updated = datetime.now()

    # Save progress
    await pipeline.loader.save_progress_metadata(
        test_entity,
        pipeline.progress[test_entity],
        TEST_METADATA_GRAPH
    )

    # Verify progress was saved
    query = f"""
    PREFIX c: <https://mobr.ai/ont/cardano#>

    SELECT ?lastId ?totalRecords ?processedRecords ?status
    WHERE {{
        GRAPH <{TEST_METADATA_GRAPH}> {{
            <{settings.CARDANO_GRAPH}/etl/progress/{test_entity}>
                c:hasLastProcessedId ?lastId ;
                c:hasTotalRecords ?totalRecords ;
                c:hasProcessedRecords ?processedRecords ;
                c:hasStatus ?status .
        }}
    }}
    """

    results = await virtuoso_client.execute_query(query)
    assert results['results']['bindings']

    binding = results['results']['bindings'][0]
    assert int(binding['lastId']['value']) == 1000
    assert int(binding['totalRecords']['value']) == 5000
    assert int(binding['processedRecords']['value']) == 1000
    assert binding['status']['value'] == 'running'

@pytest.mark.asyncio
async def test_etl_load_existing_progress(virtuoso_client: TriplestoreClient):
    """Test loading existing ETL progress from metadata."""
    # First save some progress
    pipeline1 = ETLPipeline(batch_size=100)

    test_entity = 'account'
    pipeline1.progress[test_entity].last_processed_id = 2000
    pipeline1.progress[test_entity].total_records = 10000
    pipeline1.progress[test_entity].processed_records = 2000
    pipeline1.progress[test_entity].status = ETLStatus.COMPLETED
    pipeline1.progress[test_entity].last_updated = datetime.now()

    await pipeline1.loader.save_progress_metadata(
        test_entity,
        pipeline1.progress[test_entity],
        TEST_METADATA_GRAPH
    )

    # Create new pipeline and load progress
    pipeline2 = ETLPipeline(batch_size=100)
    await pipeline2._load_existing_progress(TEST_METADATA_GRAPH)

    # Verify loaded progress
    loaded_progress = pipeline2.progress[test_entity]
    assert loaded_progress.last_processed_id == 2000
    assert loaded_progress.total_records == 10000
    assert loaded_progress.processed_records == 2000
    assert loaded_progress.status == ETLStatus.COMPLETED

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
async def test_etl_batch_loading(virtuoso_client: TriplestoreClient):
    """Test loading data batches to Virtuoso."""
    pipeline = ETLPipeline(batch_size=100)

    # Create test RDF data
    turtle_data = """
    @prefix b: <https://mobr.ai/ont/blockchain#> .
    @prefix c: <https://mobr.ai/ont/cardano#> .
    <http://test/account/1> a b:Account .

    <http://test/account/2> a b:Account .
    """

    batch_info = {
        "entity_type": "account",
        "size": 2,
        "batch_number": 1
    }

    # Load batch
    success = await pipeline.loader.load_batch(
        TEST_ETL_GRAPH,
        turtle_data,
        batch_info
    )
    assert success

    # Verify data was loaded
    count_query = f"""
    PREFIX b: <https://mobr.ai/ont/blockchain#>
    SELECT (COUNT(*) as ?count)
    WHERE {{
        GRAPH <{TEST_ETL_GRAPH}> {{
            ?s a b:Account
        }}
    }}
    """

    results = await virtuoso_client.execute_query(count_query)
    count = int(results['results']['bindings'][0]['count']['value'])
    assert count == 2

@pytest.mark.asyncio
async def test_etl_data_integrity_validation(virtuoso_client: TriplestoreClient):
    """Test data integrity validation."""
    pipeline = ETLPipeline(batch_size=100)

    # Load some test data
    turtle_data = """
    @prefix b: <https://mobr.ai/ont/blockchain#> .
    @prefix c: <https://mobr.ai/ont/cardano#> .
    <http://test/account/1> a b:Account .

    <http://test/account/2> a b:Account .
    """

    await pipeline.loader.load_batch(TEST_ETL_GRAPH, turtle_data, {"entity_type": "account", "size": 2})

    # Validate integrity
    validation_result = await pipeline.loader.validate_data_integrity(TEST_ETL_GRAPH, expected_count=4)

    assert validation_result['graph_uri'] == TEST_ETL_GRAPH
    assert validation_result['actual_count'] == 4
    assert validation_result['expected_count'] == 4
    assert validation_result['valid']

@pytest.mark.asyncio
async def test_etl_graph_statistics(virtuoso_client: TriplestoreClient):
    """Test getting graph statistics."""
    pipeline = ETLPipeline(batch_size=100)

    # Load test data
    turtle_data = """
    @prefix b: <https://mobr.ai/ont/blockchain#> .
    @prefix c: <https://mobr.ai/ont/cardano#> .

    <http://test/tx/1> a b:Transaction ;
        c:hasFee "1000000" ;
        b:hasHash "xyz789" .
    """

    await pipeline.loader.load_batch(TEST_ETL_GRAPH, turtle_data, {"entity_type": "transaction", "size": 1})

    # Get statistics
    stats = await pipeline.loader.get_graph_statistics(TEST_ETL_GRAPH)

    assert stats['graph_uri'] == TEST_ETL_GRAPH
    assert stats['subjects'] == 1
    assert stats['predicates'] == 3
    assert stats['objects'] == 3
    assert stats['triples'] == 3

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

@pytest.mark.asyncio
async def test_etl_clear_graph_data(virtuoso_client: TriplestoreClient):
    """Test clearing graph data."""
    pipeline = ETLPipeline(batch_size=100)

    # First create and load data
    turtle_data = """
    @prefix b: <https://mobr.ai/ont/blockchain#> .
    @prefix c: <https://mobr.ai/ont/cardano#> .

    <http://test/epoch/1> a c:Epoch ;
        c:hasEpochNumber "1" .
    """

    await virtuoso_client.create_graph(TEST_ETL_GRAPH, turtle_data)
    exists = await virtuoso_client.check_graph_exists(TEST_ETL_GRAPH)
    assert exists

    # Clear the graph
    success = await pipeline.loader.clear_graph_data(TEST_ETL_GRAPH)
    assert success

    # Verify graph is empty
    exists = await virtuoso_client.check_graph_exists(TEST_ETL_GRAPH)
    assert not exists