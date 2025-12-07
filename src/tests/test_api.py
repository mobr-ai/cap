# src/tests/test_api.py
import pytest
import logging

from httpx import AsyncClient
from urllib.parse import quote_plus
from cap.rdf.triplestore import TriplestoreClient

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

TEST_GRAPH = "https://mobr.ai/ont/cardano/test"
TEST_DATA = """
PREFIX c: <https://mobr.ai/ont/cardano#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
c:TestBlock rdf:type c:Block .
c:TestBlock c:status c:Pending .
"""

@pytest.fixture(autouse=True)
async def cleanup(virtuoso_client: TriplestoreClient):
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
