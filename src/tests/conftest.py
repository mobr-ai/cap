# src/tests/conftest.py
import pytest
from httpx import AsyncClient
from typing import AsyncGenerator
import logging
from cap.config import settings
from cap.rdf.triplestore import TriplestoreClient

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def virtuoso_client():
    return TriplestoreClient()

@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    base_url = f"http://{settings.CAP_HOST}:{settings.CAP_PORT}"
    logger.debug(f"Creating async client with base_url: {base_url}")

    async with AsyncClient(
        base_url=base_url,
        follow_redirects=True,
        timeout=300.0
    ) as client:
        yield client