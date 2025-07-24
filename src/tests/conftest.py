# src/tests/conftest.py
import pytest
from httpx import AsyncClient
from typing import AsyncGenerator
import logging
from cap.main import app
from cap.data.virtuoso import VirtuosoClient

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def virtuoso_client():
    return VirtuosoClient()

@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    base_url = "http://localhost:8000"
    logger.debug(f"Creating async client with base_url: {base_url}")
    
    async with AsyncClient(
        base_url=base_url,
        follow_redirects=True,
        timeout=30.0
    ) as client:
        yield client