import pytest
import asyncio

from httpx import AsyncClient

from cap.etl.cdb.service import ETLService

@pytest.mark.asyncio
async def test_etl_service_initialization():
    """Test ETL service initialization."""
    service = ETLService()

    assert service.pipeline is None
    assert service.sync_task is None

@pytest.mark.asyncio
async def test_etl_service_start_stop():
    """Test starting and stopping ETL service."""
    service = ETLService()

    # Start service with non-continuous mode for testing
    await service.start_etl(batch_size=10, sync_interval=1, continuous=False)

    assert service.pipeline is not None
    assert service.pipeline.batch_size == 10
    assert service.pipeline.sync_interval == 1

    # Give it a moment to start
    await asyncio.sleep(0.1)

    # Stop service
    await service.stop_etl()

    assert service.pipeline is None
    assert service.sync_task is None

@pytest.mark.asyncio
async def test_etl_service_status():
    """Test getting ETL service status."""
    service = ETLService()

    # Status when not initialized
    status = await service.get_status()
    assert not status['running']
    assert 'message' in status

    # Start service
    await service.start_etl(batch_size=10, sync_interval=1, continuous=False)

    # Get status
    status = await service.get_status()
    assert 'entity_progress' in status

    # Stop service
    await service.stop_etl()

@pytest.mark.asyncio
async def test_etl_api_endpoints(async_client: AsyncClient):
    """Test ETL API endpoints."""
    # Get initial status
    response = await async_client.get("/api/v1/admin/etl/status")
    assert response.status_code == 200
    status = response.json()
    assert 'running' in status

    # Start ETL
    response = await async_client.post(
        "/api/v1/admin/etl/start",
        params={"batch_size": 10, "sync_interval": 1, "continuous": False}
    )
    assert response.status_code == 200
    assert ("running already" in response.json()["message"] or "started successfully" in response.json()["message"])

    # Get status while running
    response = await async_client.get("/api/v1/admin/etl/status")
    assert response.status_code == 200

    # Stop ETL
    response = await async_client.post("/api/v1/admin/etl/stop")
    assert response.status_code == 200
    assert "stopped" in response.json()["message"]