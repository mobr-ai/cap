"""
ETL endpoints for ETL management.
"""
from fastapi import APIRouter, HTTPException
from opentelemetry import trace
import logging

from cap.etl.cdb.service import etl_service

router = APIRouter(prefix="/api/v1/admin/etl", tags=["etl"])
tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)

@router.get("status")
async def get_etl_status():
    """Get ETL pipeline status."""
    try:
        return await etl_service.get_status()
    except Exception as e:
        logger.error(f"Error getting ETL status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("start")
async def start_etl(
    batch_size: int = 1000,
    sync_interval: int = 300,
    continuous: bool = True
):
    """Manually start ETL pipeline."""
    try:
        if etl_service.pipeline and etl_service.pipeline.running:
            return {"message": "ETL pipeline is running already"}

        await etl_service.start_etl(
            batch_size=batch_size,
            sync_interval=sync_interval,
            continuous=continuous
        )
        return {"message": "ETL pipeline started successfully"}
    except RuntimeError as e:
        logger.error(f"ETL start error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected ETL start error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("stop")
async def stop_etl():
    """Stop ETL pipeline."""
    try:
        await etl_service.stop_etl()
        return {"message": "ETL pipeline stopped"}
    except Exception as e:
        logger.error(f"ETL stop error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("reset")
async def reset_etl(entity_types: list[str] = None):
    """Reset ETL progress for specified entity types."""
    try:
        await etl_service.reset_progress(entity_types)
        return {"message": f"ETL progress reset for: {entity_types or 'all entities'}"}
    except RuntimeError as e:
        logger.error(f"ETL reset error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected ETL reset error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
