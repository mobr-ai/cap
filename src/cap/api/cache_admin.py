"""
Cache administration endpoints for pre-caching and management.
"""
import logging
from typing import Optional
from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel, Field
from opentelemetry import trace
import tempfile
import os
import json

from cap.services.redis_nl_client import get_redis_client

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

router = APIRouter(prefix="/api/v1/admin/cache", tags=["cache"])


class PrecacheRequest(BaseModel):
    """Request model for pre-caching from file path."""
    file_path: str = Field(..., description="Path to the query mappings file")
    ttl: Optional[int] = Field(None, description="TTL in seconds (optional)")


class PrecacheStats(BaseModel):
    """Statistics from pre-caching operation."""
    total_queries: int
    cached_successfully: int
    failed: int
    skipped_duplicates: int
    errors: list[str]


@router.post("/precache/file", response_model=PrecacheStats)
async def precache_from_file_path(request: PrecacheRequest):
    """
    Pre-cache natural language queries from a file path.
    Args:
        request: Pre-cache request with file path and optional TTL

    Returns:
        Statistics about the pre-caching operation
    """
    with tracer.start_as_current_span("precache_file_path") as span:
        span.set_attribute("file_path", request.file_path)

        try:
            # Validate file exists
            if not os.path.exists(request.file_path):
                raise HTTPException(
                    status_code=404,
                    detail=f"File not found: {request.file_path}"
                )

            # Validate file is readable
            if not os.access(request.file_path, os.R_OK):
                raise HTTPException(
                    status_code=403,
                    detail=f"File not readable: {request.file_path}"
                )

            redis_client = get_redis_nl_client()
            stats = await redis_client.precache_from_file(
                file_path=request.file_path,
                ttl=request.ttl
            )

            span.set_attribute("cached_successfully", stats["cached_successfully"])

            return PrecacheStats(**stats)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Pre-caching error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/precache/upload", response_model=PrecacheStats)
async def precache_from_upload(
    file: UploadFile = File(..., description="Query mappings file"),
    ttl: Optional[int] = None
):
    """
    Pre-cache natural language queries from an uploaded file.
    Args:
        file: Uploaded file with query mappings
        ttl: Optional TTL in seconds

    Returns:
        Statistics about the pre-caching operation
    """
    with tracer.start_as_current_span("precache_upload") as span:
        span.set_attribute("filename", file.filename)

        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.txt',
                delete=False,
                encoding='utf-8'
            ) as tmp_file:
                # Read and write content
                content = await file.read()
                tmp_file.write(content.decode('utf-8'))
                tmp_path = tmp_file.name

            try:
                redis_client = get_redis_nl_client()
                stats = await redis_client.precache_from_file(
                    file_path=tmp_path,
                    ttl=ttl
                )

                span.set_attribute("cached_successfully", stats["cached_successfully"])

                return PrecacheStats(**stats)

            finally:
                # Clean up temporary file
                try:
                    os.unlink(tmp_path)
                except Exception as e:
                    logger.warning(f"Failed to delete temp file {tmp_path}: {e}")

        except Exception as e:
            logger.error(f"Upload pre-caching error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


@router.delete("/clear")
async def clear_cache():
    """
    Clear all cached queries (use with caution).

    Returns:
        Success message
    """
    with tracer.start_as_current_span("clear_cache") as span:
        try:
            redis_client = get_redis_nl_client()
            client = await redis_client._get_nl_client()

            # Count keys before deletion
            cache_keys = []
            count_keys = []

            async for key in client.scan_iter(match="nlq:cache:*"):
                cache_keys.append(key)

            async for key in client.scan_iter(match="nlq:count:*"):
                count_keys.append(key)

            # Delete all keys
            if cache_keys:
                await client.delete(*cache_keys)
            if count_keys:
                await client.delete(*count_keys)

            total_deleted = len(cache_keys) + len(count_keys)

            span.set_attribute("keys_deleted", total_deleted)
            logger.info(f"Cleared {total_deleted} cache keys")

            return {
                "message": "Cache cleared successfully",
                "cache_entries_deleted": len(cache_keys),
                "count_entries_deleted": len(count_keys),
                "total_deleted": total_deleted
            }

        except Exception as e:
            logger.error(f"Cache clear error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/info")
async def get_cache_info():
    """
    Get information about cached queries.

    Returns:
        Cache info
    """
    with tracer.start_as_current_span("cache_info") as span:
        try:
            redis_client = get_redis_nl_client()
            client = await redis_client._get_nl_client()

            # Count cache entries
            cache_count = 0
            precached_count = 0

            async for key in client.scan_iter(match="nlq:cache:*"):
                cache_count += 1
                # Check if pre-cached
                data = await client.get(key)
                if data:
                    cache_data = json.loads(data)
                    if cache_data.get("precached"):
                        precached_count += 1

            # Get popular queries
            popular_queries = await redis_client.get_popular_queries(limit=0)

            span.set_attribute("cache_count", cache_count)
            span.set_attribute("precached_count", precached_count)
            return {
                "total_cached_queries": cache_count,
                "precached_queries": precached_count,
                "dynamic_cached_queries": cache_count - precached_count,
                "popular_queries": [
                    {
                        "rank": idx + 1,
                        "query": query["original_query"],
                        "normalized_query": query["normalized_query"],
                        "frequency": query["count"]
                    }
                    for idx, query in enumerate(popular_queries)
                ]
            }

        except Exception as e:
            logger.error(f"Cache info error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/info/nl")
async def get_cache_info():
    """
    Get all cached natural language queries.

    Returns:
        Cached queries
    """
    with tracer.start_as_current_span("cache_info_nl") as span:
        try:
            redis_client = get_redis_nl_client()

            # Get popular queries
            popular_queries = await redis_client.get_popular_queries(limit=0)

            return {
                "nl_queries": [query["original_query"]
                    for _, query in enumerate(popular_queries)
                ]
            }

        except Exception as e:
            logger.error(f"Cache nl error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))