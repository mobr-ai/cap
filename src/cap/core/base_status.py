import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict


def _get_sync_status() -> Dict[str, Any]:
    file_path = os.getenv("CAP_SYNC_STATUS_FILE", "/app/status.json")

    if not file_path:
        return dict()

    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def is_syncing(status: Dict[str, Any] = None) -> bool:
    if not status:
        status: Dict[str, Any] = _get_sync_status()

    if not status or status.get("status") == "running" or not status.get("timestamp", 0):
        return False

    return True


def get_sync_time_remaining() -> timedelta:
    status: Dict[str, Any] = _get_sync_status()
    if not is_syncing(status=status):
        return timedelta(0)

    timestamp = status.get("timestamp")

    # Parse ISO8601 UTC timestamp ending with 'Z'
    started_at = datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )

    expected_finish = started_at + timedelta(hours=3)
    remaining = expected_finish - datetime.now(timezone.utc)

    return max(remaining, timedelta(0))
