# cap/src/cap/api/system_admin.py
import os
import subprocess
import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import User
from cap.core.auth_dependencies import get_current_admin_user

try:
    import psutil  # type: ignore
except ImportError:  # pragma: no cover
    psutil = None

router = APIRouter(prefix="/api/v1/admin/system", tags=["system_admin"])


@router.get("/metrics")
def get_system_metrics(
    db: Session = Depends(get_db),  # kept for future use (e.g. DB stats)
    admin: User = Depends(get_current_admin_user),
):
    """
    Basic system metrics snapshot.

    Only accessible to admins.
    """
    if psutil is None:
        raise HTTPException(
            status_code=503,
            detail="psutil_not_installed",
        )

    # CPU %
    cpu_percent = psutil.cpu_percent(interval=0.1)

    # Threads / cores
    cpu_threads = psutil.cpu_count(logical=True) or 0
    cpu_cores = psutil.cpu_count(logical=False) or cpu_threads

    # Memory
    vm = psutil.virtual_memory()
    mem_total = vm.total
    mem_used = vm.used
    mem_percent = vm.percent

    # Disk (root filesystem)
    disk = psutil.disk_usage("/")
    disk_total = disk.total
    disk_used = disk.used
    disk_percent = disk.percent

    # Load average (if available)
    load_avg_1 = load_avg_5 = load_avg_15 = None
    try:
        load1, load5, load15 = os.getloadavg()
        load_avg_1, load_avg_5, load_avg_15 = load1, load5, load15
    except (OSError, AttributeError):
        pass

    return {
        "cpu": {
            "percent": cpu_percent,
            "threads": cpu_threads,
            "cores": cpu_cores,
        },
        "memory": {
            "total": mem_total,
            "used": mem_used,
            "percent": mem_percent,
        },
        "disk": {
            "total": disk_total,
            "used": disk_used,
            "percent": disk_percent,
            "mount": "/",
        },
        "load_avg": {
            "1m": load_avg_1,
            "5m": load_avg_5,
            "15m": load_avg_15,
        },
        "gpu": get_gpu_info(),
    }

def get_gpu_info():
    """
    Returns GPU stats if available. Supports NVIDIA GPUs via nvidia-smi.
    Returns None if no supported GPU is present.
    """
    try:
        # Query GPU info in a parseable format
        cmd = [
            "nvidia-smi",
            "--query-gpu=index,name,driver_version,memory.total,memory.used,utilization.gpu",
            "--format=csv,noheader,nounits"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1)

        if result.returncode != 0:
            return None

        gpus = []
        for line in result.stdout.strip().split("\n"):
            idx, name, driver, mem_total, mem_used, util = [x.strip() for x in line.split(",")]

            gpus.append({
                "index": int(idx),
                "name": name,
                "driver": driver,
                "memory_total": int(mem_total),
                "memory_used": int(mem_used),
                "memory_percent": round((int(mem_used) / int(mem_total)) * 100, 1),
                "utilization": int(util),
            })

        return gpus

    except FileNotFoundError:
        return None   # nvidia-smi not installed
    except Exception:
        return None
