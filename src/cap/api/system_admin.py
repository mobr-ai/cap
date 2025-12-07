import os
import subprocess
from typing import Optional, List, Dict, Any

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
    root_disk = psutil.disk_usage("/")
    root_disk_total = root_disk.total
    root_disk_used = root_disk.used
    root_disk_percent = root_disk.percent

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
        # Backwards-compatible root disk summary
        "disk": {
            "total": root_disk_total,
            "used": root_disk_used,
            "percent": root_disk_percent,
            "mount": "/",
        },
        # New: detailed disks/partitions list
        "disks": get_disks_info(),
        "load_avg": {
            "1m": load_avg_1,
            "5m": load_avg_5,
            "15m": load_avg_15,
        },
        "gpu": get_gpu_info(),
    }


def get_disks_info() -> list[dict[str, Any]]:
    """
    Return usage info for all mounted partitions.

    Each entry has:
      - device
      - mountpoint
      - fstype
      - opts
      - total, used, percent
    """
    if psutil is None:
        return []

    disks: list[dict[str, Any]] = []

    try:
        partitions = psutil.disk_partitions(all=False)
    except Exception:
        return disks

    for part in partitions:
        try:
            usage = psutil.disk_usage(part.mountpoint)
        except (PermissionError, FileNotFoundError, OSError):
            # Skip mounts we can't read
            continue

        disks.append(
            {
                "device": part.device,
                "mountpoint": part.mountpoint,
                "fstype": part.fstype,
                "opts": part.opts,
                "total": usage.total,
                "used": usage.used,
                "percent": usage.percent,
            }
        )

    return disks


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
            "--format=csv,noheader,nounits",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1)

        if result.returncode != 0:
            return None

        gpus = []
        for line in result.stdout.strip().split("\n"):
            idx, name, driver, mem_total, mem_used, util = [
                x.strip() for x in line.split(",")
            ]

            mem_total_int = int(mem_total)
            mem_used_int = int(mem_used)

            gpus.append(
                {
                    "index": int(idx),
                    "name": name,
                    "driver": driver,
                    "memory_total": mem_total_int,
                    "memory_used": mem_used_int,
                    "memory_percent": round(
                        (mem_used_int / mem_total_int) * 100, 1
                    ),
                    "utilization": int(util),
                }
            )

        return gpus

    except FileNotFoundError:
        return None  # nvidia-smi not installed
    except Exception:
        return None
