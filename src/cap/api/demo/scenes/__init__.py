# cap/api/demo/scenes/__init__.py
from __future__ import annotations

from typing import Optional, Dict, Any

from .charts import CHART_SCENES
from .misc import MISC_SCENES

DEMO_SCENES: Dict[str, Dict[str, Any]] = {
    **CHART_SCENES,
    **MISC_SCENES,
}

def pick_scene(query: str) -> Optional[dict]:
    q = (query or "").strip().lower()
    for scene in DEMO_SCENES.values():
        if scene["match"] in q:
            return scene
    return None
