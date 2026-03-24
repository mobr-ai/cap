from dataclasses import dataclass
from typing import Literal, Optional


ReferLabel = Literal["refer", "not_refer"]
RenderFamily = Literal["chart", "table", "text"]
ChartSubtype = Literal[
    "line",
    "bar",
    "scatter",
    "bubble",
    "pie",
    "heatmap",
    "treemap",
]


@dataclass(frozen=True)
class ReferDecision:
    label: ReferLabel
    confidence: float
    reason: str = ""


@dataclass(frozen=True)
class RenderDecision:
    family: Optional[RenderFamily]
    confidence: float
    chart_subtype: Optional[str] = None
    matched_example: Optional[str] = None