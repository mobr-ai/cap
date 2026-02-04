# cap/api/demo/schemas.py
from typing import Optional, Literal
from pydantic import BaseModel, Field

NL_TOKEN = "__NL__"
DONE_SSE = "data: [DONE]"
BreakSSEMode = Optional[Literal["concat_payload", "concat_raw"]]

class DemoQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000)
    conversation_id: Optional[int] = None

    # Optional override for SSE regression tests.
    # If not provided, can be driven by DEMO_SCENES entries.
    break_sse_mode: BreakSSEMode = None

    # Artificial delay for UI testing
    delay_ms: Optional[int] = Field(
        None,
        ge=0,
        le=5000,
        description="Artificial per-step delay (ms) for demo streaming UX tests",
    )
