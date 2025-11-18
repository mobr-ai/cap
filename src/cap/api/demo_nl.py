import json
from typing import AsyncGenerator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/v1/demo", tags=["demo"])

class DemoQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000)

# --- Static demo artifacts ---------------------------------------------------
# Keep these minimal; they just need to exercise kv_results + charts/tables.

DEMO_SCENES = {
    # 1) Latest 5 blocks → table
    "latest_5_blocks": {
        "match": "list the latest 5 blocks",
        "kv": {
            "result_type": "table",
            "data": {
                "values": [
                    {
                        "col1": "blockNumber",
                        "values": ["5979789", "5979788", "5979787", "5979786", "5979785"],
                    },
                    {
                        "col2": "slotNumber",
                        "values": ["34724642", "34724623", "34724618", "34724610", "34724609"],
                    },
                    {
                        "col3": "epochNumber",
                        "values": ["80", "80", "80", "80", "80"],
                    },
                    {
                        "col4": "timestamp",
                        "values": [
                            "2021-07-14T19:28:53Z",
                            "2021-07-14T19:28:34Z",
                            "2021-07-14T19:28:29Z",
                            "2021-07-14T19:28:21Z",
                            "2021-07-14T19:28:20Z",
                        ],
                    },
                    {
                        "col5": "blockSize",
                        "values": ["2518", "1197", "573", "3", "18899"],
                    },
                    {
                        "col6": "hash",
                        "values": [
                            "7ebff2ab745f908ff0d06cea3830c1b1c6020045c4a751e1f223e9a091fd881b",
                            "7b607fdbc570eb0375d3928a945b92bf25fa7ff38cc14a4545c2086238431f47",
                            "565607225351bdf21376f0a4f9cbbce90ca953102778f96c69549a9f985053d4",
                            "490c064b83d37fbc648db61bac223fdad37f1831f01b90b49fda0359c0aad543",
                            "b223bdabc24fa85b94c33db9c6ac382f022ff06a40851da0ced5c1a430301854",
                        ],
                    },
                    {
                        "col7": "transactionCount",
                        "values": ["7", "1", "2", "0", "27"],
                    },
                ]
            },
            "metadata": {
                "count": 5,
                "columns": [
                    "blockNumber",
                    "slotNumber",
                    "epochNumber",
                    "timestamp",
                    "blockSize",
                    "hash",
                    "transactionCount",
                ],
            },
        },
        "text": (
            "The latest 5 blocks all belong to epoch 80 and show varying activity. "
            "Block 5979785 (27 transactions, 18.9KB) is the largest, while block 5979786 "
            "has 0 transactions, suggesting a system-only block. Most others have low tx counts."
        ),
    },

    # 2) Bar chart: monthly multi assets in 2021
    "multi_assets_2021_bar": {
        "match": "bar chart showing monthly multi assets created in 2021",
        "kv": {
            "result_type": "bar_chart",
            "data": {
                "values": [
                    {"category": "2021-03", "amount": 11370.0},
                    {"category": "2021-04", "amount": 94539.0},
                    {"category": "2021-05", "amount": 123611.0},
                    {"category": "2021-06", "amount": 49138.0},
                ]
            },
            "metadata": {
                "count": 4,
                "columns": ["yearMonth", "deployments"],
            },
        },
        "text": (
            "The bar chart shows a sharp rise in new multi-asset deployments peaking in May 2021 "
            "(123,611), followed by a strong drop in June. This reflects the intense burst of "
            "adoption around the Alonzo-era native asset capabilities before stabilization."
        ),
    },

    # 3) Pie chart: top 1% ADA holders share
    "top1_percent_pie": {
        "match": "pie chart to show how much the top 1% ada holders represent",
        "kv": {
            "result_type": "pie_chart",
            "data": {
                "values": [
                    {"category": "topHolders", "value": 56.9259},
                    {"category": "otherHolders", "value": 43.0741},
                ]
            },
            "metadata": {
                "count": 0,
                "columns": [],
            },
        },
        "text": (
            "The pie chart indicates that the top 1% of ADA holders control about 56.93% of the "
            "total supply, while the remaining 43.07% is held by all other addresses, showing a "
            "high concentration of holdings among a small set of accounts."
        ),
    },

    # 4) Line chart: monthly txs & outputs
    "tx_outputs_line": {
        "match": "line chart showing monthly number of transactions and outputs",
        "kv": {
            "result_type": "line_chart",
            "data": {
                "values": [
                    # (keeping your exact payload)
                    {"x": "2017-09", "y": 5071.0, "c": 0},
                    {"x": "2017-09", "y": 6465.0, "c": 1},
                    {"x": "2017-10", "y": 25599.0, "c": 0},
                    {"x": "2017-10", "y": 44255.0, "c": 1},
                    {"x": "2017-11", "y": 20041.0, "c": 0},
                    {"x": "2017-11", "y": 38873.0, "c": 1},
                    {"x": "2017-12", "y": 145536.0, "c": 0},
                    {"x": "2017-12", "y": 288657.0, "c": 1},
                    {"x": "2018-01", "y": 116508.0, "c": 0},
                    {"x": "2018-01", "y": 246615.0, "c": 1},
                    {"x": "2018-02", "y": 67663.0, "c": 0},
                    {"x": "2018-02", "y": 165597.0, "c": 1},
                    {"x": "2018-03", "y": 78380.0, "c": 0},
                    {"x": "2018-03", "y": 183756.0, "c": 1},
                    {"x": "2018-04", "y": 78243.0, "c": 0},
                    {"x": "2018-04", "y": 183385.0, "c": 1},
                    {"x": "2018-05", "y": 62922.0, "c": 0},
                    {"x": "2018-05", "y": 148707.0, "c": 1},
                    {"x": "2018-06", "y": 43838.0, "c": 0},
                    {"x": "2018-06", "y": 100913.0, "c": 1},
                    {"x": "2018-07", "y": 44536.0, "c": 0},
                    {"x": "2018-07", "y": 100381.0, "c": 1},
                    {"x": "2018-08", "y": 38428.0, "c": 0},
                    {"x": "2018-08", "y": 88388.0, "c": 1},
                    {"x": "2018-09", "y": 43476.0, "c": 0},
                    {"x": "2018-09", "y": 101993.0, "c": 1},
                    {"x": "2018-10", "y": 38528.0, "c": 0},
                    {"x": "2018-10", "y": 88483.0, "c": 1},
                    {"x": "2018-11", "y": 35955.0, "c": 0},
                    {"x": "2018-11", "y": 83833.0, "c": 1},
                    {"x": "2018-12", "y": 47002.0, "c": 0},
                    {"x": "2018-12", "y": 108255.0, "c": 1},
                    {"x": "2019-01", "y": 49173.0, "c": 0},
                    {"x": "2019-01", "y": 109823.0, "c": 1},
                    {"x": "2019-02", "y": 39680.0, "c": 0},
                    {"x": "2019-02", "y": 90224.0, "c": 1},
                    {"x": "2019-03", "y": 71119.0, "c": 0},
                    {"x": "2019-03", "y": 183265.0, "c": 1},
                    {"x": "2019-04", "y": 82981.0, "c": 0},
                    {"x": "2019-04", "y": 207822.0, "c": 1},
                    {"x": "2019-05", "y": 102160.0, "c": 0},
                    {"x": "2019-05", "y": 248418.0, "c": 1},
                    {"x": "2019-06", "y": 99432.0, "c": 0},
                    {"x": "2019-06", "y": 252409.0, "c": 1},
                    {"x": "2019-07", "y": 81275.0, "c": 0},
                    {"x": "2019-07", "y": 185170.0, "c": 1},
                    {"x": "2019-08", "y": 60474.0, "c": 0},
                    {"x": "2019-08", "y": 138715.0, "c": 1},
                    {"x": "2019-09", "y": 50620.0, "c": 0},
                    {"x": "2019-09", "y": 119971.0, "c": 1},
                    {"x": "2019-10", "y": 51736.0, "c": 0},
                    {"x": "2019-10", "y": 112742.0, "c": 1},
                    {"x": "2019-11", "y": 65652.0, "c": 0},
                    {"x": "2019-11", "y": 151067.0, "c": 1},
                    {"x": "2019-12", "y": 43199.0, "c": 0},
                    {"x": "2019-12", "y": 91939.0, "c": 1},
                    {"x": "2020-01", "y": 56101.0, "c": 0},
                    {"x": "2020-01", "y": 125170.0, "c": 1},
                    {"x": "2020-02", "y": 89682.0, "c": 0},
                    {"x": "2020-02", "y": 206725.0, "c": 1},
                    {"x": "2020-03", "y": 91968.0, "c": 0},
                    {"x": "2020-03", "y": 219591.0, "c": 1},
                    {"x": "2020-04", "y": 80766.0, "c": 0},
                    {"x": "2020-04", "y": 245499.0, "c": 1},
                    {"x": "2020-05", "y": 112588.0, "c": 0},
                    {"x": "2020-05", "y": 356756.0, "c": 1},
                    {"x": "2020-06", "y": 128583.0, "c": 0},
                    {"x": "2020-06", "y": 404635.0, "c": 1},
                    {"x": "2020-07", "y": 183817.0, "c": 0},
                    {"x": "2020-07", "y": 577813.0, "c": 1},
                    {"x": "2020-08", "y": 196480.0, "c": 0},
                    {"x": "2020-08", "y": 410480.0, "c": 1},
                    {"x": "2020-09", "y": 142330.0, "c": 0},
                    {"x": "2020-09", "y": 284790.0, "c": 1},
                    {"x": "2020-10", "y": 111464.0, "c": 0},
                    {"x": "2020-10", "y": 219460.0, "c": 1},
                    {"x": "2020-11", "y": 196231.0, "c": 0},
                    {"x": "2020-11", "y": 384403.0, "c": 1},
                    {"x": "2020-12", "y": 234838.0, "c": 0},
                    {"x": "2020-12", "y": 508828.0, "c": 1},
                    {"x": "2021-01", "y": 418899.0, "c": 0},
                    {"x": "2021-01", "y": 1055117.0, "c": 1},
                    {"x": "2021-02", "y": 892566.0, "c": 0},
                    {"x": "2021-02", "y": 2198616.0, "c": 1},
                    {"x": "2021-03", "y": 981809.0, "c": 0},
                    {"x": "2021-03", "y": 2471629.0, "c": 1},
                    {"x": "2021-04", "y": 1147245.0, "c": 0},
                    {"x": "2021-04", "y": 2715161.0, "c": 1},
                    {"x": "2021-05", "y": 1616505.0, "c": 0},
                    {"x": "2021-05", "y": 3780275.0, "c": 1},
                    {"x": "2021-06", "y": 720326.0, "c": 0},
                    {"x": "2021-06", "y": 1560860.0, "c": 1},
                ]
            },
            "metadata": {
                "count": 46,
                "columns": ["yearMonth", "txCount", "outputCount"],
            },
        },
        "text": (
            "The line chart shows strong growth in both monthly transactions and outputs from "
            "2017 through mid-2021, with a steep surge in 2020–2021. Outputs consistently "
            "outpace transactions, reflecting richer transaction structures and growing "
            "on-chain activity on Cardano."
        ),
    },
}


def _pick_scene(query: str):
    q = query.lower().strip()
    for scene in DEMO_SCENES.values():
        if scene["match"] in q:
            return scene
    return None


async def _demo_stream(scene) -> AsyncGenerator[str, None]:
    # Minimal SSE-compatible stream; matches useLLMStream expectations.
    yield "status: Processing your query\n"
    yield "status: Fetching contextual data from knowledge graph\n"
    yield "status: Analyzing context and preparing answer\n"

    kv_json = json.dumps(scene["kv"])
    yield f"kv_results:{kv_json}\n"
    yield "_kv_results_end_\n"

    # Plain markdown explanation
    yield scene["text"] + "\n"
    yield "data: [DONE]\n"


@router.post("/query")
async def demo_query(req: DemoQueryRequest):
    scene = _pick_scene(req.query)
    if not scene:
        # For unmatched queries, either 400 or trivial answer.
        raise HTTPException(
            status_code=400,
            detail="Demo endpoint only supports the predefined showcase queries.",
        )

    return StreamingResponse(
        _demo_stream(scene),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
