SCENE = {
    "match": "monthly multi assets created in 2021",
    "kv": {
        "result_type": "bar_chart",
        "metadata": {"columns": ["yearMonth", "deployments"]},
        "data": {
            "values": [
                {"yearMonth": "2021-01", "deployments": 120},
                {"yearMonth": "2021-02", "deployments": 220},
                {"yearMonth": "2021-03", "deployments": 540},
                {"yearMonth": "2021-04", "deployments": 610},
                {"yearMonth": "2021-05", "deployments": 430},
                {"yearMonth": "2021-06", "deployments": 720},
                {"yearMonth": "2021-07", "deployments": 980},
                {"yearMonth": "2021-08", "deployments": 860},
                {"yearMonth": "2021-09", "deployments": 650},
                {"yearMonth": "2021-10", "deployments": 770},
                {"yearMonth": "2021-11", "deployments": 690},
                {"yearMonth": "2021-12", "deployments": 910},
            ]
        },
    },
    "assistant_text": (
        "This bar chart shows a demo monthly count of native assets created in 2021. "
        "In production, this would be computed from minting policies and asset creation events."
    ),
}
