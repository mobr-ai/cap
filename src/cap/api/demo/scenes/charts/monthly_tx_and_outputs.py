SCENE = {
    "match": "monthly number of transactions and outputs",
    "kv": {
        "result_type": "line_chart",
        "metadata": {"columns": ["yearMonth", "txCount", "outputCount"]},
        "data": {
            "values": [
                {"yearMonth": "2021-01", "txCount": 1100000, "outputCount": 3100000},
                {"yearMonth": "2021-02", "txCount": 1200000, "outputCount": 3300000},
                {"yearMonth": "2021-03", "txCount": 1350000, "outputCount": 3650000},
                {"yearMonth": "2021-04", "txCount": 1500000, "outputCount": 4000000},
                {"yearMonth": "2021-05", "txCount": 1420000, "outputCount": 3920000},
                {"yearMonth": "2021-06", "txCount": 1600000, "outputCount": 4300000},
                {"yearMonth": "2021-07", "txCount": 1750000, "outputCount": 4700000},
                {"yearMonth": "2021-08", "txCount": 1680000, "outputCount": 4550000},
                {"yearMonth": "2021-09", "txCount": 1550000, "outputCount": 4200000},
                {"yearMonth": "2021-10", "txCount": 1620000, "outputCount": 4380000},
                {"yearMonth": "2021-11", "txCount": 1580000, "outputCount": 4320000},
                {"yearMonth": "2021-12", "txCount": 1700000, "outputCount": 4600000},
            ]
        },
    },
    "assistant_text": (
        "This line chart shows a demo monthly series for transactions and outputs. "
        "In production, these would be computed from transaction bodies and UTxO outputs per period."
    ),
}
