SCENE = {
    "match": "transaction fee vs transaction value",
    "kv": {
        "result_type": "scatter_chart",
        "metadata": {"columns": ["txValue", "fee"]},
        "data": {
            "values": [
                {"txValue": 6154.057738, "fee": 0.169857},
                {"txValue": 18.745449, "fee": 0.186671},
                {"txValue": 43.552177, "fee": 0.179449},
                {"txValue": 225.302762, "fee": 0.261042},
                {"txValue": 107132.967442, "fee": 0.185345},
                {"txValue": 57426.384985, "fee": 0.187853},
                {"txValue": 1395.561435, "fee": 0.171793},
                {"txValue": 38163.236689, "fee": 0.171177},
                {"txValue": 5569568.454729, "fee": 0.772811},
                {"txValue": 5.986541, "fee": 0.172849},
                {"txValue": 165514.041794, "fee": 0.168537},
                {"txValue": 1636794.642518, "fee": 0.606547},
                {"txValue": 4585430.881029, "fee": 0.664046},
                {"txValue": 676612.473896, "fee": 0.667823},
                {"txValue": 16079.229708, "fee": 0.177161},
                {"txValue": 175.759637, "fee": 0.172541},
                {"txValue": 39176.317296, "fee": 0.876277},
                {"txValue": 11085.754283, "fee": 0.169417},
                {"txValue": 60.145994, "fee": 0.324707},
                {"txValue": 2363993.13253, "fee": 0.699296},
                {"txValue": 51520.154376, "fee": 0.440164},
                {"txValue": 53681.788743, "fee": 0.476986},
                {"txValue": 1465815.147129, "fee": 0.481592},
                {"txValue": 99.999433, "fee": 0.42121},
                {"txValue": 16.916551, "fee": 0.183365},
                {"txValue": 5.50964, "fee": 0.49036},
                {"txValue": 3003.447968, "fee": 0.434756},
                {"txValue": 735.460929, "fee": 0.166588},
                {"txValue": 2991.99741, "fee": 0.434756},
                {"txValue": 243541.14223, "fee": 0.245381},
            ]
        },
    },
    "assistant_text": (
        "Scatter chart of transaction fee vs transaction value for a random sample "
        "from the last 24 hours (demo data). In production, this would be sampled "
        "from recent transactions and rendered with a log scale on value to handle "
        "large variance."
    ),
}
