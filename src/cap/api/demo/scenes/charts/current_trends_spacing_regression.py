SCENE = {
    "match": "current trends",
    "assistant_text": (
        "The data shows a peak in activity on December 11th, with the highest number of NFTs minted "
        "(1,271) and accounts created (6,473). However, all metrics sharply declined afterward, with "
        "scripts deployed dropping to 5 on December 15th and multi-assets created falling to 1."
    ),
    "kv": {
        "result_type": "line_chart",
        "data": {
            "values": [
                {"x": "2025-12-09", "y": "95", "c": 0},
                {"x": "2025-12-09", "y": "84", "c": 1},
                {"x": "2025-12-09", "y": "438", "c": 2},
                {"x": "2025-12-09", "y": "7161", "c": 3},
                {"x": "2025-12-10", "y": "51", "c": 0},
                {"x": "2025-12-10", "y": "24", "c": 1},
                {"x": "2025-12-10", "y": "609", "c": 2},
                {"x": "2025-12-10", "y": "5806", "c": 3},
                {"x": "2025-12-11", "y": "24", "c": 0},
                {"x": "2025-12-11", "y": "17", "c": 1},
                {"x": "2025-12-11", "y": "1271", "c": 2},
                {"x": "2025-12-11", "y": "6473", "c": 3},
                {"x": "2025-12-12", "y": "27", "c": 0},
                {"x": "2025-12-12", "y": "37", "c": 1},
                {"x": "2025-12-12", "y": "1385", "c": 2},
                {"x": "2025-12-12", "y": "3965", "c": 3},
                {"x": "2025-12-13", "y": "22", "c": 0},
                {"x": "2025-12-13", "y": "36", "c": 1},
                {"x": "2025-12-13", "y": "543", "c": 2},
                {"x": "2025-12-13", "y": "3199", "c": 3},
                {"x": "2025-12-14", "y": "41", "c": 0},
                {"x": "2025-12-14", "y": "38", "c": 1},
                {"x": "2025-12-14", "y": "453", "c": 2},
                {"x": "2025-12-14", "y": "3759", "c": 3},
                {"x": "2025-12-15", "y": "5", "c": 0},
                {"x": "2025-12-15", "y": "1", "c": 1},
                {"x": "2025-12-15", "y": "40", "c": 2},
                {"x": "2025-12-15", "y": "288", "c": 3},
            ]
        },
        "metadata": {
            "count": 7,
            "columns": ["date", "scriptsDeployed", "multiAssetsCreated", "nftsMinted", "accountsCreated"],
        },
    },
    "kv_type": "line",
    "artifact_type": "chart",
}
