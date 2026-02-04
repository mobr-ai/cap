import math

SCENE = {
    "match": "heatmap of transaction activity by day and hour",
    "kv": {
        "result_type": "heatmap",
        "data": {
            "values": [
                *[
                    {
                        "x": f"2025-03-{day:02d}",
                        "y": f"{h:02d}",
                        "value": (
                            0.31
                            + 0.040 * math.exp(-0.5 * ((h - 20.0) / 2.7) ** 2)
                            + 0.020 * math.exp(-0.5 * ((h - 2.0) / 2.4) ** 2)
                            - 0.010 * math.exp(-0.5 * ((h - 12.0) / 3.5) ** 2)
                            + 0.010 * math.sin(2 * math.pi * (day / 7.0))
                            + 0.006 * math.sin(2 * math.pi * (day / 14.0))
                            + 0.006
                            * (
                                (
                                    (
                                        math.sin((day * 12.9898 + h * 78.233) * 0.12)
                                        * 43758.5453
                                    )
                                    % 1.0
                                )
                                - 0.5
                            )
                            + 0.004
                            * (
                                (
                                    (
                                        math.sin((day * 93.989 + h * 67.345) * 0.07)
                                        * 12345.6789
                                    )
                                    % 1.0
                                )
                                - 0.5
                            )
                            + (
                                0.020
                                if (
                                    ((math.sin(day * 1.77 + h * 2.33) * 10000.0) % 1.0)
                                    > 0.995
                                )
                                else 0.0
                            )
                        ),
                    }
                    for day in range(1, 32)
                    for h in range(24)
                ]
            ]
        },
        "metadata": {"count": 31 * 24, "columns": ["Day", "Hour", "Avg Fee"]},
    },
    "assistant_text": (
        "Heatmap of average fee by day (x, YYYY-MM-DD) and hour (y, 00–23). "
        "Cell intensity is the avg fee value (demo data)."
    ),
}
