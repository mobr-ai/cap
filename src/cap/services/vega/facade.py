import logging
from typing import Any

from opentelemetry import trace

from cap.services.vega.chart_converters import VegaChartConverter
from cap.services.vega.coordinate_mapper import VegaCoordinate
from cap.services.vega.field_utils import VegaField
from cap.services.vega.table_converter import VegaTableConverter
from cap.services.vega.value_util import VegaValue

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class VegaConverter(
    VegaChartConverter,
    VegaTableConverter,
    VegaCoordinate,
    VegaField,
    VegaValue,
):
    """Util to convert data to Vega format."""

    x_candidates = [
        'yearMonth', 'year', 'month', 'date', 'timePeriod', 'timestamp', 'ts',
        'hour', 'hourOfDay', 'hour_of_day', 'epoch', 'epochNumber', 'x', 'index', 'blockHeight', 'blockNumber',
        'name', 'label', 'category'
    ]

    known_types = {
        "bar_chart",
        "pie_chart",
        "line_chart",
        "scatter_chart",
        "bubble_chart",
        "treemap",
        "heatmap",
        "table",
        "text",
    }

    @classmethod
    def convert_to_vega_format(
        cls,
        kv_results: dict[str, Any],
        user_query: str
        ) -> dict[str, Any]:
        """
        Convert kv_results to Vega-compatible format based on result_type and data structure.
        """
        result_type = kv_results.get("result_type", "")
        data = kv_results.get("data", [])

        if not data:
            return {"values": []}

        data = cls._preprocess_values(data)

        try:
            if result_type == "bar_chart":
                return cls._convert_bar_chart(data, user_query)

            elif result_type == "pie_chart":
                return cls._convert_pie_chart(data, user_query)

            elif result_type == "line_chart":
                return cls._convert_line_chart(data, user_query)

            elif result_type == "table":
                return cls._convert_table(data, user_query)

            elif result_type == "scatter_chart":
                return cls._convert_scatter_chart(data, user_query)

            elif result_type == "bubble_chart":
                return cls._convert_bubble_chart(data, user_query)

            elif result_type == "treemap":
                return cls._convert_treemap(data, user_query)

            elif result_type == "heatmap":
                return cls._convert_heatmap(data, user_query)
            else:
                return {"values": []}

        except Exception as e:
            logger.error(f"Error converting to Vega format: {e}")
            return {"values": []}
